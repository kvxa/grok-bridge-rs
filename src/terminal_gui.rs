use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, SyncSender, TrySendError, channel, sync_channel},
    },
    thread,
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use eframe::egui;

use crate::{
    gui_fonts::install_cjk_font,
    protocol::{MAX_WRITE_BYTES, Request, ResponseEnvelope, ResponseResult},
    transport::call,
};

const READ_LIMIT: u32 = 65_536;
const READ_WAIT_MS: u64 = 500;
const SCROLLBACK_ROWS: usize = 10_000;
const WRITER_QUEUE_CAPACITY: usize = 128;
const FONT_SIZE: f32 = 14.0;
const TERMINAL_PADDING: f32 = 4.0;
const MIN_COLS: u16 = 20;
const MAX_COLS: u16 = 500;
const MIN_ROWS: u16 = 5;
const MAX_ROWS: u16 = 200;

const DEFAULT_FOREGROUND: egui::Color32 = egui::Color32::from_rgb(216, 222, 233);
const DEFAULT_BACKGROUND: egui::Color32 = egui::Color32::from_rgb(11, 14, 20);

pub(crate) fn run(session: String) -> Result<()> {
    let title = format!("Grok Terminal - {session}");
    let app_id = format!("grok-bridge-terminal-{session}");
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_app_id(app_id)
            .with_inner_size([1_080.0, 720.0])
            .with_min_inner_size([640.0, 360.0]),
        ..Default::default()
    };

    eframe::run_native(
        &title,
        options,
        Box::new(move |creation_context| {
            let font_status = match install_cjk_font(&creation_context.egui_ctx) {
                Ok(font) => format!(
                    "Fonts: Consolas ({}) + Microsoft YaHei ({}, face {})",
                    font.latin_path.display(),
                    font.cjk_path.display(),
                    font.cjk_face_index
                ),
                Err(error) => format!("Terminal font unavailable: {error:#}"),
            };
            Ok(Box::new(TerminalApp::new(
                session,
                creation_context.egui_ctx.clone(),
                font_status,
            )))
        }),
    )
    .map_err(|error| anyhow!(error.to_string()))
}

struct Snapshot {
    rows: u16,
    cols: u16,
    state: Vec<u8>,
    cursor: u64,
}

enum ReaderMessage {
    Snapshot(Snapshot),
    Output { data: Vec<u8>, next_cursor: u64 },
    Eof,
    Error(String),
}

enum WriterCommand {
    Write(Vec<u8>),
    Resize { cols: u16, rows: u16 },
    Close,
}

enum WriterMessage {
    Closed,
    Error {
        message: String,
        close_failed: bool,
        resize_failed: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CellPosition {
    row: u16,
    col: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Selection {
    anchor: CellPosition,
    head: CellPosition,
}

struct TerminalApp {
    session: String,
    parser: Option<vt100::Parser>,
    reader_rx: Receiver<ReaderMessage>,
    writer_tx: SyncSender<WriterCommand>,
    writer_rx: Receiver<WriterMessage>,
    stop_reader: Arc<AtomicBool>,
    cursor: u64,
    last_resize: Option<(u16, u16)>,
    selection: Option<Selection>,
    scroll_remainder: f32,
    close_pending: bool,
    eof: bool,
    status: String,
    error: Option<String>,
    font_status: String,
}

impl TerminalApp {
    fn new(session: String, context: egui::Context, font_status: String) -> Self {
        let (reader_tx, reader_rx) = channel();
        let (writer_tx, writer_jobs) = sync_channel(WRITER_QUEUE_CAPACITY);
        let (writer_done, writer_rx) = channel();
        let stop_reader = Arc::new(AtomicBool::new(false));

        spawn_reader(
            session.clone(),
            context.clone(),
            Arc::clone(&stop_reader),
            reader_tx,
        );
        spawn_writer(session.clone(), context, writer_jobs, writer_done);

        Self {
            session,
            parser: None,
            reader_rx,
            writer_tx,
            writer_rx,
            stop_reader,
            cursor: 0,
            last_resize: None,
            selection: None,
            scroll_remainder: 0.0,
            close_pending: false,
            eof: false,
            status: "正在连接会话…".to_owned(),
            error: None,
            font_status,
        }
    }

    fn apply_reader_message(&mut self, message: ReaderMessage) {
        match message {
            ReaderMessage::Snapshot(snapshot) => {
                let rows = snapshot.rows.max(1);
                let cols = snapshot.cols.max(1);
                let mut parser = vt100::Parser::new(rows, cols, SCROLLBACK_ROWS);
                parser.process(&snapshot.state);
                self.parser = Some(parser);
                self.cursor = snapshot.cursor;
                self.last_resize = None;
                self.selection = None;
                self.scroll_remainder = 0.0;
                self.eof = false;
                self.error = None;
                self.status = format!("已连接 · {cols}×{rows}");
            }
            ReaderMessage::Output { data, next_cursor } => {
                if let Some(parser) = self.parser.as_mut() {
                    parser.process(&data);
                    self.cursor = next_cursor;
                    self.status = format!("实时读取 · cursor {}", self.cursor);
                }
            }
            ReaderMessage::Eof => {
                self.eof = true;
                self.status = "会话输出已结束".to_owned();
            }
            ReaderMessage::Error(error) => {
                self.error = Some(error);
                self.status = "读取已停止".to_owned();
            }
        }
    }

    fn apply_writer_message(&mut self, context: &egui::Context, message: WriterMessage) {
        match message {
            WriterMessage::Closed => {
                self.status = "会话已关闭".to_owned();
                context.send_viewport_cmd(egui::ViewportCommand::Close);
            }
            WriterMessage::Error {
                message,
                close_failed,
                resize_failed,
            } => {
                if close_failed {
                    self.close_pending = false;
                }
                if resize_failed {
                    self.last_resize = None;
                }
                self.error = Some(message);
            }
        }
    }

    fn queue_writer(&mut self, command: WriterCommand) -> bool {
        match self.writer_tx.try_send(command) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                self.error = Some("终端输入队列已满，请稍后重试".to_owned());
                false
            }
            Err(TrySendError::Disconnected(_)) => {
                self.error = Some("终端 writer worker 已退出".to_owned());
                false
            }
        }
    }

    fn resize_terminal(&mut self, cols: u16, rows: u16) {
        if self.parser.is_none() {
            return;
        }
        if self.last_resize == Some((cols, rows)) {
            return;
        }
        if self.queue_writer(WriterCommand::Resize { cols, rows }) {
            self.last_resize = Some((cols, rows));
            if let Some(parser) = self.parser.as_mut() {
                parser.screen_mut().set_size(rows, cols);
            }
        }
    }

    fn send_input(&mut self, bytes: Vec<u8>) {
        if !bytes.is_empty() {
            self.queue_writer(WriterCommand::Write(bytes));
        }
    }

    fn scroll_terminal(&mut self, delta_y: f32, cell_height: f32) {
        if delta_y == 0.0 {
            return;
        }
        self.scroll_remainder += delta_y / cell_height;
        let rows = self.scroll_remainder.trunc() as isize;
        if rows == 0 {
            return;
        }
        self.scroll_remainder -= rows as f32;
        if let Some(parser) = self.parser.as_mut() {
            let current = parser.screen().scrollback() as isize;
            parser
                .screen_mut()
                .set_scrollback(current.saturating_add(rows).max(0) as usize);
        }
    }

    fn show_toolbar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.strong("Grok Terminal");
            ui.separator();
            ui.monospace(&self.session);
            ui.separator();
            ui.label(&self.status);

            let close = ui.add_enabled(
                !self.close_pending,
                egui::Button::new(if self.close_pending {
                    "正在关闭会话…"
                } else {
                    "关闭会话"
                }),
            );
            if close.clicked() && self.queue_writer(WriterCommand::Close) {
                self.close_pending = true;
            }
        });
        ui.small(format!(
            "{} · 关闭窗口只会分离，不会关闭会话",
            self.font_status
        ));
        if let Some(error) = &self.error {
            ui.colored_label(egui::Color32::LIGHT_RED, error);
        }
        ui.separator();
    }

    fn show_terminal(&mut self, ui: &mut egui::Ui) {
        let font_id = egui::FontId::monospace(FONT_SIZE);
        let (cell_width, cell_height) = ui.fonts_mut(|fonts| {
            (
                fonts.glyph_width(&font_id, 'M').max(1.0),
                fonts.row_height(&font_id).max(1.0),
            )
        });

        let available = ui.available_size();
        let (rect, response) = ui.allocate_exact_size(available, egui::Sense::click_and_drag());
        if response.clicked_by(egui::PointerButton::Primary) {
            response.request_focus();
            self.selection = None;
        } else if response.drag_started_by(egui::PointerButton::Primary) {
            response.request_focus();
        }
        if response.has_focus() {
            ui.memory_mut(|memory| {
                memory.set_focus_lock_filter(
                    response.id,
                    egui::EventFilter {
                        tab: true,
                        horizontal_arrows: true,
                        vertical_arrows: true,
                        escape: true,
                    },
                );
            });
        }

        let terminal_rect = rect.shrink(TERMINAL_PADDING);
        let cols = ((terminal_rect.width() / cell_width).floor() as u16).clamp(MIN_COLS, MAX_COLS);
        let rows =
            ((terminal_rect.height() / cell_height).floor() as u16).clamp(MIN_ROWS, MAX_ROWS);
        self.resize_terminal(cols, rows);

        if response.hovered() {
            let delta_y = ui.input(|input| input.smooth_scroll_delta.y);
            self.scroll_terminal(delta_y, cell_height);
        }

        let pointer_cell = response.interact_pointer_pos().and_then(|position| {
            cell_position(terminal_rect, position, cell_width, cell_height, rows, cols)
        });
        if response.drag_started_by(egui::PointerButton::Primary) {
            if let Some(position) = pointer_cell {
                let position = canonical_position(self.parser.as_ref(), position);
                self.selection = Some(Selection {
                    anchor: position,
                    head: position,
                });
            }
        } else if response.dragged_by(egui::PointerButton::Primary)
            && let Some(position) = pointer_cell
        {
            let position = canonical_position(self.parser.as_ref(), position);
            if let Some(selection) = self.selection.as_mut() {
                selection.head = position;
            }
        }

        let blink_on = ui.input(|input| ((input.time * 2.0) as u64).is_multiple_of(2));
        if let Some(parser) = self.parser.as_ref() {
            paint_terminal(
                ui,
                parser.screen(),
                TerminalPaintOptions {
                    rect: terminal_rect,
                    font_id: &font_id,
                    cell_width,
                    cell_height,
                    focused: response.has_focus(),
                    blink_on,
                    selection: self.selection,
                },
            );
        } else {
            ui.painter().rect_filled(rect, 0.0, DEFAULT_BACKGROUND);
            ui.painter().text(
                rect.center(),
                egui::Align2::CENTER_CENTER,
                "正在载入终端快照…",
                font_id,
                DEFAULT_FOREGROUND,
            );
        }

        if response.has_focus() {
            let (events, modifiers) = ui.input(|input| (input.events.clone(), input.modifiers));
            let application_cursor = self
                .parser
                .as_ref()
                .is_some_and(|parser| parser.screen().application_cursor());
            let bracketed_paste = self
                .parser
                .as_ref()
                .is_some_and(|parser| parser.screen().bracketed_paste());
            let has_selection = self.selection.is_some();
            if has_selection && copy_requested(&events) {
                let text = self
                    .parser
                    .as_ref()
                    .and_then(|parser| {
                        self.selection
                            .map(|selection| selection_text(parser.screen(), selection))
                    })
                    .unwrap_or_default();
                ui.ctx().copy_text(text);
            }
            if !self.close_pending && !self.eof {
                self.send_input(input_bytes(
                    &events,
                    modifiers,
                    application_cursor,
                    bracketed_paste,
                    has_selection,
                ));
            }
        }
    }
}

impl eframe::App for TerminalApp {
    fn logic(&mut self, context: &egui::Context, _frame: &mut eframe::Frame) {
        while let Ok(message) = self.reader_rx.try_recv() {
            self.apply_reader_message(message);
        }
        while let Ok(message) = self.writer_rx.try_recv() {
            self.apply_writer_message(context, message);
        }
        context.request_repaint_after(Duration::from_millis(250));
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        ui.add_space(6.0);
        self.show_toolbar(ui);
        self.show_terminal(ui);
    }
}

impl Drop for TerminalApp {
    fn drop(&mut self) {
        self.stop_reader.store(true, Ordering::Release);
    }
}

fn spawn_reader(
    session: String,
    context: egui::Context,
    stop: Arc<AtomicBool>,
    tx: std::sync::mpsc::Sender<ReaderMessage>,
) {
    thread::spawn(move || {
        while !stop.load(Ordering::Acquire) {
            let snapshot = match read_snapshot(&session) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    let _ = tx.send(ReaderMessage::Error(format!("{error:#}")));
                    context.request_repaint();
                    return;
                }
            };
            let mut cursor = snapshot.cursor;
            if tx.send(ReaderMessage::Snapshot(snapshot)).is_err() {
                return;
            }
            context.request_repaint();

            while !stop.load(Ordering::Acquire) {
                let read = match read_increment(&session, cursor) {
                    Ok(read) => read,
                    Err(error) => {
                        let _ = tx.send(ReaderMessage::Error(format!("{error:#}")));
                        context.request_repaint();
                        return;
                    }
                };
                if read.truncated {
                    break;
                }

                let data = match BASE64.decode(&read.data_base64) {
                    Ok(data) => data,
                    Err(error) => {
                        let _ = tx.send(ReaderMessage::Error(format!(
                            "runtime returned invalid terminal output: {error}"
                        )));
                        context.request_repaint();
                        return;
                    }
                };
                cursor = read.next_cursor;
                if !data.is_empty()
                    && tx
                        .send(ReaderMessage::Output {
                            data,
                            next_cursor: cursor,
                        })
                        .is_err()
                {
                    return;
                }
                if read.eof {
                    let _ = tx.send(ReaderMessage::Eof);
                    context.request_repaint();
                    return;
                }
                context.request_repaint();
            }
        }
    });
}

fn read_snapshot(session: &str) -> Result<Snapshot> {
    let response = call(
        Request::Show {
            session: session.to_owned(),
        },
        true,
    )?;
    let state = match successful_result(response)? {
        ResponseResult::Session(state) => state,
        _ => bail!("runtime returned an unexpected show response"),
    };
    let snapshot = BASE64
        .decode(&state.screen_ansi_base64)
        .context("runtime returned an invalid terminal snapshot")?;
    Ok(Snapshot {
        rows: state.rows,
        cols: state.cols,
        state: snapshot,
        cursor: state.last_cursor,
    })
}

fn read_increment(session: &str, cursor: u64) -> Result<crate::protocol::ReadResult> {
    let response = call(
        Request::Read {
            session: session.to_owned(),
            cursor: Some(cursor),
            limit: Some(READ_LIMIT),
            wait_ms: Some(READ_WAIT_MS),
        },
        true,
    )?;
    match successful_result(response)? {
        ResponseResult::Read(read) => Ok(read),
        _ => bail!("runtime returned an unexpected read response"),
    }
}

fn spawn_writer(
    session: String,
    context: egui::Context,
    jobs: Receiver<WriterCommand>,
    done: std::sync::mpsc::Sender<WriterMessage>,
) {
    thread::spawn(move || {
        while let Ok(command) = jobs.recv() {
            let close = matches!(command, WriterCommand::Close);
            let resize = matches!(command, WriterCommand::Resize { .. });
            let result = match command {
                WriterCommand::Write(data) => write_all(&session, &data),
                WriterCommand::Resize { cols, rows } => call(
                    Request::Resize {
                        session: session.clone(),
                        cols,
                        rows,
                    },
                    true,
                )
                .and_then(expect_writer_success),
                WriterCommand::Close => call(
                    Request::Close {
                        session: session.clone(),
                    },
                    true,
                )
                .and_then(expect_writer_success),
            };
            match result {
                Ok(()) if close => {
                    let _ = done.send(WriterMessage::Closed);
                    context.request_repaint();
                    return;
                }
                Ok(()) => {}
                Err(error) => {
                    if done
                        .send(WriterMessage::Error {
                            message: format!("{error:#}"),
                            close_failed: close,
                            resize_failed: resize,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
            }
            context.request_repaint();
        }
    });
}

fn write_all(session: &str, data: &[u8]) -> Result<()> {
    for chunk in data.chunks(MAX_WRITE_BYTES) {
        call(
            Request::Write {
                session: session.to_owned(),
                data_base64: BASE64.encode(chunk),
            },
            true,
        )
        .and_then(expect_writer_success)?;
    }
    Ok(())
}

fn successful_result(response: ResponseEnvelope) -> Result<ResponseResult> {
    if !response.ok {
        return Err(response_error(response));
    }
    response
        .result
        .ok_or_else(|| anyhow!("runtime returned a successful response without a result"))
}

fn expect_writer_success(response: ResponseEnvelope) -> Result<()> {
    match successful_result(response)? {
        ResponseResult::Accepted { accepted: true } | ResponseResult::Session(_) => Ok(()),
        ResponseResult::Accepted { accepted: false } => bail!("runtime rejected the request"),
        _ => bail!("runtime returned an unexpected writer response"),
    }
}

fn response_error(response: ResponseEnvelope) -> anyhow::Error {
    match response.error {
        Some(error) => anyhow!("{}: {}", error.code, error.message),
        None => anyhow!("runtime returned an unsuccessful response without an error"),
    }
}

fn cell_position(
    rect: egui::Rect,
    position: egui::Pos2,
    cell_width: f32,
    cell_height: f32,
    rows: u16,
    cols: u16,
) -> Option<CellPosition> {
    if rows == 0 || cols == 0 || rect.width() <= 0.0 || rect.height() <= 0.0 {
        return None;
    }
    let x = (position.x - rect.left()).clamp(0.0, rect.width().max(1.0) - f32::EPSILON);
    let y = (position.y - rect.top()).clamp(0.0, rect.height().max(1.0) - f32::EPSILON);
    Some(CellPosition {
        row: ((y / cell_height).floor() as u16).min(rows - 1),
        col: ((x / cell_width).floor() as u16).min(cols - 1),
    })
}

fn canonical_position(parser: Option<&vt100::Parser>, mut position: CellPosition) -> CellPosition {
    if parser.is_some_and(|parser| {
        parser
            .screen()
            .cell(position.row, position.col)
            .is_some_and(vt100::Cell::is_wide_continuation)
    }) {
        position.col = position.col.saturating_sub(1);
    }
    position
}

fn normalized_selection(
    selection: Selection,
    rows: u16,
    cols: u16,
) -> Option<(CellPosition, CellPosition)> {
    if rows == 0 || cols == 0 {
        return None;
    }
    let clamp = |position: CellPosition| CellPosition {
        row: position.row.min(rows - 1),
        col: position.col.min(cols - 1),
    };
    let anchor = clamp(selection.anchor);
    let head = clamp(selection.head);
    if (anchor.row, anchor.col) <= (head.row, head.col) {
        Some((anchor, head))
    } else {
        Some((head, anchor))
    }
}

fn position_selected(
    screen: &vt100::Screen,
    row: u16,
    col: u16,
    normalized: Option<(CellPosition, CellPosition)>,
) -> bool {
    let Some((start, end)) = normalized else {
        return false;
    };
    let col = if screen
        .cell(row, col)
        .is_some_and(vt100::Cell::is_wide_continuation)
    {
        col.saturating_sub(1)
    } else {
        col
    };
    (start.row, start.col) <= (row, col) && (row, col) <= (end.row, end.col)
}

fn selection_text(screen: &vt100::Screen, selection: Selection) -> String {
    let (rows, cols) = screen.size();
    let Some((start, end)) = normalized_selection(selection, rows, cols) else {
        return String::new();
    };
    let mut output = String::new();

    for row in start.row..=end.row {
        let first_col = if row == start.row { start.col } else { 0 };
        let last_col = if row == end.row { end.col } else { cols - 1 };
        let row_start = output.len();
        for col in first_col..=last_col {
            let Some(cell) = screen.cell(row, col) else {
                continue;
            };
            if cell.is_wide_continuation() {
                continue;
            }
            if cell.has_contents() {
                output.push_str(cell.contents());
            } else {
                output.push(' ');
            }
        }
        while output.len() > row_start && output.ends_with(' ') {
            output.pop();
        }
        if row != end.row && !screen.row_wrapped(row) {
            output.push('\n');
        }
    }
    output
}

fn copy_requested(events: &[egui::Event]) -> bool {
    events.iter().any(|event| match event {
        egui::Event::Copy => true,
        egui::Event::Key {
            key: egui::Key::C,
            pressed: true,
            modifiers,
            ..
        } => modifiers.ctrl,
        _ => false,
    })
}

struct TerminalPaintOptions<'a> {
    rect: egui::Rect,
    font_id: &'a egui::FontId,
    cell_width: f32,
    cell_height: f32,
    focused: bool,
    blink_on: bool,
    selection: Option<Selection>,
}

fn paint_terminal(ui: &egui::Ui, screen: &vt100::Screen, options: TerminalPaintOptions<'_>) {
    let TerminalPaintOptions {
        rect,
        font_id,
        cell_width,
        cell_height,
        focused,
        blink_on,
        selection,
    } = options;
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 0.0, DEFAULT_BACKGROUND);

    let (screen_rows, screen_cols) = screen.size();
    let visible_cols = ((rect.width() / cell_width).floor() as u16).min(screen_cols);
    let visible_rows = ((rect.height() / cell_height).floor() as u16).min(screen_rows);
    let normalized_selection =
        selection.and_then(|selection| normalized_selection(selection, visible_rows, visible_cols));

    for row in 0..visible_rows {
        for col in 0..visible_cols {
            let Some(cell) = screen.cell(row, col) else {
                continue;
            };
            let mut foreground = terminal_color(cell.fgcolor(), DEFAULT_FOREGROUND, cell.bold());
            let mut background = terminal_color(cell.bgcolor(), DEFAULT_BACKGROUND, false);
            if cell.inverse() {
                std::mem::swap(&mut foreground, &mut background);
            }
            if cell.dim() {
                foreground = dim_color(foreground);
            }

            let left = rect.left() + f32::from(col) * cell_width;
            let top = rect.top() + f32::from(row) * cell_height;
            let cell_rect = egui::Rect::from_min_size(
                egui::pos2(left, top),
                egui::vec2(cell_width, cell_height),
            );
            painter.rect_filled(cell_rect, 0.0, background);
            if position_selected(screen, row, col, normalized_selection) {
                painter.rect_filled(
                    cell_rect,
                    0.0,
                    egui::Color32::from_rgba_unmultiplied(80, 135, 230, 100),
                );
            }

            if !cell.is_wide_continuation() && cell.has_contents() {
                let text_position = cell_rect.left_top();
                painter.text(
                    text_position,
                    egui::Align2::LEFT_TOP,
                    cell.contents(),
                    font_id.clone(),
                    foreground,
                );
                if cell.bold() {
                    painter.text(
                        text_position + egui::vec2(0.5, 0.0),
                        egui::Align2::LEFT_TOP,
                        cell.contents(),
                        font_id.clone(),
                        foreground,
                    );
                }
            }
            if cell.underline() {
                painter.line_segment(
                    [
                        egui::pos2(cell_rect.left(), cell_rect.bottom() - 1.0),
                        egui::pos2(cell_rect.right(), cell_rect.bottom() - 1.0),
                    ],
                    egui::Stroke::new(1.0, foreground),
                );
            }
        }
    }

    if screen.scrollback() == 0 && !screen.hide_cursor() && blink_on {
        let (row, col) = screen.cursor_position();
        if row < visible_rows && col < visible_cols {
            let cursor_width = if screen.cell(row, col).is_some_and(vt100::Cell::is_wide) {
                cell_width * 2.0
            } else {
                cell_width
            };
            let cursor_left = rect.left() + f32::from(col) * cell_width;
            let cursor_rect = egui::Rect::from_min_size(
                egui::pos2(cursor_left, rect.top() + f32::from(row) * cell_height),
                egui::vec2(cursor_width.min(rect.right() - cursor_left), cell_height),
            );
            let color = if focused {
                egui::Color32::from_rgba_unmultiplied(235, 235, 235, 110)
            } else {
                egui::Color32::from_rgba_unmultiplied(235, 235, 235, 55)
            };
            painter.rect_filled(cursor_rect, 0.0, color);
        }
    }
}

fn input_bytes(
    events: &[egui::Event],
    modifiers: egui::Modifiers,
    application_cursor: bool,
    bracketed_paste: bool,
    has_selection: bool,
) -> Vec<u8> {
    let has_paste = events
        .iter()
        .any(|event| matches!(event, egui::Event::Paste(_)));
    let mut output = Vec::new();

    for event in events {
        match event {
            egui::Event::Text(text) => output.extend(text_bytes(text, modifiers.alt)),
            egui::Event::Paste(text) => output.extend(paste_bytes(text, bracketed_paste)),
            egui::Event::Ime(egui::ImeEvent::Commit(text)) => {
                output.extend(text_bytes(text, modifiers.alt));
            }
            egui::Event::Key {
                key,
                pressed: true,
                modifiers,
                ..
            } if !(modifiers.ctrl
                && ((has_paste && *key == egui::Key::V)
                    || (has_selection && *key == egui::Key::C))) =>
            {
                if let Some(bytes) = key_bytes(*key, *modifiers, application_cursor) {
                    output.extend(bytes);
                }
            }
            _ => {}
        }
    }
    output
}

fn text_bytes(text: &str, alt: bool) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(text.len() + usize::from(alt));
    if alt {
        bytes.push(0x1b);
    }
    bytes.extend_from_slice(text.as_bytes());
    bytes
}

fn paste_bytes(text: &str, bracketed: bool) -> Vec<u8> {
    if !bracketed {
        return text.as_bytes().to_vec();
    }
    let mut bytes = Vec::with_capacity(text.len() + 12);
    bytes.extend_from_slice(b"\x1b[200~");
    bytes.extend_from_slice(text.as_bytes());
    bytes.extend_from_slice(b"\x1b[201~");
    bytes
}

fn key_bytes(
    key: egui::Key,
    modifiers: egui::Modifiers,
    application_cursor: bool,
) -> Option<Vec<u8>> {
    let bytes: &[u8] = if modifiers.ctrl {
        if let Some(control) = control_byte(key) {
            return Some(with_alt(vec![control], modifiers.alt));
        }
        match key {
            egui::Key::Space => return Some(with_alt(vec![0], modifiers.alt)),
            _ => key_sequence(key, modifiers.shift, application_cursor)?,
        }
    } else {
        key_sequence(key, modifiers.shift, application_cursor)?
    };
    Some(with_alt(bytes.to_vec(), modifiers.alt))
}

fn key_sequence(key: egui::Key, shift: bool, application_cursor: bool) -> Option<&'static [u8]> {
    let application = application_cursor;
    match key {
        egui::Key::Enter => Some(b"\r"),
        egui::Key::Tab if shift => Some(b"\x1b[Z"),
        egui::Key::Tab => Some(b"\t"),
        egui::Key::Backspace => Some(b"\x7f"),
        egui::Key::Escape => Some(b"\x1b"),
        egui::Key::ArrowUp if application => Some(b"\x1bOA"),
        egui::Key::ArrowDown if application => Some(b"\x1bOB"),
        egui::Key::ArrowRight if application => Some(b"\x1bOC"),
        egui::Key::ArrowLeft if application => Some(b"\x1bOD"),
        egui::Key::ArrowUp => Some(b"\x1b[A"),
        egui::Key::ArrowDown => Some(b"\x1b[B"),
        egui::Key::ArrowRight => Some(b"\x1b[C"),
        egui::Key::ArrowLeft => Some(b"\x1b[D"),
        egui::Key::Home if application => Some(b"\x1bOH"),
        egui::Key::End if application => Some(b"\x1bOF"),
        egui::Key::Home => Some(b"\x1b[H"),
        egui::Key::End => Some(b"\x1b[F"),
        egui::Key::Insert => Some(b"\x1b[2~"),
        egui::Key::Delete => Some(b"\x1b[3~"),
        egui::Key::PageUp => Some(b"\x1b[5~"),
        egui::Key::PageDown => Some(b"\x1b[6~"),
        egui::Key::F1 => Some(b"\x1bOP"),
        egui::Key::F2 => Some(b"\x1bOQ"),
        egui::Key::F3 => Some(b"\x1bOR"),
        egui::Key::F4 => Some(b"\x1bOS"),
        egui::Key::F5 => Some(b"\x1b[15~"),
        egui::Key::F6 => Some(b"\x1b[17~"),
        egui::Key::F7 => Some(b"\x1b[18~"),
        egui::Key::F8 => Some(b"\x1b[19~"),
        egui::Key::F9 => Some(b"\x1b[20~"),
        egui::Key::F10 => Some(b"\x1b[21~"),
        egui::Key::F11 => Some(b"\x1b[23~"),
        egui::Key::F12 => Some(b"\x1b[24~"),
        _ => None,
    }
}

fn control_byte(key: egui::Key) -> Option<u8> {
    match key {
        egui::Key::A => Some(1),
        egui::Key::B => Some(2),
        egui::Key::C => Some(3),
        egui::Key::D => Some(4),
        egui::Key::E => Some(5),
        egui::Key::F => Some(6),
        egui::Key::G => Some(7),
        egui::Key::H => Some(8),
        egui::Key::I => Some(9),
        egui::Key::J => Some(10),
        egui::Key::K => Some(11),
        egui::Key::L => Some(12),
        egui::Key::M => Some(13),
        egui::Key::N => Some(14),
        egui::Key::O => Some(15),
        egui::Key::P => Some(16),
        egui::Key::Q => Some(17),
        egui::Key::R => Some(18),
        egui::Key::S => Some(19),
        egui::Key::T => Some(20),
        egui::Key::U => Some(21),
        egui::Key::V => Some(22),
        egui::Key::W => Some(23),
        egui::Key::X => Some(24),
        egui::Key::Y => Some(25),
        egui::Key::Z => Some(26),
        _ => None,
    }
}

fn with_alt(mut bytes: Vec<u8>, alt: bool) -> Vec<u8> {
    if alt {
        bytes.insert(0, 0x1b);
    }
    bytes
}

fn terminal_color(color: vt100::Color, default: egui::Color32, bold: bool) -> egui::Color32 {
    match color {
        vt100::Color::Default => default,
        vt100::Color::Idx(index) => {
            let index = if bold && index < 8 { index + 8 } else { index };
            indexed_color(index)
        }
        vt100::Color::Rgb(red, green, blue) => egui::Color32::from_rgb(red, green, blue),
    }
}

fn indexed_color(index: u8) -> egui::Color32 {
    const BASIC: [[u8; 3]; 16] = [
        [0, 0, 0],
        [205, 0, 0],
        [0, 205, 0],
        [205, 205, 0],
        [0, 0, 238],
        [205, 0, 205],
        [0, 205, 205],
        [229, 229, 229],
        [127, 127, 127],
        [255, 0, 0],
        [0, 255, 0],
        [255, 255, 0],
        [92, 92, 255],
        [255, 0, 255],
        [0, 255, 255],
        [255, 255, 255],
    ];
    if index < 16 {
        let [red, green, blue] = BASIC[usize::from(index)];
        return egui::Color32::from_rgb(red, green, blue);
    }
    if index < 232 {
        const LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];
        let offset = index - 16;
        let red = LEVELS[usize::from(offset / 36)];
        let green = LEVELS[usize::from((offset % 36) / 6)];
        let blue = LEVELS[usize::from(offset % 6)];
        return egui::Color32::from_rgb(red, green, blue);
    }
    let level = 8 + 10 * (index - 232);
    egui::Color32::from_gray(level)
}

fn dim_color(color: egui::Color32) -> egui::Color32 {
    egui::Color32::from_rgba_premultiplied(
        ((f32::from(color.r()) * 0.6).round()) as u8,
        ((f32::from(color.g()) * 0.6).round()) as u8,
        ((f32::from(color.b()) * 0.6).round()) as u8,
        color.a(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_terminal_keys_and_cursor_modes() {
        assert_eq!(
            key_bytes(egui::Key::Enter, egui::Modifiers::default(), false),
            Some(b"\r".to_vec())
        );
        assert_eq!(
            key_bytes(egui::Key::ArrowUp, egui::Modifiers::default(), false),
            Some(b"\x1b[A".to_vec())
        );
        assert_eq!(
            key_bytes(egui::Key::ArrowUp, egui::Modifiers::default(), true),
            Some(b"\x1bOA".to_vec())
        );
        assert_eq!(
            key_bytes(
                egui::Key::Tab,
                egui::Modifiers {
                    shift: true,
                    ..Default::default()
                },
                false,
            ),
            Some(b"\x1b[Z".to_vec())
        );
        assert_eq!(
            key_bytes(egui::Key::F12, egui::Modifiers::default(), false),
            Some(b"\x1b[24~".to_vec())
        );
    }

    #[test]
    fn maps_control_and_alt_keys() {
        assert_eq!(
            key_bytes(
                egui::Key::C,
                egui::Modifiers {
                    ctrl: true,
                    ..Default::default()
                },
                false,
            ),
            Some(vec![3])
        );
        assert_eq!(
            key_bytes(
                egui::Key::ArrowLeft,
                egui::Modifiers {
                    alt: true,
                    ..Default::default()
                },
                false,
            ),
            Some(b"\x1b\x1b[D".to_vec())
        );
        assert_eq!(text_bytes("中", true), b"\x1b\xe4\xb8\xad".to_vec());
    }

    #[test]
    fn ctrl_c_copies_selection_instead_of_sending_interrupt() {
        let modifiers = egui::Modifiers {
            ctrl: true,
            ..Default::default()
        };
        let events = [egui::Event::Key {
            key: egui::Key::C,
            physical_key: Some(egui::Key::C),
            pressed: true,
            repeat: false,
            modifiers,
        }];
        assert!(copy_requested(&events));
        assert_eq!(
            input_bytes(&events, modifiers, false, false, false),
            vec![3]
        );
        assert!(input_bytes(&events, modifiers, false, false, true).is_empty());
    }

    #[test]
    fn wraps_bracketed_paste_only_when_enabled() {
        assert_eq!(paste_bytes("a\nb", false), b"a\nb".to_vec());
        assert_eq!(
            paste_bytes("a\nb", true),
            b"\x1b[200~a\nb\x1b[201~".to_vec()
        );
    }

    #[test]
    fn maps_indexed_and_truecolor_values() {
        assert_eq!(indexed_color(16), egui::Color32::from_rgb(0, 0, 0));
        assert_eq!(indexed_color(21), egui::Color32::from_rgb(0, 0, 255));
        assert_eq!(indexed_color(231), egui::Color32::from_rgb(255, 255, 255));
        assert_eq!(indexed_color(232), egui::Color32::from_gray(8));
        assert_eq!(indexed_color(255), egui::Color32::from_gray(238));
        assert_eq!(
            terminal_color(vt100::Color::Idx(1), DEFAULT_FOREGROUND, true),
            indexed_color(9)
        );
        assert_eq!(
            terminal_color(vt100::Color::Rgb(1, 2, 3), DEFAULT_FOREGROUND, false),
            egui::Color32::from_rgb(1, 2, 3)
        );
    }

    #[test]
    fn dims_foreground_without_changing_alpha() {
        assert_eq!(
            dim_color(egui::Color32::from_rgba_unmultiplied(100, 150, 200, 77)),
            egui::Color32::from_rgba_unmultiplied(60, 90, 120, 77)
        );
    }

    #[test]
    fn normalizes_and_clamps_cell_selection() {
        let selection = Selection {
            anchor: CellPosition { row: 9, col: 9 },
            head: CellPosition { row: 0, col: 2 },
        };
        assert_eq!(
            normalized_selection(selection, 3, 5),
            Some((
                CellPosition { row: 0, col: 2 },
                CellPosition { row: 2, col: 4 }
            ))
        );
        assert_eq!(normalized_selection(selection, 0, 5), None);
        assert_eq!(normalized_selection(selection, 3, 0), None);
    }

    #[test]
    fn extracts_visible_selection_without_wide_continuation_duplicates() {
        let mut parser = vt100::Parser::new(3, 8, 0);
        parser.process("abc\r\nhello\r\n中A".as_bytes());

        assert_eq!(
            selection_text(
                parser.screen(),
                Selection {
                    anchor: CellPosition { row: 1, col: 1 },
                    head: CellPosition { row: 0, col: 1 },
                }
            ),
            "bc\nhe"
        );
        assert_eq!(
            selection_text(
                parser.screen(),
                Selection {
                    anchor: CellPosition { row: 2, col: 0 },
                    head: CellPosition { row: 2, col: 2 },
                }
            ),
            "中A"
        );
    }

    #[test]
    fn joins_wrapped_rows_when_copying_selection() {
        let mut parser = vt100::Parser::new(2, 4, 0);
        parser.process(b"abcde");
        assert_eq!(
            selection_text(
                parser.screen(),
                Selection {
                    anchor: CellPosition { row: 0, col: 0 },
                    head: CellPosition { row: 1, col: 3 },
                }
            ),
            "abcde"
        );
    }
}
