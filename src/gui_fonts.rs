use std::{env, fs, path::PathBuf, sync::Arc};

use anyhow::{Result, anyhow};
use eframe::egui;
use skrifa::{FontRef, MetadataProvider, raw::FileRef};

#[derive(Debug, Clone, PartialEq, Eq)]
struct FontCandidate {
    path: PathBuf,
    preferred_face: Option<u32>,
}

#[derive(Debug)]
pub(crate) struct InstalledFont {
    pub(crate) cjk_path: PathBuf,
    pub(crate) cjk_face_index: u32,
    #[cfg(windows)]
    pub(crate) latin_path: PathBuf,
}

const CJK_FONT_NAME: &str = "grok-bridge-cjk";
const CHINESE_GLYPH_PROBE: &str = "中文状态测试";
#[cfg(windows)]
const LATIN_FONT_NAME: &str = "grok-bridge-consolas";
#[cfg(windows)]
const LATIN_GLYPH_PROBE: &str = "Consolas AaZz0123";

pub(crate) fn install_cjk_font(context: &egui::Context) -> Result<InstalledFont> {
    #[cfg(windows)]
    let (latin_path, latin_bytes) = load_windows_consolas()?;

    let mut read_errors = Vec::new();
    for candidate in cjk_font_candidates()? {
        match fs::read(&candidate.path) {
            Ok(bytes) if !bytes.is_empty() => {
                let Some(face_index) = find_chinese_face(&bytes, candidate.preferred_face) else {
                    read_errors.push(format!(
                        "{} has no usable face covering {CHINESE_GLYPH_PROBE}",
                        candidate.path.display()
                    ));
                    continue;
                };
                #[cfg(windows)]
                context.set_fonts(font_definitions_with_windows_fonts(
                    latin_bytes,
                    bytes,
                    face_index,
                ));
                #[cfg(not(windows))]
                context.set_fonts(font_definitions_with_cjk(bytes, face_index));
                return Ok(InstalledFont {
                    cjk_path: candidate.path,
                    cjk_face_index: face_index,
                    #[cfg(windows)]
                    latin_path,
                });
            }
            Ok(_) => read_errors.push(format!("{} is empty", candidate.path.display())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => read_errors.push(format!("{}: {error}", candidate.path.display())),
        }
    }
    if read_errors.is_empty() {
        Err(anyhow!(
            "no supported system font was found; set GROK_BRIDGE_CJK_FONT"
        ))
    } else {
        Err(anyhow!(read_errors.join("; ")))
    }
}

#[cfg(windows)]
fn load_windows_consolas() -> Result<(PathBuf, Vec<u8>)> {
    let windows = env::var_os("WINDIR").unwrap_or_else(|| "C:\\Windows".into());
    let path = PathBuf::from(windows).join("Fonts").join("consola.ttf");
    let bytes = fs::read(&path)
        .map_err(|error| anyhow!("cannot read Consolas at {}: {error}", path.display()))?;
    if find_latin_face(&bytes).is_none() {
        return Err(anyhow!(
            "Consolas at {} does not cover {LATIN_GLYPH_PROBE}",
            path.display()
        ));
    }
    Ok((path, bytes))
}

fn find_chinese_face(bytes: &[u8], preferred_face: Option<u32>) -> Option<u32> {
    if let Some(index) = preferred_face {
        return FontRef::from_index(bytes, index)
            .ok()
            .filter(font_covers_chinese)
            .map(|_| index);
    }

    FileRef::new(bytes)
        .ok()?
        .fonts()
        .enumerate()
        .find_map(|(index, font)| font.ok().filter(font_covers_chinese).map(|_| index as u32))
}

fn font_covers_chinese(font: &FontRef<'_>) -> bool {
    let charmap = font.charmap();
    CHINESE_GLYPH_PROBE
        .chars()
        .all(|character| charmap.map(character).is_some())
}

#[cfg(windows)]
fn find_latin_face(bytes: &[u8]) -> Option<u32> {
    FileRef::new(bytes)
        .ok()?
        .fonts()
        .enumerate()
        .find_map(|(index, font)| font.ok().filter(font_covers_latin).map(|_| index as u32))
}

#[cfg(windows)]
fn font_covers_latin(font: &FontRef<'_>) -> bool {
    let charmap = font.charmap();
    LATIN_GLYPH_PROBE
        .chars()
        .all(|character| charmap.map(character).is_some())
}

#[cfg(not(windows))]
fn font_definitions_with_cjk(bytes: Vec<u8>, face_index: u32) -> egui::FontDefinitions {
    let mut fonts = egui::FontDefinitions::default();
    let mut font_data = egui::FontData::from_owned(bytes);
    font_data.index = face_index;
    fonts
        .font_data
        .insert(CJK_FONT_NAME.to_owned(), Arc::new(font_data));
    for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
        fonts
            .families
            .entry(family)
            .or_default()
            .push(CJK_FONT_NAME.to_owned());
    }
    fonts
}

#[cfg(windows)]
fn font_definitions_with_windows_fonts(
    latin_bytes: Vec<u8>,
    cjk_bytes: Vec<u8>,
    cjk_face_index: u32,
) -> egui::FontDefinitions {
    let mut fonts = egui::FontDefinitions::default();
    fonts.font_data.insert(
        LATIN_FONT_NAME.to_owned(),
        Arc::new(egui::FontData::from_owned(latin_bytes)),
    );

    let mut cjk_font_data = egui::FontData::from_owned(cjk_bytes);
    cjk_font_data.index = cjk_face_index;
    fonts
        .font_data
        .insert(CJK_FONT_NAME.to_owned(), Arc::new(cjk_font_data));

    for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
        let family_fonts = fonts.families.entry(family).or_default();
        family_fonts.insert(0, LATIN_FONT_NAME.to_owned());
        family_fonts.insert(1, CJK_FONT_NAME.to_owned());
    }
    fonts
}

fn cjk_font_candidates() -> Result<Vec<FontCandidate>> {
    let mut paths = Vec::new();
    if let Some(path) = env::var_os("GROK_BRIDGE_CJK_FONT") {
        let preferred_face = env::var("GROK_BRIDGE_CJK_FONT_INDEX")
            .ok()
            .map(|value| {
                value.parse::<u32>().map_err(|_| {
                    anyhow!("GROK_BRIDGE_CJK_FONT_INDEX must be a non-negative integer")
                })
            })
            .transpose()?;
        push_candidate(&mut paths, PathBuf::from(path), preferred_face);
    }

    #[cfg(windows)]
    {
        let windows = env::var_os("WINDIR").unwrap_or_else(|| "C:\\Windows".into());
        let fonts = PathBuf::from(windows).join("Fonts");
        push_font_names(
            &mut paths,
            &fonts,
            &[
                "msyh.ttc",
                "msyhl.ttc",
                "simhei.ttf",
                "Deng.ttf",
                "simsun.ttc",
            ],
        );
        if let Some(local_app_data) = env::var_os("LOCALAPPDATA") {
            let user_fonts = PathBuf::from(local_app_data)
                .join("Microsoft")
                .join("Windows")
                .join("Fonts");
            push_font_names(
                &mut paths,
                &user_fonts,
                &[
                    "NotoSansCJKsc-Regular.otf",
                    "SourceHanSansSC-Regular.otf",
                    "msyh.ttc",
                    "simhei.ttf",
                ],
            );
        }
    }

    #[cfg(target_os = "macos")]
    {
        for path in [
            "/System/Library/Fonts/PingFang.ttc",
            "/System/Library/Fonts/Hiragino Sans GB.ttc",
            "/System/Library/Fonts/Supplemental/Songti.ttc",
            "/Library/Fonts/NotoSansCJKsc-Regular.otf",
            "/Library/Fonts/SourceHanSansSC-Regular.otf",
        ] {
            push_candidate(&mut paths, PathBuf::from(path), None);
        }
        if let Some(home) = env::var_os("HOME") {
            let user_fonts = PathBuf::from(home).join("Library").join("Fonts");
            push_font_names(
                &mut paths,
                &user_fonts,
                &["NotoSansCJKsc-Regular.otf", "SourceHanSansSC-Regular.otf"],
            );
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Some(path) = fontconfig_chinese_font() {
            push_candidate(&mut paths, path, None);
        }
        for path in [
            "/usr/share/fonts/opentype/noto/NotoSansCJKsc-Regular.otf",
            "/usr/share/fonts/opentype/source-han-sans/SourceHanSansSC-Regular.otf",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
            "/usr/share/fonts/adobe-source-han-sans/SourceHanSansSC-Regular.otf",
        ] {
            push_candidate(&mut paths, PathBuf::from(path), None);
        }
        if let Some(home) = env::var_os("HOME") {
            let home = PathBuf::from(home);
            for directory in [home.join(".local/share/fonts"), home.join(".fonts")] {
                push_font_names(
                    &mut paths,
                    &directory,
                    &[
                        "NotoSansCJKsc-Regular.otf",
                        "SourceHanSansSC-Regular.otf",
                        "wqy-microhei.ttc",
                    ],
                );
            }
        }
    }

    Ok(paths)
}

fn push_font_names(paths: &mut Vec<FontCandidate>, directory: &std::path::Path, names: &[&str]) {
    for name in names {
        push_candidate(paths, directory.join(name), None);
    }
}

fn push_candidate(paths: &mut Vec<FontCandidate>, path: PathBuf, preferred_face: Option<u32>) {
    if !paths.iter().any(|candidate| candidate.path == path) {
        paths.push(FontCandidate {
            path,
            preferred_face,
        });
    }
}

#[cfg(target_os = "linux")]
fn fontconfig_chinese_font() -> Option<PathBuf> {
    let output = std::process::Command::new("fc-match")
        .args(["-f", "%{file}\\n", "sans:lang=zh-cn"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = String::from_utf8(output.stdout).ok()?;
    let path = path.lines().next()?.trim();
    (!path.is_empty()).then(|| PathBuf::from(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(windows))]
    #[test]
    fn installs_cjk_as_a_fallback_for_both_font_families() {
        let fonts = font_definitions_with_cjk(vec![1, 2, 3], 7);
        assert!(fonts.font_data.contains_key(CJK_FONT_NAME));
        assert_eq!(fonts.font_data[CJK_FONT_NAME].index, 7);
        for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
            assert_eq!(
                fonts
                    .families
                    .get(&family)
                    .and_then(|fonts| fonts.last())
                    .map(String::as_str),
                Some(CJK_FONT_NAME)
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_font_chain_prefers_consolas_then_cjk_for_both_families() {
        let fonts = font_definitions_with_windows_fonts(vec![1, 2, 3], vec![4, 5, 6], 7);
        assert!(fonts.font_data.contains_key(LATIN_FONT_NAME));
        assert!(fonts.font_data.contains_key(CJK_FONT_NAME));
        assert_eq!(fonts.font_data[CJK_FONT_NAME].index, 7);
        for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
            let family_fonts = fonts.families.get(&family).unwrap();
            assert_eq!(
                family_fonts.first().map(String::as_str),
                Some(LATIN_FONT_NAME)
            );
            assert_eq!(family_fonts.get(1).map(String::as_str), Some(CJK_FONT_NAME));
        }
    }

    #[test]
    fn rejects_invalid_and_non_cjk_font_data() {
        assert_eq!(find_chinese_face(&[1, 2, 3], None), None);

        let default_fonts = egui::FontDefinitions::default();
        let latin_font = &default_fonts.font_data["Ubuntu-Light"].font;
        assert_eq!(find_chinese_face(latin_font.as_ref(), None), None);
    }

    #[cfg(windows)]
    #[test]
    fn finds_a_verified_windows_chinese_font_when_installed() {
        let Some((path, bytes, face_index)) = cjk_font_candidates()
            .unwrap()
            .into_iter()
            .filter_map(|candidate| {
                let bytes = fs::read(&candidate.path).ok()?;
                let face_index = find_chinese_face(&bytes, candidate.preferred_face)?;
                Some((candidate.path, bytes, face_index))
            })
            .next()
        else {
            return;
        };

        let font = FontRef::from_index(&bytes, face_index).unwrap();
        assert!(font_covers_chinese(&font), "{}", path.display());
    }

    #[cfg(windows)]
    #[test]
    fn finds_a_verified_windows_consolas_font_when_installed() {
        let Ok((path, bytes)) = load_windows_consolas() else {
            return;
        };
        let face_index = find_latin_face(&bytes).unwrap();
        let font = FontRef::from_index(&bytes, face_index).unwrap();
        assert!(font_covers_latin(&font), "{}", path.display());
    }
}
