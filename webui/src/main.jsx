import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.jsx";
import { bootstrapWebUiCapability } from "./utils/webUiCapability.js";
import "@tabler/core/dist/css/tabler.min.css";
import "@xterm/xterm/css/xterm.css";
import "./index.css";

// Capture per-Runtime capability from the bootstrap URL before any API calls.
bootstrapWebUiCapability();

createRoot(document.getElementById("root")).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
