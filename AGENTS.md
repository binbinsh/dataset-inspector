# Dataset Inspector

Fastest response time, arbitrary viewing, and maximum file preview support.

## General Instructions
- Always `use context7` for the most recent docs and best practices.
- All comments and documentations in English.
- Include only brief end-user instructions in the root README.md file.
- Place detailed development documentation in docs/*.md (use lowercase filenames).
- Prioritize ast-grep (cmd: `sg`) over regex/string-replace for code manipulation, using AST patterns to ensure structural accuracy and avoid syntax errors.

## Python Instructions
- Always use `uv` for python package manager. The `.venv` is located in the project root.

## Tauri App Instructions

### Tech Stack
- **Platform**: Tauri v2 + Rust (Tokio async runtime)
- **Frontend**: React 19 + Vite + TanStack (Router, Query, Form)
- **Styling**: Tailwind CSS v4 + shadcn/ui
- **Remote**: gRPC (tonic + protobuf-ts)
- **Animations**: Motion (framer-motion)

### Rust Rules
- All `#[tauri::command]` must be async
- Use `thiserror` for errors, return `Result<T, CustomError>`
- gRPC client lives in Rust, frontend never connects directly

### Frontend Rules
- Wrap all Tauri commands in `src/lib/tauri-api.ts`
- Use TanStack Query for caching and state management
- Generate types from `.proto` files, keep types in sync

### UI Rules
- Use shadcn/ui components first
- Root layout: `select-none cursor-default h-screen overflow-hidden`
- shadcn/ui components + Motion for micro-interactions
- Add page transitions, hover effects, skeleton loading
- Use CSS variables + Tailwind for theming (config in `globals.css`)

### Security Rules
- Minimize Tauri capabilities exposure
- Validate all inputs in Rust layer

