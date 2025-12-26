# Dataset Inspector

## General Instructions
- Always `use context7` for the most recent docs and best practices.
- All comments and documentations in English.
- Include only brief end-user instructions in the root README.md file.
- Place detailed development documentation in docs/*.md (use lowercase filenames).
- Always prioritize ast-grep (cmd: `sg`) over regex/string-replace for code manipulation, using AST patterns to ensure structural accuracy and avoid syntax errors. Examples:
    1. Swap Args: `sg run -p 'fn($A, $B)' -r 'fn($B, $A)'`
    2. Wrap Error: `sg run -p 'return $E' -r 'return wrap($E)'`
    3. API Update: `sg run -p 'user.id' -r 'user.get_id()'`

## Python Instructions
- Always use `uv` for python package manager. The `.venv` is located in the project root.

## Tauri App Instructions

### Tech Stack
- **Platform**: Tauri v2 + Rust (Tokio async runtime)
- **Frontend**: React 19 + Vite + TanStack (Router, Query, Form)
- **Styling**: Tailwind CSS v4 + NextUI
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
- Use NextUI components first
- Root layout: `select-none cursor-default h-screen overflow-hidden`
- NextUI components + Motion for micro-interactions
- Add page transitions, hover effects, skeleton loading
- Use NextUI theming for consistent design

### Security Rules
- Minimize Tauri capabilities exposure
- Validate all inputs in Rust layer

## The Architect's Decree
- I want to move faster. Please execute the entire plan (Steps 1 through x) in a single pass right now. Do not stop to ask for confirmation between steps. I am comfortable reviewing a large set of changes.
- Please batch these changes together. Instead of small increments, I need you to implement the full scope of features in this response. Treat this as a single, atomic refactor. Go ahead and write the complete implementation for all points listed above.
- Stop prioritizing 'safe, small increments' for this task. I explicitly authorize a comprehensive refactor. I need the system to be functional after your next response, so please proceed with implementing all x items immediately. Don't wait for a 'next' command.
- If the output is too long, please implement the first half, and then automatically continue with the second half in your immediate next message without waiting for my input. Just get it all done.
