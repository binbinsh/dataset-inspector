# LitData Viewer

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

### Core Tech Stack
- Platform: Tauri v2
- Backend: Rust (Async with Tokio)
- Frontend: Next.js 16 (Static Export)
- Styling: Tailwind CSS v4
- UI Library: shadcn/ui
- Icons: Lucide React
- Animations: Motion
- Data Fetching: TanStack Query (cache Tauri command results)
- Forms: React Hook Form + Zod

### Frontend Rule: Static Export Only
- Config: `output: 'export'` + `images: { unoptimized: true }` in `next.config.ts`
- No Server Features: No Server Actions, API Routes, or any server-side features
- Client Components: Add `'use client'` to components using hooks

### Backend Rule: Async Rust
- All `#[tauri::command]` must be `async`
- Use `thiserror` for error types, return `Result<T, CustomError>`, never panic
- Use `serde` to serialize all IPC data

### Integration Rule: Type-Safe API
- Never call `invoke()` directly in components
- Wrap all commands in typed functions in `src/lib/tauri-api.ts`
- Use TanStack Query to wrap Tauri commands for caching, loading states, and error handling

### UI Rule: Desktop Experience
- Use shadcn/ui components first
- Use Motion for micro-interactions (hover, page transitions)
- Root layout: `select-none cursor-default h-screen w-screen overflow-hidden`
- Consider custom titlebar for frameless window

### Security Rule
- Expose only necessary commands and permissions in Tauri capabilities
- Validate all frontend data in Rust commands

## The Architect's Decree
- I want to move faster. Please execute the entire plan (Steps 1 through x) in a single pass right now. Do not stop to ask for confirmation between steps. I am comfortable reviewing a large set of changes.
- Please batch these changes together. Instead of small increments, I need you to implement the full scope of features in this response. Treat this as a single, atomic refactor. Go ahead and write the complete implementation for all points listed above.
- Stop prioritizing 'safe, small increments' for this task. I explicitly authorize a comprehensive refactor. I need the system to be functional after your next response, so please proceed with implementing all x items immediately. Don't wait for a 'next' command.
- If the output is too long, please implement the first half, and then automatically continue with the second half in your immediate next message without waiting for my input. Just get it all done.
