# Development

## Prerequisites
- Node.js 20+
- Rust toolchain

## Commands
- Install deps: `npm install`
- Dev (web only): `npm run dev`
- Dev (Tauri): `npm run tauri dev`
- Build web assets: `npm run build` (outputs `dist/`)
- Build desktop app: `npm run tauri build`

## Frontend layout
- `src/main.tsx`: providers and app bootstrap
- `src/router.tsx`: TanStack Router setup and page transitions
- `src/routes/inspector.tsx`: primary UI and data flows
- `src/lib/tauri-api.ts`: all Tauri invoke wrappers
- `src/store/viewer.ts`: UI state store
- `src/styles/app.css` + `src/hero.ts`: Tailwind v4 + HeroUI theme

## Theming
- `src/hero.ts` defines the HeroUI theme (`atlas`)
- `src/styles/app.css` handles global typography, background, and Tailwind v4 tokens
