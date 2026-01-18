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

## Flutter App Instructions

### Tech Stack
- **Platform**: Flutter (desktop)
- **State**: Provider + ChangeNotifier
- **Remote**: HTTP (Dart)
- **Styling**: Material 3 + custom ThemeData
- **Audio**: audioplayers

### Dart Rules
- Keep heavy dataset parsing inside `lib/services/*`.
- Avoid blocking the UI isolate; prefer async IO or isolates for large scans.
- Use `shared_preferences` for persisted settings.

### UI Rules
- Root layout uses `SafeArea` + `Scaffold`.
- Add hover feedback, simple transitions, and skeleton loading.
- Keep preview panes scrollable and selectable.

### Flutter Performance Best Practices
- Keep `build()` cheap: avoid heavy work in `build()`, split large widgets, localize `setState()`, reuse widget instances, and prefer `const` constructors with `StatelessWidget` for reusable UI.
- Use `StringBuffer` for repeated string concatenation inside loops.
- Treat `saveLayer()` as expensive: use only when required, precompute overlapping transparency when possible, avoid unnecessary overlap, and audit packages that trigger it.
- Minimize opacity and clipping: apply alpha directly when possible, prefer `FadeInImage` or `AnimatedOpacity` for fades, avoid `Clip.antiAliasWithSaveLayer`, and use `borderRadius` instead of clipping for rounded corners.
- Build lists and grids lazily with builder APIs; avoid concrete child lists when most items are offscreen.
- Avoid intrinsic layout passes by using fixed sizes or an anchor cell strategy; use DevTools layout tracking to spot intrinsic passes.
- Target <=16ms total frame time on 60Hz (<=8ms build + <=8ms render) to improve battery life, thermals, and low-end/120Hz device smoothness.
- Pitfalls: avoid opacity or clipping in animations, keep static subtrees out of `AnimatedBuilder` by using the `child` parameter, and avoid overriding `operator ==` on `Widget` except rare leaf cases.

### Security Rules
- Validate all inputs in Dart before processing.
- Restrict remote hosts for Hugging Face/Zenodo access.
