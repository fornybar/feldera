# AGENTS.md

This file provides guidance for agentic coding agents working with the Feldera codebase.

## Build/Test Commands

**Rust (main codebase):**
- Build: `cargo build` or `cargo build --release`
- Test all: `cargo test`
- Test single crate: `cargo test -p <crate-name>` (e.g., `cargo test -p dbsp`)
- Lint: `cargo clippy`
- Format: `cargo fmt`

**Web Console (SvelteKit/TypeScript):**
- Install: `bun install` (uses Bun, not npm)
- Dev server: `bun run dev`
- Build: `bun run build`
- Test: `bun run test-e2e` or `bun run test-ct`
- Lint: `bun run lint`
- Format: `bun run format`

**Python client:**
- Test: `uv run pytest` or `pytest`
- Lint: `ruff check`
- Format: `ruff format`

## Code Style Guidelines

**Rust:**
- Follow standard Rust conventions (snake_case, proper error handling with Result<T, E>)
- Use `thiserror` for error types, `anyhow` for error handling
- Prefer explicit types over inference in public APIs
- Use `#[derive]` macros extensively (Debug, Clone, Serialize, etc.)
- Document public APIs with `///` comments

**TypeScript/Svelte:**
- Use Svelte 5 runes syntax (`$state`, `$derived`, `$effect`)
- Strict TypeScript configuration - all types must be explicit
- Use TailwindCSS for styling, Skeleton UI for components
- Auto-generated API client in `src/lib/services/manager/` - do not edit manually
- Use composition functions for state management in `src/lib/compositions/`

**Python:**
- Follow PEP 8, enforced by ruff
- Use type hints for all function signatures
- Use `pretty_errors` for enhanced error display