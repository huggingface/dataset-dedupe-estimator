fmt:
    uv run ruff format de
    cargo fmt

lint:
    uv run ruff format --check de
    uv run ruff check de
    cargo fmt --check
    cargo check
    cargo clippy

typecheck:
    uv run mypy de

test:
    uv run pytest de
