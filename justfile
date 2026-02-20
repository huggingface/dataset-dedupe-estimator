fmt:
    uv run ruff format de
    cargo fmt

lint:
    uv run ruff check de

typecheck:
    uv run mypy de

test:
    uv run pytest de/tests
