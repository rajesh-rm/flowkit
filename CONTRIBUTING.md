# Contributing

## Setup

See [docs/quickstart-dev.md](docs/quickstart-dev.md) for environment setup (uv, venv, enterprise proxy).

## Branch and PR Workflow

- **Never push directly to `main`.** All changes go through feature branches + PR.
- One branch per task: `feature/<name>`, `fix/<name>`, `docs/<name>`.
- Keep PRs small and focused — one concern per PR.
- After merge: switch to `main`, pull, delete the local branch.

## Code Style

| Tool | Config | Command |
|------|--------|---------|
| **ruff** | `pyproject.toml` — rules E, F, I, N, W, UP; line-length 100 | `make lint` |
| **mypy** | `pyproject.toml` — strict mode, Python 3.11 | `make typecheck` |
| **ruff fix** | Auto-fix safe lint issues | `make lint-fix` |

Run before every commit:
```bash
make lint && make typecheck
```

## Testing

- **Coverage target: 90%+** (currently ~95%).
- Unit tests for all modules. Integration tests for end-to-end flows.
- Unit tests must run without Docker or external services (mock APIs, mock DB).
- Integration tests use testcontainers for Postgres (Docker required).

```bash
make test-unit          # Fast — no Docker
make test-integration   # Requires Docker
make test-cov           # Unit tests with coverage report
```

See [docs/testing.md](docs/testing.md) for the full guide — directory structure, fixtures, patterns, and debugging.

## Commit Messages

- Imperative mood: "Add feature" not "Added feature"
- First line: concise summary (< 70 chars)
- Body: explain **why**, not what (the diff shows what)

## Adding a New Asset

See [docs/extending.md](docs/extending.md) for the step-by-step guide. Quick checklist:

1. Create asset class with `@register` decorator
2. Add `__init__.py` import
3. Add JSON fixture in `tests/fixtures/<source>/`
4. Add unit test in `tests/unit/assets/`
5. Run `make test-unit` to verify

## PR Checklist

Before requesting review:

- [ ] `make test-unit` passes
- [ ] `make lint` clean
- [ ] `make typecheck` clean (recommended)
- [ ] New/changed functionality has tests
- [ ] Documentation updated if API changed
- [ ] No secrets or credentials in committed files
