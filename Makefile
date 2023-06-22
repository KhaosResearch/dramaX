.PHONY: install install-dev build format lint static-check tests

install:
	@python -m pip install .

install-dev:
	@python -m pip install -e .

build:
	@python -m build

format:
	@python -m black src/ tests/ examples/

lint:
	@python -m ruff src/ tests/ examples/

static-check:
	@python -m mypy src/ tests/

tests:
	@python -m pytest