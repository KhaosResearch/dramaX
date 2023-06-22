.PHONY: install install-dev build format lint static-check tests

install:
	@python -m pip install .

install-dev:
	@python -m pip install -e .

build:
	@python -m build

format:
	@python -m black src/ examples/

lint:
	@python -m ruff src/ examples/

static-check:
	@python -m mypy src/

tests:
	@python -m pytest src/