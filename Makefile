VERSION := 0.0.0

install_dependencies:
	uv sync --all-extras

update:
	# Update all dependencies to their latest versions
	uv sync --all-extras --upgrade

lint:
	# Apply linter before running tests
	uv run ruff format

test: lint install_dependencies
	# Run tests using behave
	uv run behave

build: install_dependencies test
	# Build the project using uv
	uv version $(VERSION)
	uv build