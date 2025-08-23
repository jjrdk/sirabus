VERSION := 0.0.0

install_dependencies:
	uv sync --all-extras

update:
	# Update all dependencies to their latest versions
	uv sync --all-extras --upgrade

test: install_dependencies
	# Apply linter before running tests
	uv run ruff format
	# Run tests using behave
	uv run behave
	python3 -m junit_to_markdown

build: install_dependencies test
	# Build the project using uv
	uv version $(VERSION)
	uv build