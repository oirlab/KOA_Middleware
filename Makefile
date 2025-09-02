# Makefile for KOA_Middleware

.PHONY: all help docs test install clean

# Default target
all: help

help:
	@echo "Available commands:"
	@echo "  help     - Display this help message"
	@echo "  docs     - Build the HTML documentation"
	@echo "  test     - Run all tests, including notebook tests"
	@echo "  install  - Install project dependencies (including test and docs extras)"
	@echo "  clean    - Clean build artifacts and Python caches"

docs:
	@echo "Building HTML documentation..."
	@uv run make -C docs html

test:
	@echo "Running tests..."
	@uv run pytest

install:
	@echo "Installing project dependencies..."
	@uv pip install .[test,docs]

clean:
	@echo "Cleaning build artifacts and Python caches..."
	@rm -rf build/
	@rm -rf dist/
	@rm -rf .pytest_cache/
	@rm -rf *.egg-info/
	@rm -rf docs/build/
	@find . -name "__pycache__" -type d -exec rm -rf {} +