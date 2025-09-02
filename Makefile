# Makefile for KOA_Middleware

.PHONY: all help docs test install clean

# Check for uv and set commands accordingly
ifeq ($(shell command -v uv 2>/dev/null),)
    RUN_CMD = python -m
    PIP_CMD = python -m pip
else
    RUN_CMD = uv run
    PIP_CMD = uv pip
endif

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
	@$(RUN_CMD) make -C docs html

test:
	@echo "Running tests..."
	@$(RUN_CMD) pytest

install:
	@echo "Installing project dependencies..."
	@$(PIP_CMD) install .[test,docs]

clean:
	@echo "Cleaning build artifacts and Python caches..."
	@rm -rf build/
	@rm -rf dist/
	@rm -rf .pytest_cache/
	@rm -rf *.egg-info/
	@rm -rf docs/build/
	@find . -name "__pycache__" -type d -exec rm -rf {} +