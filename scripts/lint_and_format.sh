#!/bin/bash

# Run ruff check with auto-fix
ruff check . --fix

# Run ruff formatter
ruff format .
