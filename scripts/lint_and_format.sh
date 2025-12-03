#!/bin/bash

# Run ruff check with auto-fix
ruff check . --fix

# Run black formatter
black .
