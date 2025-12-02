#!/bin/bash
set -e

echo "🚀 Initializing XLT development environment..."

# Install the package in editable mode with dev dependencies using uv
echo "📦 Installing xlt in editable mode with uv..."
uv pip install --system -e ".[dev]"

# Verify installation
echo "✅ Verifying xlt CLI installation..."
xlt --version

echo "✨ Development environment ready!"
