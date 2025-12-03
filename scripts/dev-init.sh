#!/bin/bash
set -e

echo "🚀 Initializing DXT development environment..."

# Install the package in editable mode with dev dependencies using uv
echo "📦 Installing dxt in editable mode with uv..."
uv pip install --system -e ".[dev]"

# Verify installation
echo "✅ Verifying dxt CLI installation..."
dxt --version

# Restore the dvdrental sample database
echo ""
bash scripts/restore-dvdrental.sh

echo ""
echo "✨ Development environment ready!"
