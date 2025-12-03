#!/bin/bash
set -e

echo "📀 Setting up dvdrental sample database..."

# Configuration
DB_NAME="dvdrental"
TAR_FILE="/workspace/assets/data/dvdrental.tar"
RESTORE_DIR="/tmp/dvdrental_restore"

# Wait for PostgreSQL to be ready (uses PGHOST, PGUSER from env)
echo "⏳ Waiting for PostgreSQL to be ready..."
until pg_isready -q; do
  echo "   PostgreSQL is unavailable - sleeping"
  sleep 2
done
echo "✅ PostgreSQL is ready!"

# Check if the database already exists (uses env vars)
if psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "✅ Database '$DB_NAME' already exists. Skipping restore."
    exit 0
fi

echo "📦 Creating database '$DB_NAME'..."
createdb "$DB_NAME"

echo "📂 Extracting dvdrental.tar to temporary directory..."
mkdir -p "$RESTORE_DIR"
tar -xf "$TAR_FILE" -C "$RESTORE_DIR"

echo "🔄 Restoring database from backup..."
pg_restore -d "$DB_NAME" -v "$RESTORE_DIR"

echo "🧹 Cleaning up temporary files..."
rm -rf "$RESTORE_DIR"

echo "✅ dvdrental database restored successfully!"
echo ""
echo "Connection details:"
echo "  Database: $DB_NAME"
echo "  Host: $PGHOST (internal)"
echo "  User: $PGUSER"
echo ""
echo "Quick test: psql -d dvdrental -c '\\dt'"
