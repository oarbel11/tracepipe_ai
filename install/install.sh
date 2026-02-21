#!/bin/bash
# Debug AI - One-line installer
# 
# Usage: curl -fsSL https://raw.githubusercontent.com/oarbel11/tracepipe_ai/master/install/install.sh | bash
# 
# Repository: https://github.com/oarbel11/tracepipe_ai
#            Users can also set TRACEPIPE_AI_REPO env var to use a custom repo URL.

set -e

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                    Tracepipe AI Installer                           ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Detect git repo URL (default to master branch)
REPO_URL="${TRACEPIPE_AI_REPO:-https://github.com/oarbel11/tracepipe_ai.git}"
BRANCH="${TRACEPIPE_AI_BRANCH:-master}"
INSTALL_DIR="${TRACEPIPE_AI_DIR:-./tracepipe_ai}"

# Check if directory already exists
if [ -d "$INSTALL_DIR" ]; then
    echo "⚠️  Directory '$INSTALL_DIR' already exists."
    read -p "   Remove it and reinstall? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Installation cancelled."
        exit 1
    fi
    rm -rf "$INSTALL_DIR"
fi

# Clone repository
echo "📦 Cloning Tracepipe AI..."
git clone -b "$BRANCH" "$REPO_URL" "$INSTALL_DIR" || {
    echo "❌ Failed to clone repository."
    echo "   Make sure git is installed: https://git-scm.com/downloads"
    exit 1
}

cd "$INSTALL_DIR"

# Check Python version
echo ""
echo "🐍 Checking Python..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed."
    echo "   Install Python 3.8+ from: https://www.python.org/downloads/"
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
if [ "$(printf '%s\n' "3.8" "$PYTHON_VERSION" | sort -V | head -n1)" != "3.8" ]; then
    echo "❌ Python 3.8+ required. Found: $(python3 --version)"
    exit 1
fi

echo "   ✅ Python $(python3 --version) found"

# Create virtual environment
echo ""
echo "📦 Creating virtual environment..."
python3 -m venv .venv

# Activate virtual environment
echo "   Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "   Upgrading pip..."
pip install --quiet --upgrade pip

# Install dependencies
echo ""
echo "📥 Installing dependencies..."
pip install --quiet -r requirements.txt

# Verify installation
echo ""
echo "✅ Installation complete!"
echo ""

# Run setup wizard
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "🚀 Starting setup wizard..."
echo ""
python scripts/setup_wizard.py

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📖 Full documentation: See README.md"
echo ""
