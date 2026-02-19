# Debug AI - Windows PowerShell installer
# 
# Usage: irm https://raw.githubusercontent.com/oarbel11/debug_ai/main/install/install.ps1 | iex
# 
# Repository: https://github.com/oarbel11/debug_ai
#            Users can also set DEBUG_AI_REPO env var to use a custom repo URL.

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                    Debug AI Installer                              ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Detect git repo URL (default to main branch)
$RepoUrl = if ($env:DEBUG_AI_REPO) { $env:DEBUG_AI_REPO } else { "https://github.com/oarbel11/debug_ai.git" }
$Branch = if ($env:DEBUG_AI_BRANCH) { $env:DEBUG_AI_BRANCH } else { "main" }
$InstallDir = if ($env:DEBUG_AI_DIR) { $env:DEBUG_AI_DIR } else { "./debug_ai" }

# Check if directory already exists
if (Test-Path $InstallDir) {
    Write-Host "⚠️  Directory '$InstallDir' already exists." -ForegroundColor Yellow
    $response = Read-Host "   Remove it and reinstall? (y/N)"
    if ($response -ne "y" -and $response -ne "Y") {
        Write-Host "❌ Installation cancelled." -ForegroundColor Red
        exit 1
    }
    Remove-Item -Recurse -Force $InstallDir
}

# Check git
Write-Host "📦 Checking git..." -ForegroundColor Cyan
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Host "❌ Git is not installed." -ForegroundColor Red
    Write-Host "   Install from: https://git-scm.com/downloads/win" -ForegroundColor Yellow
    exit 1
}
Write-Host "   ✅ Git found" -ForegroundColor Green

# Clone repository
Write-Host ""
Write-Host "📦 Cloning Debug AI..." -ForegroundColor Cyan
try {
    git clone -b $Branch $RepoUrl $InstallDir
    if ($LASTEXITCODE -ne 0) { throw "Git clone failed" }
} catch {
    Write-Host "❌ Failed to clone repository: $_" -ForegroundColor Red
    exit 1
}

Set-Location $InstallDir

# Check Python
Write-Host ""
Write-Host "🐍 Checking Python..." -ForegroundColor Cyan
try {
    $pythonVersion = python --version 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Python not found" }
    Write-Host "   ✅ $pythonVersion found" -ForegroundColor Green
    
    # Check Python version (3.8+)
    $versionMatch = $pythonVersion -match "Python (\d+)\.(\d+)"
    if ($versionMatch) {
        $major = [int]$matches[1]
        $minor = [int]$matches[2]
        if ($major -lt 3 -or ($major -eq 3 -and $minor -lt 8)) {
            Write-Host "❌ Python 3.8+ required. Found: $pythonVersion" -ForegroundColor Red
            exit 1
        }
    }
} catch {
    Write-Host "❌ Python is not installed or not in PATH." -ForegroundColor Red
    Write-Host "   Install Python 3.8+ from: https://www.python.org/downloads/" -ForegroundColor Yellow
    exit 1
}

# Create virtual environment
Write-Host ""
Write-Host "📦 Creating virtual environment..." -ForegroundColor Cyan
python -m venv .venv
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to create virtual environment" -ForegroundColor Red
    exit 1
}

# Activate virtual environment
Write-Host "   Activating virtual environment..." -ForegroundColor Cyan
& .\.venv\Scripts\Activate.ps1

# Upgrade pip
Write-Host "   Upgrading pip..." -ForegroundColor Cyan
python -m pip install --quiet --upgrade pip

# Install dependencies
Write-Host ""
Write-Host "📥 Installing dependencies..." -ForegroundColor Cyan
pip install --quiet -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to install dependencies" -ForegroundColor Red
    exit 1
}

# Verify installation
Write-Host ""
Write-Host "✅ Installation complete!" -ForegroundColor Green
Write-Host ""

# Run setup wizard
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
Write-Host ""
Write-Host "🚀 Starting setup wizard..." -ForegroundColor Yellow
Write-Host ""
python scripts/setup_wizard.py

Write-Host ""
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
Write-Host ""
Write-Host "📖 Full documentation: See README.md" -ForegroundColor Cyan
Write-Host ""
