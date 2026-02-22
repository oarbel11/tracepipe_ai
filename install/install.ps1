# Debug AI - Windows PowerShell installer
# Run from your IDE terminal (VS Code, Cursor, etc.) — works in cmd, PowerShell, and Git Bash:
#   powershell -NoProfile -ExecutionPolicy Bypass -Command "curl.exe -fsSL 'https://raw.githubusercontent.com/oarbel11/tracepipe_ai/master/install/install.ps1' -o install.ps1; .\install.ps1"
#
# Repository: https://github.com/oarbel11/tracepipe_ai

$ErrorActionPreference = "Stop"

function Pause-BeforeExit {
    Write-Host ""
    Read-Host "Press Enter to close"
}

try {
    Write-Host ""
    Write-Host "===================================================================" -ForegroundColor Cyan
    Write-Host "                    Tracepipe AI Installer                          " -ForegroundColor Cyan
    Write-Host "===================================================================" -ForegroundColor Cyan
    Write-Host ""

    # Detect git repo URL (default to main branch)
    $RepoUrl = if ($env:TRACEPIPE_AI_REPO) { $env:TRACEPIPE_AI_REPO } else { "https://github.com/oarbel11/tracepipe_ai.git" }
    $Branch = if ($env:TRACEPIPE_AI_BRANCH) { $env:TRACEPIPE_AI_BRANCH } else { "master" }
    $InstallDir = if ($env:TRACEPIPE_AI_DIR) { $env:TRACEPIPE_AI_DIR } else { "./tracepipe_ai" }

    # Check if directory already exists
    if (Test-Path $InstallDir) {
        Write-Host "⚠️  Directory '$InstallDir' already exists." -ForegroundColor Yellow
        $response = Read-Host "   Remove it and reinstall? (y/N)"
        if ($response -ne "y" -and $response -ne "Y") {
            Write-Host "❌ Installation cancelled." -ForegroundColor Red
            Pause-BeforeExit
            exit 1
        }
        Remove-Item -Recurse -Force $InstallDir
    }

    # Check git
    Write-Host "📦 Checking git..." -ForegroundColor Cyan
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        Write-Host "❌ Git is not installed." -ForegroundColor Red
        Write-Host "   Install from: https://git-scm.com/downloads/win" -ForegroundColor Yellow
        Pause-BeforeExit
        exit 1
    }
    Write-Host "   ✅ Git found" -ForegroundColor Green

    # Clone repository
    Write-Host ""
    Write-Host "📦 Cloning Tracepipe AI..." -ForegroundColor Cyan
    try {
        git clone -b $Branch $RepoUrl $InstallDir
        if ($LASTEXITCODE -ne 0) { throw "Git clone failed" }
    } catch {
        Write-Host "❌ Failed to clone repository: $_" -ForegroundColor Red
        Pause-BeforeExit
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
                Pause-BeforeExit
                exit 1
            }
        }
    } catch {
        Write-Host "❌ Python is not installed or not in PATH." -ForegroundColor Red
        Write-Host "   Install Python 3.8+ from: https://www.python.org/downloads/" -ForegroundColor Yellow
        Pause-BeforeExit
        exit 1
    }

    # Create virtual environment
    Write-Host ""
    Write-Host "📦 Creating virtual environment..." -ForegroundColor Cyan
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to create virtual environment" -ForegroundColor Red
        Pause-BeforeExit
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
    python -m pip install --quiet -r requirements.txt
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to install dependencies" -ForegroundColor Red
        Pause-BeforeExit
        exit 1
    }

    # Verify installation
    Write-Host ""
    Write-Host "✅ Installation complete!" -ForegroundColor Green
    Write-Host ""

    # Run setup wizard (handles lineage build + peer review prompt)
    Write-Host "-------------------------------------------------------------------" -ForegroundColor Cyan
    Write-Host ""
    python scripts/setup_wizard.py

    Write-Host ""
}
catch {
    Write-Host ""
    Write-Host "❌ Error: $_" -ForegroundColor Red
    Write-Host $_.ScriptStackTrace -ForegroundColor DarkGray
}
finally {
    Pause-BeforeExit
}
