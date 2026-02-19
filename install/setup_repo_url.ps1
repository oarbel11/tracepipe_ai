# Helper script to replace YOUR_USERNAME with your actual GitHub username
# Usage: .\setup_repo_url.ps1 YOUR_GITHUB_USERNAME

param(
    [Parameter(Mandatory=$true)]
    [string]$Username
)

Write-Host "Replacing YOUR_USERNAME with: $Username" -ForegroundColor Cyan
Write-Host ""

# Get all files to update
$files = Get-ChildItem -Recurse -Include *.sh,*.ps1,*.md -Exclude setup_repo_url.* | 
    Where-Object { $_.FullName -notmatch '\.git' -and $_.FullName -notmatch '\.venv' }

$updatedFiles = @()

foreach ($file in $files) {
    $content = Get-Content $file.FullName -Raw
    if ($content -match 'YOUR_USERNAME') {
        $newContent = $content -replace 'YOUR_USERNAME', $Username
        Set-Content -Path $file.FullName -Value $newContent -NoNewline
        $updatedFiles += $file.Name
    }
}

Write-Host "✅ Done! All occurrences of YOUR_USERNAME have been replaced with $Username" -ForegroundColor Green
Write-Host ""
Write-Host "Files updated:" -ForegroundColor Yellow
$updatedFiles | ForEach-Object { Write-Host "  - $_" }
