#!/bin/bash
# Helper script to replace YOUR_USERNAME with your actual GitHub username
# Usage: ./setup_repo_url.sh YOUR_GITHUB_USERNAME

if [ -z "$1" ]; then
    echo "Usage: ./setup_repo_url.sh YOUR_GITHUB_USERNAME"
    echo ""
    echo "Example: ./setup_repo_url.sh oarbel11"
    exit 1
fi

USERNAME="$1"
echo "Replacing YOUR_USERNAME with: $USERNAME"
echo ""

# Replace in all files
find . -type f \( -name "*.sh" -o -name "*.ps1" -o -name "*.md" \) ! -path "./.git/*" ! -path "./.venv/*" -exec sed -i.bak "s/YOUR_USERNAME/$USERNAME/g" {} \;

# Remove backup files
find . -name "*.bak" -delete

echo "✅ Done! All occurrences of YOUR_USERNAME have been replaced with $USERNAME"
echo ""
echo "Files updated:"
grep -r "$USERNAME" --include="*.sh" --include="*.ps1" --include="*.md" . | grep -v ".git" | cut -d: -f1 | sort -u
