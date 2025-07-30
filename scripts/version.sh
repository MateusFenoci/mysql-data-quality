#!/bin/bash

# Version management script
# Usage: ./scripts/version.sh [major|minor|patch]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to display usage
usage() {
    echo -e "${BLUE}Usage: $0 [major|minor|patch]${NC}"
    echo ""
    echo -e "${YELLOW}Version Bump Types:${NC}"
    echo "  major  - Breaking changes (1.0.0 -> 2.0.0)"
    echo "  minor  - New features (1.0.0 -> 1.1.0)"
    echo "  patch  - Bug fixes (1.0.0 -> 1.0.1)"
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo "  $0 patch   # Bump patch version"
    echo "  $0 minor   # Bump minor version"
    echo "  $0 major   # Bump major version"
    exit 1
}

# Check if Poetry is available
if ! command -v poetry &> /dev/null; then
    echo -e "${RED}❌ Poetry is not installed or not in PATH${NC}"
    exit 1
fi

# Get bump type from argument
BUMP_TYPE=${1:-patch}

# Validate bump type
if [[ ! "$BUMP_TYPE" =~ ^(major|minor|patch)$ ]]; then
    echo -e "${RED}❌ Invalid version bump type: $BUMP_TYPE${NC}"
    usage
fi

# Get current version
CURRENT_VERSION=$(poetry version --short)
echo -e "${BLUE}📌 Current version: $CURRENT_VERSION${NC}"

# Bump version
echo -e "${YELLOW}🔄 Bumping $BUMP_TYPE version...${NC}"
poetry version $BUMP_TYPE

# Get new version
NEW_VERSION=$(poetry version --short)
echo -e "${GREEN}✅ New version: $NEW_VERSION${NC}"

# Update changelog
echo -e "${YELLOW}📝 Updating CHANGELOG.md...${NC}"
CHANGELOG_FILE="CHANGELOG.md"

# Create changelog if it doesn't exist
if [[ ! -f "$CHANGELOG_FILE" ]]; then
    cat > "$CHANGELOG_FILE" << EOF
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

EOF
fi

# Add new version entry to changelog
DATE=$(date '+%Y-%m-%d')
sed -i.bak "s/## \[Unreleased\]/## [Unreleased]\n\n## [$NEW_VERSION] - $DATE/" "$CHANGELOG_FILE"
rm "${CHANGELOG_FILE}.bak"

echo -e "${GREEN}✅ Updated $CHANGELOG_FILE${NC}"

# Create git commit
echo -e "${YELLOW}📝 Creating git commit...${NC}"
git add pyproject.toml "$CHANGELOG_FILE"
git commit -m "chore: bump version to $NEW_VERSION"

# Create git tag
echo -e "${YELLOW}🏷️  Creating git tag...${NC}"
git tag "v$NEW_VERSION"

echo -e "${GREEN}🎉 Version bump completed!${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Review the changes: git show"
echo "  2. Push to remote: git push origin main --tags"
echo "  3. Or push to develop: git push origin develop --tags"
echo ""
echo -e "${YELLOW}💡 The CI/CD pipeline will automatically:${NC}"
echo "  - Run tests and quality checks"
echo "  - Create a GitHub release"
echo "  - Update README shields"
echo "  - Deploy to appropriate environment"
