#!/bin/bash

# Update README shields with current metrics
# Usage: ./scripts/update-shields.sh

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🛡️  Updating README shields...${NC}"

# Check if Poetry is available
if ! command -v poetry &> /dev/null; then
    echo -e "${RED}❌ Poetry is not installed or not in PATH${NC}"
    exit 1
fi

# Get current metrics
echo -e "${YELLOW}📊 Gathering current metrics...${NC}"

# Get version
VERSION=$(poetry version --short)
echo -e "   Version: $VERSION"

# Run tests and get coverage
COVERAGE_OUTPUT=$(poetry run pytest --cov=src/data_quality --cov-report=term-missing --quiet 2>/dev/null)
COVERAGE=$(echo "$COVERAGE_OUTPUT" | grep "TOTAL" | awk '{print $4}' | sed 's/%//' || echo "0")
echo -e "   Coverage: ${COVERAGE}%"

# Count total tests
TOTAL_TESTS=$(poetry run pytest --collect-only -q 2>/dev/null | grep "test" | wc -l | tr -d ' ')
echo -e "   Tests: $TOTAL_TESTS"

# Determine coverage color
if [ "$COVERAGE" -ge 90 ]; then
    COVERAGE_COLOR="brightgreen"
elif [ "$COVERAGE" -ge 80 ]; then
    COVERAGE_COLOR="yellow"
elif [ "$COVERAGE" -ge 70 ]; then
    COVERAGE_COLOR="orange"
else
    COVERAGE_COLOR="red"
fi

# Update README.md shields
echo -e "${YELLOW}📝 Updating README.md...${NC}"

# Backup README
cp README.md README.md.bak

# Update version shield
sed -i "s/version-[^-]*-blue/version-${VERSION}-blue/g" README.md

# Update coverage shield
sed -i "s/coverage-[^%]*%25-[^)]*)/coverage-${COVERAGE}%25-${COVERAGE_COLOR})/g" README.md

# Update tests shield
sed -i "s/tests-[^%]*%20passed-brightgreen/tests-${TOTAL_TESTS}%20passed-brightgreen/g" README.md

# Check if changes were made
if diff README.md README.md.bak > /dev/null; then
    echo -e "${BLUE}ℹ️  No changes needed${NC}"
    rm README.md.bak
else
    echo -e "${GREEN}✅ Shields updated successfully!${NC}"
    echo -e "${BLUE}📋 Changes made:${NC}"
    echo -e "   Version: ${VERSION}"
    echo -e "   Coverage: ${COVERAGE}% (${COVERAGE_COLOR})"
    echo -e "   Tests: ${TOTAL_TESTS} passed"

    rm README.md.bak

    # Ask if user wants to commit changes
    echo ""
    read -p "🤔 Commit these changes? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git add README.md
        git commit -m "chore: update README shields with latest metrics

- Version: ${VERSION}
- Coverage: ${COVERAGE}%
- Tests: ${TOTAL_TESTS} passed"
        echo -e "${GREEN}✅ Changes committed${NC}"
    else
        echo -e "${BLUE}ℹ️  Changes not committed${NC}"
    fi
fi

echo -e "${GREEN}🎉 Shield update completed!${NC}"
