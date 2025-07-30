#!/bin/bash

# Git Flow Helper Script for Tupy Data Quality
# Usage: ./scripts/git-flow-helper.sh <command> [args]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if we're in a git repository
check_git_repo() {
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        error "Not in a git repository"
        exit 1
    fi
}

# Get current branch
get_current_branch() {
    git branch --show-current
}

# Check if branch exists
branch_exists() {
    git show-ref --verify --quiet refs/heads/"$1"
}

# Start a new feature
start_feature() {
    local feature_name="$1"

    if [ -z "$feature_name" ]; then
        error "Feature name is required"
        echo "Usage: $0 start-feature <feature-name>"
        echo "Example: $0 start-feature add-postgres-connector"
        exit 1
    fi

    local branch_name="feature/$feature_name"

    if branch_exists "$branch_name"; then
        error "Branch $branch_name already exists"
        exit 1
    fi

    info "Starting feature: $feature_name"

    # Switch to develop and update
    git checkout develop
    git pull origin develop

    # Create and checkout feature branch
    git checkout -b "$branch_name"

    success "Feature branch '$branch_name' created and checked out"
    info "You can now start working on your feature"
    info "Remember to make small, focused commits following conventional commit format"
}

# Finish a feature (create PR)
finish_feature() {
    local current_branch=$(get_current_branch)

    if [[ ! "$current_branch" =~ ^feature/ ]]; then
        error "Not on a feature branch. Current branch: $current_branch"
        exit 1
    fi

    local feature_name="${current_branch#feature/}"

    info "Finishing feature: $feature_name"

    # Push the feature branch
    git push origin "$current_branch"

    # Create pull request using gh CLI if available
    if command -v gh &> /dev/null; then
        info "Creating pull request..."

        # Get the last few commits for PR body
        local commits=$(git log develop..HEAD --pretty=format:"- %s" | head -10)

        gh pr create \
            --title "feat: $feature_name" \
            --body "$(cat <<EOF
## 📋 Summary
Feature implementation: $feature_name

## 🔄 Changes
$commits

## 🧪 Test Plan
- [ ] Unit tests passing
- [ ] Integration tests verified
- [ ] Documentation updated
- [ ] CLI integration tested

## 📚 Documentation
- [ ] Code documentation updated
- [ ] User documentation updated
- [ ] Examples provided

🤖 Generated with Git Flow Helper
EOF
)" \
            --base develop

        success "Pull request created successfully"
    else
        warning "gh CLI not found. Please create PR manually at:"
        echo "https://github.com/$(git remote get-url origin | sed 's/.*github.com[:/]\([^/]*\/[^/]*\).*/\1/' | sed 's/\.git$//')/compare/develop...$current_branch"
    fi
}

# Start a hotfix
start_hotfix() {
    local hotfix_name="$1"

    if [ -z "$hotfix_name" ]; then
        error "Hotfix name is required"
        echo "Usage: $0 start-hotfix <hotfix-name>"
        echo "Example: $0 start-hotfix fix-memory-leak"
        exit 1
    fi

    local branch_name="hotfix/$hotfix_name"

    if branch_exists "$branch_name"; then
        error "Branch $branch_name already exists"
        exit 1
    fi

    info "Starting hotfix: $hotfix_name"

    # Switch to main and update
    git checkout main
    git pull origin main

    # Create and checkout hotfix branch
    git checkout -b "$branch_name"

    success "Hotfix branch '$branch_name' created and checked out"
    warning "Remember: hotfixes should be minimal and focused on the critical issue"
}

# Create a release
start_release() {
    local version="$1"

    if [ -z "$version" ]; then
        error "Version is required"
        echo "Usage: $0 start-release <version>"
        echo "Example: $0 start-release 1.2.0"
        exit 1
    fi

    local branch_name="release/$version"

    if branch_exists "$branch_name"; then
        error "Branch $branch_name already exists"
        exit 1
    fi

    info "Starting release: $version"

    # Switch to develop and update
    git checkout develop
    git pull origin develop

    # Create and checkout release branch
    git checkout -b "$branch_name"

    # Update version in pyproject.toml
    poetry version "$version"
    git add pyproject.toml
    git commit -m "chore(release): bump version to $version"

    success "Release branch '$branch_name' created"
    info "Complete any final preparations and run: $0 finish-release $version"
}

# Finish a release
finish_release() {
    local version="$1"
    local current_branch=$(get_current_branch)

    if [ -z "$version" ]; then
        error "Version is required"
        echo "Usage: $0 finish-release <version>"
        exit 1
    fi

    if [ "$current_branch" != "release/$version" ]; then
        error "Not on release branch release/$version. Current: $current_branch"
        exit 1
    fi

    info "Finishing release: $version"

    # Generate changelog
    info "Generating changelog..."
    echo "# Changelog for $version" > "CHANGELOG-$version.md"
    echo "" >> "CHANGELOG-$version.md"
    git log --pretty=format:"- %s (%h)" develop..HEAD >> "CHANGELOG-$version.md"

    git add "CHANGELOG-$version.md"
    git commit -m "docs(release): add changelog for $version"

    # Merge to main
    git checkout main
    git pull origin main
    git merge --no-ff "release/$version"
    git tag -a "v$version" -m "Release version $version"

    # Merge back to develop
    git checkout develop
    git pull origin develop
    git merge --no-ff "release/$version"

    # Push everything
    git push origin main develop --tags

    # Clean up release branch
    git branch -d "release/$version"
    if git show-ref --verify --quiet "refs/remotes/origin/release/$version"; then
        git push origin --delete "release/$version"
    fi

    success "Release $version completed and tagged"
    info "GitHub Actions will handle the rest of the release process"
}

# Run tests with coverage
run_tests() {
    info "Running tests with coverage..."
    cd "$PROJECT_ROOT"

    poetry run pytest \
        --cov=src/data_quality \
        --cov-report=term-missing \
        --cov-report=html \
        --cov-fail-under=50

    success "Tests completed. Coverage report available in htmlcov/"
}

# Run linting
run_lint() {
    info "Running linting checks..."
    cd "$PROJECT_ROOT"

    echo "🔍 Running ruff..."
    poetry run ruff check src/ tests/

    echo "🔍 Running black..."
    poetry run black --check src/ tests/

    success "Linting completed"
}

# Fix linting issues
fix_lint() {
    info "Fixing linting issues..."
    cd "$PROJECT_ROOT"

    echo "🔧 Running black..."
    poetry run black src/ tests/

    echo "🔧 Running ruff with fixes..."
    poetry run ruff check --fix src/ tests/

    success "Linting fixes applied"
}

# Show status
show_status() {
    local current_branch=$(get_current_branch)

    echo "===================="
    echo "📊 Project Status"
    echo "===================="
    echo "Current branch: $current_branch"
    echo "Git status:"
    git status --short
    echo ""

    if [[ "$current_branch" =~ ^feature/ ]]; then
        echo "🔧 You're working on a feature branch"
        echo "Available commands:"
        echo "  $0 finish-feature   # Create PR for this feature"
        echo "  $0 run-tests        # Run test suite"
        echo "  $0 run-lint         # Check code quality"
    elif [[ "$current_branch" =~ ^hotfix/ ]]; then
        echo "🚨 You're working on a hotfix branch"
        echo "Available commands:"
        echo "  $0 run-tests        # Run test suite"
        echo "  $0 run-lint         # Check code quality"
    elif [ "$current_branch" = "develop" ]; then
        echo "🌱 You're on the develop branch"
        echo "Available commands:"
        echo "  $0 start-feature <name>   # Start new feature"
        echo "  $0 start-release <version> # Start new release"
    elif [ "$current_branch" = "main" ]; then
        echo "🚀 You're on the main branch"
        echo "Available commands:"
        echo "  $0 start-hotfix <name>    # Start critical hotfix"
    fi

    echo ""
    echo "General commands:"
    echo "  $0 run-tests        # Run test suite with coverage"
    echo "  $0 run-lint         # Run linting checks"
    echo "  $0 fix-lint         # Fix linting issues"
}

# Main command dispatcher
main() {
    check_git_repo

    case "${1:-}" in
        "start-feature")
            start_feature "$2"
            ;;
        "finish-feature")
            finish_feature
            ;;
        "start-hotfix")
            start_hotfix "$2"
            ;;
        "start-release")
            start_release "$2"
            ;;
        "finish-release")
            finish_release "$2"
            ;;
        "run-tests")
            run_tests
            ;;
        "run-lint")
            run_lint
            ;;
        "fix-lint")
            fix_lint
            ;;
        "status")
            show_status
            ;;
        "help"|"--help"|"-h"|"")
            echo "Git Flow Helper for Tupy Data Quality"
            echo ""
            echo "Usage: $0 <command> [args]"
            echo ""
            echo "Commands:"
            echo "  start-feature <name>     Start a new feature branch"
            echo "  finish-feature           Create PR for current feature"
            echo "  start-hotfix <name>      Start a hotfix branch"
            echo "  start-release <version>  Start a release branch"
            echo "  finish-release <version> Complete and tag a release"
            echo "  run-tests               Run test suite with coverage"
            echo "  run-lint                Run linting checks"
            echo "  fix-lint                Fix linting issues automatically"
            echo "  status                  Show current project status"
            echo "  help                    Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 start-feature add-postgres-support"
            echo "  $0 finish-feature"
            echo "  $0 start-release 1.2.0"
            echo "  $0 run-tests"
            ;;
        *)
            error "Unknown command: $1"
            echo "Run '$0 help' for usage information"
            exit 1
            ;;
    esac
}

main "$@"
