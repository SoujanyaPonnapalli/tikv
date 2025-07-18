#!/bin/bash

# TiKV Build Script - Clean, Concise, and Idempotent
# Supports building on active branch or specific versions

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-$SCRIPT_DIR}"
TIKV_VERSION="${TIKV_VERSION:-current}"  # Options: current, master, latest, or specific version like v8.5.2
BUILD_TYPE="${BUILD_TYPE:-release}"     # Options: debug, release
RUST_TOOLCHAIN="${RUST_TOOLCHAIN:-nightly-2023-12-28}"
CACHE_FILE="$BUILD_DIR/.build_cache"
LOG_FILE="$BUILD_DIR/build.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() { echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"; }

# Check if rebuild is needed
check_rebuild_needed() {
    if [[ ! -f "$CACHE_FILE" ]]; then
        return 0  # Rebuild needed
    fi

    local current_hash=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
    local cached_hash=$(head -1 "$CACHE_FILE" 2>/dev/null || echo "")

    if [[ "$current_hash" != "$cached_hash" ]]; then
        return 0  # Rebuild needed
    fi

    # Check if binaries exist
    local tikv_server="$BUILD_DIR/target/$BUILD_TYPE/tikv-server"
    local tikv_ctl="$BUILD_DIR/target/$BUILD_TYPE/tikv-ctl"

    if [[ ! -f "$tikv_server" ]] || [[ ! -f "$tikv_ctl" ]]; then
        return 0  # Rebuild needed
    fi

    return 1  # No rebuild needed
}

# Save build cache
save_build_cache() {
    local current_hash=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
    echo "$current_hash" > "$CACHE_FILE"
    log_info "Build cache saved for commit: $current_hash"
}

# Install system dependencies
install_dependencies() {
    log_info "Checking and installing system dependencies..."

    # Update package lists
    sudo apt-get update >/dev/null 2>&1

    # Install essential build dependencies
    local deps=(
        "build-essential"
        "cmake"
        "pkg-config"
        "libssl-dev"
        "curl"
        "git"
        "gcc-12"
        "g++-12"
    )

    for dep in "${deps[@]}"; do
        if ! dpkg -l | grep -q "^ii  $dep "; then
            log_info "Installing $dep..."
            sudo apt-get install -y "$dep" >/dev/null 2>&1
        fi
    done

    log_success "System dependencies installed"
}

# Install Rust toolchain
install_rust() {
    log_info "Checking Rust installation..."

    if ! command -v rustup &> /dev/null; then
        log_info "Installing Rust..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source "$HOME/.cargo/env"
    fi

    # Install required toolchain
    if ! rustup toolchain list | grep -q "$RUST_TOOLCHAIN"; then
        log_info "Installing Rust toolchain: $RUST_TOOLCHAIN"
        rustup toolchain install "$RUST_TOOLCHAIN"
    fi

    # Set default toolchain
    rustup default "$RUST_TOOLCHAIN"

    # Install required components
    rustup component add rust-src rustc-dev llvm-tools-preview

    log_success "Rust toolchain ready"
}

# Setup repository (respects active branch)
setup_repository() {
    log_info "Setting up TiKV repository..."

    # Check if we're in a git repository
    if [[ ! -d ".git" ]]; then
        log_error "Not in a git repository. Please run this script from the TiKV source directory."
        exit 1
    fi

    # Fetch latest changes
    git fetch --all --tags --prune

    # Handle different version options
    if [[ "$TIKV_VERSION" == "current" ]]; then
        log_info "Building on current branch: $(git branch --show-current)"
        log_info "Current commit: $(git rev-parse HEAD)"
    elif [[ "$TIKV_VERSION" == "latest" ]]; then
        # Get the latest stable release
        TIKV_VERSION=$(git tag --list "v*" --sort=-version:refname | head -n1)
        log_info "Latest stable release: $TIKV_VERSION"
        git checkout "$TIKV_VERSION"
    elif [[ "$TIKV_VERSION" == "master" ]]; then
        log_info "Switching to master branch..."
        git checkout master
        git pull origin master
    else
        log_info "Switching to version: $TIKV_VERSION"
        git checkout "$TIKV_VERSION"
    fi

    log_success "Repository ready: $(git rev-parse HEAD)"
}

# Build TiKV
build_tikv() {
    log_info "Building TiKV ($BUILD_TYPE mode)..."
    
    # Set environment variables for the build - use GCC 12
    export CC=gcc-12
    export CXX=g++-12

    # Set compiler flags to suppress warnings that cause build failures
    export CFLAGS="-Wno-error=array-bounds -Wno-array-bounds -Wno-error=stringop-overread -Wno-stringop-overread"
    export CXXFLAGS="-Wno-error=array-bounds -Wno-array-bounds -Wno-error=stringop-overread -Wno-stringop-overread"

    # Set frame pointer for better profiling (consistent with Makefile)
    export TIKV_FRAME_POINTER=1
    export RUSTFLAGS="${RUSTFLAGS:-} -Cforce-frame-pointers=yes"
    export CFLAGS="${CFLAGS:-} -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer"
    export CXXFLAGS="${CXXFLAGS:-} -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer"

    # Clean previous build artifacts (but keep dependencies)
    cargo clean --target-dir="$BUILD_DIR/target"

    # Build both tikv-server and tikv-ctl with a single cargo command
    log_info "Building tikv-server and tikv-ctl with single cargo command..."

    # Define features for each package
    local server_features=(
        "memory-engine"
        "pprof-fp"
        "jemalloc"
        "mem-profiling"
        "portable"
        "sse"
        "test-engine-kv-rocksdb"
        "test-engine-raft-raft-engine"
        "trace-async-tasks"
        "openssl-vendored"
    )

    local ctl_features=(
        "test-engine-kv-rocksdb"
        "test-engine-raft-raft-engine"
    )

    local server_features_str=$(IFS=,; echo "${server_features[*]}")
    local ctl_features_str=$(IFS=,; echo "${ctl_features[*]}")

    log_info "Building tikv-server with features: $server_features_str"
    log_info "Building tikv-ctl with features: $ctl_features_str"

    # Single build command for both packages
    local build_args=(
        "--target-dir=$BUILD_DIR/target"
        "-p" "tikv-server" "--no-default-features" "--features=$server_features_str"
        "-p" "tikv-ctl" "--features=$ctl_features_str"
    )

    if [[ "$BUILD_TYPE" == "release" ]]; then
        build_args+=("--release")
    fi

    if cargo build "${build_args[@]}"; then
        log_success "tikv-server and tikv-ctl build completed"
    else
        log_error "Build failed"
        exit 1
    fi

    save_build_cache
    log_success "TiKV build completed successfully"
}

# Verify build
verify_build() {
    log_info "Verifying build..."

    local tikv_server="$BUILD_DIR/target/$BUILD_TYPE/tikv-server"
    local tikv_ctl="$BUILD_DIR/target/$BUILD_TYPE/tikv-ctl"

    if [[ -f "$tikv_server" ]] && [[ -f "$tikv_ctl" ]]; then
        log_success "Build verification passed"
        log_info "TiKV Server: $tikv_server"
        log_info "TiKV Control: $tikv_ctl"
        
        # Show version information
        if "$tikv_server" --version 2>/dev/null; then
            log_success "TiKV version check passed"
        fi
    else
        log_error "Build verification failed - binaries not found"
        exit 1
    fi
}

# Main execution
main() {
    # Create build directory if it doesn't exist
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"
    
    # Handle log file (clean or keep based on user preference)
    if [[ "$KEEP_LOGS" == "false" ]] && [[ -f "$LOG_FILE" ]]; then
        log_info "Cleaning previous build logs..."
        rm -f "$LOG_FILE"
    elif [[ "$KEEP_LOGS" == "true" ]] && [[ -f "$LOG_FILE" ]]; then
        log_info "Preserving previous build logs..."
    fi

    log_info "Starting TiKV build process..."
    log_info "Build directory: $BUILD_DIR"
    log_info "Target version: $TIKV_VERSION"
    log_info "Build type: $BUILD_TYPE"
    log_info "Current branch: $(git branch --show-current 2>/dev/null || echo 'unknown')"

    # Check if rebuild is needed
    if [[ "$FORCE_REBUILD" == "true" ]]; then
        log_info "Force rebuild requested - clearing cache"
        rm -f "$CACHE_FILE"
    elif ! check_rebuild_needed; then
        log_success "Build is up to date - no rebuild needed"
        verify_build
        return 0
    fi

    # Install dependencies
    install_dependencies
    install_rust

    # Setup repository
    setup_repository

    # Build TiKV
    build_tikv

    # Verify build
    verify_build

    log_success "TiKV build process completed successfully!"
}

# Parse command line arguments
FORCE_REBUILD=false
KEEP_LOGS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            TIKV_VERSION="$2"
            shift 2
            ;;
        --build-type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        --build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        --force)
            FORCE_REBUILD=true
            shift
            ;;
        --keep-logs)
            KEEP_LOGS=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --version VERSION    TiKV version to build (current, master, latest, or specific version)"
            echo "  --build-type TYPE    Build type (debug or release)"
            echo "  --build-dir DIR      Build directory"
            echo "  --force              Force rebuild even if no changes detected"
            echo "  --keep-logs          Keep previous build logs (default: clean logs)"
            echo "  --help               Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                    # Build current branch"
            echo "  $0 --version master   # Build master branch"
            echo "  $0 --version v8.5.2   # Build specific version"
            echo "  $0 --force            # Force rebuild current branch"
            echo "  $0 --keep-logs        # Build with log preservation"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Run main function
main "$@" 