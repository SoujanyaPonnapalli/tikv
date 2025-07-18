#!/bin/bash

# TiKV Build Script - Clean, Concise, and Idempotent
# Builds the latest master branch or stable release with dependency management

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-$SCRIPT_DIR}"
TIKV_VERSION="${TIKV_VERSION:-master}"  # Options: master, latest, or specific version like v8.5.2
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

# Cleanup function
cleanup() {
    log_info "Build script completed"
}

trap cleanup EXIT

# Check if rebuild is needed
check_rebuild_needed() {
    local current_hash
    local cached_hash
    
    # Calculate current source hash
    current_hash=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
    
    # Check if binaries exist
    if [[ ! -f "$BUILD_DIR/target/$BUILD_TYPE/tikv-server" ]] || [[ ! -f "$BUILD_DIR/target/$BUILD_TYPE/tikv-ctl" ]]; then
        log_info "Binaries not found - rebuild needed"
        return 0
    fi
    
    # Check cached hash
    if [[ -f "$CACHE_FILE" ]]; then
        cached_hash=$(cat "$CACHE_FILE" 2>/dev/null || echo "")
        if [[ "$current_hash" == "$cached_hash" ]]; then
            log_success "No changes detected - binaries are up to date"
            return 1
        fi
    fi
    
    log_info "Source has changed - rebuild needed"
    return 0
}

# Save build cache
save_build_cache() {
    local current_hash
    current_hash=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
    echo "$current_hash" > "$CACHE_FILE"
    log_info "Build cache updated"
}

# Install system dependencies
install_dependencies() {
    log_info "Checking and installing system dependencies..."
    
    # Update package list
    sudo apt-get update -qq
    
    # Install essential build dependencies
    local deps=(
        "build-essential"
        "cmake"
        "pkg-config"
        "libssl-dev"
        "libclang-dev"
        "clang"
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

# Setup TiKV repository
setup_repository() {
    log_info "Setting up TiKV repository..."
    
    # Check if we're in a git repository
    if [[ ! -d ".git" ]]; then
        log_error "Not in a git repository. Please run this script from the TiKV source directory."
        exit 1
    fi
    
    # Fetch latest changes
    git fetch --all --tags --prune
    
    # Determine version to build
    if [[ "$TIKV_VERSION" == "latest" ]]; then
        # Get the latest stable release
        TIKV_VERSION=$(git tag --list "v*" --sort=-version:refname | head -n1)
        log_info "Latest stable release: $TIKV_VERSION"
    fi
    
    # Checkout the target version
    if [[ "$TIKV_VERSION" == "master" ]]; then
        git checkout master
        git pull origin master
    else
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
    
    # Define features consistent with Makefile defaults
    local features=(
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
    
    local features_str=$(IFS=,; echo "${features[*]}")
    log_info "Building with features: $features_str"
    
    # Build tikv-server
    log_info "Building tikv-server..."
    local build_args=(
        "--target-dir=$BUILD_DIR/target"
        "--no-default-features"
        "--features=$features_str"
    )
    
    if [[ "$BUILD_TYPE" == "release" ]]; then
        build_args+=("--release")
    fi
    
    if cargo build -p tikv-server "${build_args[@]}"; then
        log_success "tikv-server build completed"
    else
        log_error "tikv-server build failed"
        exit 1
    fi
    
    # Build tikv-ctl
    log_info "Building tikv-ctl..."
    if cargo build -p tikv-ctl "${build_args[@]}"; then
        log_success "tikv-ctl build completed"
    else
        log_error "tikv-ctl build failed"
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
    log_info "Starting TiKV build process..."
    log_info "Build directory: $BUILD_DIR"
    log_info "Target version: $TIKV_VERSION"
    log_info "Build type: $BUILD_TYPE"
    
    # Create build directory if it doesn't exist
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"
    
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
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --version VERSION    TiKV version to build (master, latest, or specific version)"
            echo "  --build-type TYPE    Build type (debug or release)"
            echo "  --build-dir DIR      Build directory"
            echo "  --force              Force rebuild even if no changes detected"
            echo "  --help               Show this help message"
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