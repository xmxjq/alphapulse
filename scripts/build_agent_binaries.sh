#!/usr/bin/env sh
set -eu

repo="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
output="${1:-$repo/dist/agents}"
version="${VERSION:-}"
if [ -z "$version" ] && command -v git >/dev/null 2>&1; then
    version="$(git -C "$repo" rev-parse --short HEAD)"
fi
version="${version:-dev}"
mkdir -p "$output"
cd "$repo/agent"

build() {
    os="$1"
    arch="$2"
    arm="$3"
    name="$4"
    echo "Building $os/$arch -> $output/$name"
    CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" GOARM="$arm" \
        go build -trimpath -ldflags="-s -w -X main.version=$version" \
        -o "$output/$name" ./cmd/alphapulse-agent
}

build linux amd64 "" alphapulse-agent-linux-amd64
build linux arm64 "" alphapulse-agent-linux-arm64
build linux arm 7 alphapulse-agent-linux-armv7
build windows amd64 "" alphapulse-agent-windows-amd64.exe
build windows arm64 "" alphapulse-agent-windows-arm64.exe
build darwin amd64 "" alphapulse-agent-darwin-amd64
build darwin arm64 "" alphapulse-agent-darwin-arm64
