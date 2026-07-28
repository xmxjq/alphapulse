param(
    [string]$OutputDirectory = "dist/agents"
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$agent = Join-Path $repo "agent"
$output = Join-Path $repo $OutputDirectory
New-Item -ItemType Directory -Force -Path $output | Out-Null

$targets = @(
    @{ GOOS = "linux"; GOARCH = "amd64"; GOARM = ""; Name = "alphapulse-agent-linux-amd64" },
    @{ GOOS = "linux"; GOARCH = "arm64"; GOARM = ""; Name = "alphapulse-agent-linux-arm64" },
    @{ GOOS = "linux"; GOARCH = "arm"; GOARM = "7"; Name = "alphapulse-agent-linux-armv7" },
    @{ GOOS = "windows"; GOARCH = "amd64"; GOARM = ""; Name = "alphapulse-agent-windows-amd64.exe" },
    @{ GOOS = "windows"; GOARCH = "arm64"; GOARM = ""; Name = "alphapulse-agent-windows-arm64.exe" },
    @{ GOOS = "darwin"; GOARCH = "amd64"; GOARM = ""; Name = "alphapulse-agent-darwin-amd64" },
    @{ GOOS = "darwin"; GOARCH = "arm64"; GOARM = ""; Name = "alphapulse-agent-darwin-arm64" }
)

foreach ($target in $targets) {
    $env:CGO_ENABLED = "0"
    $env:GOOS = $target.GOOS
    $env:GOARCH = $target.GOARCH
    if ($target.GOARM) {
        $env:GOARM = $target.GOARM
    } else {
        Remove-Item Env:GOARM -ErrorAction SilentlyContinue
    }
    $destination = Join-Path $output $target.Name
    Write-Host "Building $($target.GOOS)/$($target.GOARCH) -> $destination"
    & go build -trimpath -ldflags "-s -w" -o $destination ./cmd/alphapulse-agent
    if ($LASTEXITCODE -ne 0) {
        throw "Go build failed for $($target.GOOS)/$($target.GOARCH)"
    }
}

Remove-Item Env:GOOS, Env:GOARCH, Env:GOARM, Env:CGO_ENABLED -ErrorAction SilentlyContinue
