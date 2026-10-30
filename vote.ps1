# BBB Vote PowerShell Script
# Automatically ensures Tor is running on port 9050 and runs the voting loop.

param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ArgsList
)

$PSScriptRoot = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition
if (-not $PSScriptRoot) {
    $PSScriptRoot = Get-Location
}
Set-Location $PSScriptRoot

$torPath = Join-Path $PSScriptRoot "tor-bundle\tor\tor.exe"
$torData = Join-Path $PSScriptRoot ".tor"
$geoip   = Join-Path $PSScriptRoot "tor-bundle\data\geoip"
$geoip6  = Join-Path $PSScriptRoot "tor-bundle\data\geoip6"
$socksPort = 9050

function Test-TorPort {
    param([int]$Port = 9050)
    $conn = Get-NetTCPConnection -LocalAddress 127.0.0.1 -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    return [bool]$conn
}

function Ensure-TorRunning {
    if (Test-TorPort -Port $socksPort) {
        Write-Host "Tor is ready and listening on 127.0.0.1:$socksPort." -ForegroundColor Green
        return
    }

    if (-not (Test-Path $torPath)) {
        Write-Error "Tor binary not found at $torPath"
        exit 1
    }

    Write-Host "Starting Tor proxy on port $socksPort..." -ForegroundColor Cyan
    $torArgs = @(
        "--SocksPort", "$socksPort",
        "--DataDirectory", "$torData",
        "--GeoIPFile", "$geoip",
        "--GeoIPv6File", "$geoip6"
    )

    $torProcess = Start-Process -FilePath $torPath -ArgumentList $torArgs -PassThru -WindowStyle Hidden

    Write-Host "Waiting for Tor to bootstrap on port $socksPort..." -ForegroundColor Yellow
    $waited = 0
    while (-not (Test-TorPort -Port $socksPort) -and ($waited -lt 30)) {
        Start-Sleep -Seconds 1
        $waited++
        if ($torProcess.HasExited) {
            Write-Error "Tor process exited unexpectedly with code $($torProcess.ExitCode)."
            exit 1
        }
    }

    if (Test-TorPort -Port $socksPort) {
        Write-Host "Tor bootstrapped successfully on port $socksPort!" -ForegroundColor Green
    } else {
        Write-Error "Tor failed to start listening within 30 seconds."
        exit 1
    }
}

# Check Node.js availability
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
    Write-Error "Node.js is not found in PATH. Please install Node.js or add it to PATH."
    exit 1
}

# Determine CLI args for node script
$nodeArgs = if ($ArgsList -and $ArgsList.Count -gt 0) {
    $ArgsList
} else {
    @("Lakshmi Priya", "Jason", "Muniyammal", "--tor-streams", "12", "--times", "800", "--target", "500", "--tally-each")
}

Write-Host "=== Bigg Boss Tamil Voting Automation ===" -ForegroundColor Cyan
Write-Host "Arguments: $nodeArgs" -ForegroundColor DarkGray

Ensure-TorRunning

while ($true) {
    Ensure-TorRunning

    Write-Host "`nStarting vote batch at $(Get-Date -Format 'HH:mm:ss')..." -ForegroundColor Cyan
    & node worker.mjs @nodeArgs

    Write-Host "Script cycle finished. Restarting in 2 seconds (Ctrl+C to stop)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 2
}
