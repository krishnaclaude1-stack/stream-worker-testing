@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

REM Determine arguments (use defaults if none provided)
if "%~1"=="" (
    set ARGS="Lakshmi Priya" Jason Muniyammal --tor-streams 12 --times 800 --target 500 --tally-each
) else (
    set "ARGS=%*"
)

echo ========================================================
echo        Bigg Boss Tamil Voting Automation (Batch)
echo ========================================================
echo Arguments: %ARGS%

:loop
REM Ensure Tor is listening on 127.0.0.1:9050
netstat -ano | findstr "127.0.0.1:9050 " | findstr "LISTENING" >nul
if errorlevel 1 (
    echo.
    echo [Tor] Port 9050 not listening. Starting Tor...
    start "Tor Proxy" /min "%~dp0tor-bundle\tor\tor.exe" --SocksPort 9050 --DataDirectory "%~dp0.tor" --GeoIPFile "%~dp0tor-bundle\data\geoip" --GeoIPv6File "%~dp0tor-bundle\data\geoip6"
    
    echo [Tor] Waiting for Tor to bootstrap on port 9050...
    :wait_tor
    ping 127.0.0.1 -n 2 >nul
    netstat -ano | findstr "127.0.0.1:9050 " | findstr "LISTENING" >nul
    if errorlevel 1 goto wait_tor
    echo [Tor] Tor is connected and listening on port 9050!
)

echo.
echo [%TIME%] Starting vote batch...
node worker.mjs %ARGS%

echo.
echo [%TIME%] Batch finished. Restarting in 2 seconds (Ctrl+C to stop)...
ping 127.0.0.1 -n 3 >nul
goto loop