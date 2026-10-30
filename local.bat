@echo off
setlocal
cd /d "%~dp0"

REM Single-file launcher: start Tor on 127.0.0.1:9050, then vote in a loop.
set "POLL_TIMEOUT=45000"

REM ---- locate tor.exe: same folder first, then portable bundle, Desktop, or installer dirs
set "TOR_EXE=%~dp0tor.exe"
if not exist "%TOR_EXE%" set "TOR_EXE=%~dp0tor\tor.exe"
if not exist "%TOR_EXE%" set "TOR_EXE=%~dp0tor-bundle\tor\tor.exe"
if not exist "%TOR_EXE%" set "TOR_EXE=%~dp0Tor Browser\Browser\TorBrowser\Tor\tor.exe"
if not exist "%TOR_EXE%" set "TOR_EXE=%~dp0tor-browser\Browser\TorBrowser\Tor\tor.exe"
if not exist "%TOR_EXE%" set "TOR_EXE=%USERPROFILE%\Desktop\Tor Browser\Browser\TorBrowser\Tor\tor.exe"
if not exist "%TOR_EXE%" set "TOR_EXE=%ProgramFiles%\Tor Browser\Browser\TorBrowser\Tor\tor.exe"
if not exist "%TOR_EXE%" set "TOR_EXE=%ProgramFiles(x86)%\Tor Browser\Browser\TorBrowser\Tor\tor.exe"
if not exist "%TOR_EXE%" set "TOR_EXE=%LOCALAPPDATA%\Tor Browser\Browser\TorBrowser\Tor\tor.exe"

where node >nul 2>&1
if errorlevel 1 (
  echo [error] Node.js not found in PATH.
  echo Install Node.js 20+ from https://nodejs.org, then run this file again.
  pause
  exit /b 1
)
if not exist "%~dp0worker.mjs" (
  echo [error] worker.mjs must be in the same folder as local.bat.
  pause
  exit /b 1
)
if not exist "%TOR_EXE%" (
  echo [error] tor.exe not found. Looked in:
  echo    %~dp0tor.exe
  echo    %~dp0tor\tor.exe
  echo    %~dp0tor-bundle\tor\tor.exe
  echo    %~dp0Tor Browser\Browser\TorBrowser\Tor\tor.exe
  echo    %USERPROFILE%\Desktop\Tor Browser\...
  echo    %ProgramFiles%\Tor Browser\...
  echo    %LOCALAPPDATA%\Tor Browser\...
  echo Install Tor Browser, or drop a portable Tor bundle into the tor-bundle folder
  echo next to this script. Then run this file again.
  pause
  exit /b 1
)

REM geoip files live beside the tor.exe layout; fall back to the portable bundle layout
for %%I in ("%TOR_EXE%") do set "TOR_DIR=%%~dpI"
set "TOR_GEO=%TOR_DIR%..\Data\Tor\geoip"
set "TOR_GEO6=%TOR_DIR%..\Data\Tor\geoip6"
if not exist "%TOR_GEO%" set "TOR_GEO=%~dp0tor-bundle\data\geoip"
if not exist "%TOR_GEO6%" set "TOR_GEO6=%~dp0tor-bundle\data\geoip6"
if not exist "%TOR_GEO%" set "TOR_GEO=%~dp0tor\data\geoip"
if not exist "%TOR_GEO6%" set "TOR_GEO6=%~dp0tor\data\geoip6"
if not exist "%TOR_GEO%" set "TOR_GEO=%~dp0data\geoip"
if not exist "%TOR_GEO6%" set "TOR_GEO6=%~dp0data\geoip6"

:loop
call :ensure_tor
if errorlevel 1 goto fatal

echo.
echo [%TIME%] voting: "Lakshmi Priya" Jason Muniyammal --tor-streams 12 --target 500 --times 2000
node worker.mjs "Lakshmi Priya" Jason Muniyammal --tor-streams 12 --target 500 --times 2000 --tally-each
echo [%TIME%] cycle finished; restarting in 3s ^(Ctrl+C to stop^)
ping -n 4 127.0.0.1 >nul
goto loop

:ensure_tor
REM TCP probe via node: netstat on this box can stall for minutes.
node -e "const s=require('net').connect(9050,'127.0.0.1');s.on('connect',()=>{s.destroy();process.exit(0)});s.on('error',()=>process.exit(1));setTimeout(()=>process.exit(1),1000)" >nul 2>&1
if not errorlevel 1 exit /b 0

echo [Tor] starting: "%TOR_EXE%"
set "GEO_ARGS="
if exist "%TOR_GEO%" if exist "%TOR_GEO6%" set "GEO_ARGS=--GeoIPFile "%TOR_GEO%" --GeoIPv6File "%TOR_GEO6%""
start /b "" "%TOR_EXE%" --SocksPort 9050 --DataDirectory "%~dp0.tor" %GEO_ARGS%

set /a tries=0
:wait_port
node -e "const s=require('net').connect(9050,'127.0.0.1');s.on('connect',()=>{s.destroy();process.exit(0)});s.on('error',()=>process.exit(1));setTimeout(()=>process.exit(1),1000)" >nul 2>&1
if not errorlevel 1 goto tor_port_ok
set /a tries+=1
if %tries% geq 60 exit /b 1
ping -n 2 127.0.0.1 >nul
goto wait_port

:tor_port_ok
echo [Tor] listening; waiting for bootstrap...
where curl >nul 2>&1
if errorlevel 1 (
  echo [Tor] curl not found; waiting 40s for bootstrap
  ping -n 41 127.0.0.1 >nul
  exit /b 0
)
set /a btries=0
:wait_boot
curl -s --max-time 20 --socks5-hostname 127.0.0.1:9050 https://check.torproject.org/api/ip | findstr /i "IsTor" >nul
if not errorlevel 1 (
  echo [Tor] ready.
  exit /b 0
)
set /a btries+=1
if %btries% geq 10 (
  echo [Tor] warning: bootstrap not confirmed after ~3 min; continuing anyway.
  exit /b 0
)
ping -n 4 127.0.0.1 >nul
goto wait_boot

:fatal
echo.
echo [error] Tor could not start on 127.0.0.1:9050.
echo If another Tor or Tor Browser is already using port 9050, close it and rerun.
pause
exit /b 1
