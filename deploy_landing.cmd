@echo off
REM Deploy the landing site + presentations to the VPS.
REM Double-click this file, or run it from cmd / PowerShell / Explorer.
REM It just launches scripts/deploy_landing_vps.sh inside Git Bash.

set "GITBASH=H:\Program Files\Git\bin\bash.exe"
if not exist "%GITBASH%" set "GITBASH=C:\Program Files\Git\bin\bash.exe"

"%GITBASH%" -lc "cd \"$(cygpath -u '%~dp0')\" && bash scripts/deploy_landing_vps.sh"

echo.
echo ===== Deploy finished. Press any key to close. =====
pause >nul
