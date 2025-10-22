@echo off
echo Uploading data to server ...

scp "db\*.duckdb" marcel@217.26.28.186:/srv/duckdb/

if %ERRORLEVEL% equ 0 (
    echo ✅ Data successfully uploaded.
) else (
    echo ❌ There was an error while uploading 
)

pause



