@echo off
cls
REM Update script: Downloads the latest version from GitHub without requiring Git

setlocal EnableDelayedExpansion

REM Configure repository information
set REPO_OWNER=azeez-d3v
set REPO_NAME=Store-Locator
set GITHUB_URL=https://github.com/%REPO_OWNER%/%REPO_NAME%/archive/refs/heads/main.zip
set TEMP_ZIP=%TEMP%\store-locator-update.zip
set TEMP_DIR=%TEMP%\store-locator-update

echo ===================================================
echo           STORE LOCATOR UPDATER UTILITY
echo ===================================================
echo.
echo This will update your Store Locator to the latest version
echo from GitHub without affecting your existing data files.
echo.

REM Check if the user wants to proceed
set /p CONTINUE=Do you want to continue? (Y/N): 
if /i "%CONTINUE%" NEQ "Y" (
    echo Update cancelled.
    goto :end
)

echo.
echo Creating backup of current files...
if not exist "backup\" mkdir backup
powershell -Command "Get-ChildItem -Path '.' -Exclude 'output', 'logs', 'backup', '.venv' | Where-Object {$_.Name -notmatch '^(?:\.git|__pycache__).*'} | ForEach-Object { Copy-Item -Path $_.FullName -Destination ('backup\' + $_.Name) -Recurse -Force }"
echo Backup completed.

echo.
echo Downloading the latest version from GitHub...
powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '%GITHUB_URL%' -OutFile '%TEMP_ZIP%'"

if %ERRORLEVEL% NEQ 0 (
    echo Failed to download the update. Please check your internet connection.
    goto :error
)

echo Download completed.

echo.
echo Extracting files...
if exist "%TEMP_DIR%" rmdir /s /q "%TEMP_DIR%"
powershell -Command "Expand-Archive -Path '%TEMP_ZIP%' -DestinationPath '%TEMP_DIR%' -Force"

if %ERRORLEVEL% NEQ 0 (
    echo Failed to extract the update package.
    goto :error
)

echo.
echo Updating files...
REM Get the extracted directory name
for /f "delims=" %%a in ('dir /b /ad "%TEMP_DIR%"') do (
    set EXTRACTED_DIR=%%a
)

REM Copy files, excluding specific directories and files that shouldn't be overwritten
powershell -Command "Get-ChildItem -Path '%TEMP_DIR%\!EXTRACTED_DIR!' -Exclude 'output', 'logs', '.git', '__pycache__' | ForEach-Object { Copy-Item -Path $_.FullName -Destination '.' -Recurse -Force }"

echo.
echo Cleaning up temporary files...
del "%TEMP_ZIP%" 2>nul
rmdir /s /q "%TEMP_DIR%" 2>nul

echo.
echo ===================================================
echo Update completed successfully!
echo ===================================================
echo.
echo Your Store Locator has been updated to the latest version.
echo A backup of your previous installation is in the 'backup' folder.
echo.

goto :end

:error
echo.
echo ===================================================
echo An error occurred during the update process.
echo Your original files remain unchanged.
echo ===================================================
echo.
if exist "%TEMP_ZIP%" del "%TEMP_ZIP%" 2>nul
if exist "%TEMP_DIR%" rmdir /s /q "%TEMP_DIR%" 2>nul

:end
pause
