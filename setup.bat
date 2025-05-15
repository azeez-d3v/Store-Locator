@echo off
cls
REM Setup script: directly install uv and use it for dependency management

REM Start the timer
set start_time=%time%

REM Check if uv is already installed
where uv >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo uv is already installed. Skipping installation.
) else (
    echo Installing uv directly from astral.sh...
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    
    REM Check if uv was successfully installed
    where uv >nul 2>&1
    if %ERRORLEVEL% NEQ 0 (
        echo Failed to install uv. Please check your internet connection and try again.
        goto :end
    ) else (
        echo uv successfully installed.
    )
)

REM Check if pyproject.toml exists before initializing uv
if not exist "pyproject.toml" (
    echo Initializing uv...
    uv init
) else (
    echo pyproject.toml found. Skipping uv init.
)

REM Create virtual environment using uv
echo Creating virtual environment with uv...
uv venv

REM Install dependencies
if exist requirements.txt (
    echo Installing dependencies from requirements.txt using uv...
    uv add -r requirements.txt
) else (
    echo requirements.txt not found. Installing default packages using uv...
    uv add pandas streamlit beautifulsoup4 curl-cffi plotly lxml rich openpyxl
)

REM Calculate elapsed time
:end
set end_time=%time%

REM Convert times to centiseconds for calculation
for /F "tokens=1-4 delims=:.," %%a in ("%start_time%") do (
   set /A "start=(((%%a*60)+1%%b %% 100)*60+1%%c %% 100)*100+1%%d %% 100"
)
for /F "tokens=1-4 delims=:.," %%a in ("%end_time%") do (
   set /A "end=(((%%a*60)+1%%b %% 100)*60+1%%c %% 100)*100+1%%d %% 100"
)

REM Calculate elapsed time in centiseconds, then convert to appropriate units
set /A elapsed=end-start
if %elapsed% lss 0 set /A elapsed+=24*60*60*100

REM Calculate hours, minutes, seconds and centiseconds
set /A hh=elapsed/(60*60*100), rest=elapsed%%(60*60*100)
set /A mm=rest/(60*100), rest%%=60*100
set /A ss=rest/100, cc=rest%%100

REM Format the output with leading zeros
if %hh% lss 10 set hh=0%hh%
if %mm% lss 10 set mm=0%mm%
if %ss% lss 10 set ss=0%ss%
if %cc% lss 10 set cc=0%cc%

cls
echo Setup complete.
echo.
echo Time taken: %hh%:%mm%:%ss%.%cc% (HH:MM:SS.CC)
echo.
echo Installation status:
where uv >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo - uv: NOT INSTALLED
) else (
    echo - uv: INSTALLED
)
if exist ".venv\" (
    echo - Virtual environment: CREATED
) else (
    echo - Virtual environment: FAILED
)
if exist requirements.txt (
    echo - Dependencies: INSTALLED FROM requirements.txt
) else (
    echo - Dependencies: INSTALLED DEFAULT PACKAGES
)
pause