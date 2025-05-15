@echo off
cls
REM Setup script: create venv and install dependencies with uv package manager

REM Start the timer
set start_time=%time%

set VENV_DIR=.venv

REM Check if virtual environment exists
if not exist "%VENV_DIR%\" (
    echo Virtual environment not found. Creating new one...
    python -m venv %VENV_DIR%
) else (
    echo Virtual environment already exists.
)

REM Activate virtual environment
call %VENV_DIR%\Scripts\activate.bat

REM Check if uv folder exists in the virtual environment
if exist "%VENV_DIR%\Lib\site-packages\uv\" (
    echo uv package is already installed in the virtual environment.
) else (
    echo uv package not found. Installing uv package manager...
    python -m pip install uv
)

REM Check if pyproject.toml exists before initializing uv
if not exist "pyproject.toml" (
    echo Initializing uv...
    uv init
) else (
    echo pyproject.toml found. Skipping uv init.
)

REM Install dependencies
if exist requirements.txt (
    echo Installing dependencies from requirements.txt using uv...
    uv add -r requirements.txt
) else (
    echo requirements.txt not found. Installing default packages using uv...
    uv add pandas streamlit beautifulsoup4 curl-cffi plotly lxml rich openpyxl
)

REM Calculate elapsed time
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
echo Virtual environment status:
if exist "%VENV_DIR%\" (
    echo - Virtual environment: INSTALLED
) else (
    echo - Virtual environment: NOT INSTALLED
)
if exist "%VENV_DIR%\Lib\site-packages\uv\" (
    echo - uv package: INSTALLED
) else (
    echo - uv package: NOT INSTALLED
)
pause