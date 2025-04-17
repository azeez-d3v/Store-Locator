@echo off
cls
REM Setup script: create venv and install dependencies

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

echo Upgrading pip to the latest version...
python -m pip install --upgrade pip

REM Install dependencies
if exist requirements.txt (
    echo Installing dependencies from requirements.txt...
    pip install -r requirements.txt
) else (
    echo requirements.txt not found. Installing default packages...
    pip install pandas streamlit beautifulsoup4 curl_cffi plotly lxml
)
cls
echo Setup complete.
pause
