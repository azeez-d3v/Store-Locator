@echo off
cls
REM Run script: use uv to run Streamlit app

set PYTHON_SCRIPT=app.py

REM Check if uv is installed
where uv >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo uv is not installed. Please run setup.bat first.
    pause
    exit /b 1
)

REM Check if virtual environment exists
if not exist ".venv\" (
    echo Virtual environment not found. Please run setup.bat first.
    pause
    exit /b 1
)

cls
REM Run Streamlit app using uv
echo Running Streamlit app with uv: %PYTHON_SCRIPT%
uv run -p .venv streamlit run %PYTHON_SCRIPT% %