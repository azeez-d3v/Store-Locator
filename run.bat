@echo off
cls
REM Run script: activate venv and run Streamlit app

set PYTHON_SCRIPT=app.py
set VENV_DIR=.venv

REM Check if virtual environment exists
if not exist "%VENV_DIR%\" (
    echo Virtual environment not found. Please run setup.bat first.
    pause
    exit /b 1
)

REM Check if uv package exists
if not exist "%VENV_DIR%\Lib\site-packages\uv\" (
    echo uv package is not installed in the virtual environment. Please run setup.bat first.
    pause
    exit /b 1
)

REM Activate virtual environment
call %VENV_DIR%\Scripts\activate.bat

cls
REM Run Streamlit app using uv
echo Running Streamlit app with uv: %PYTHON_SCRIPT%
uv run streamlit run %PYTHON_SCRIPT% %