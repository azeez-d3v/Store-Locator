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

REM Activate virtual environment
call %VENV_DIR%\Scripts\activate.bat

REM List of required packages (space-separated)
set PACKAGES=streamlit pandas plotly lxml bs4 curl_cffi
REM Check each package
for %%P in (%PACKAGES%) do (
    echo Checking if package %%P is installed...
    python -c "import %%P" 2>NUL
    if errorlevel 1 (
        echo Package %%P is not installed. Please run setup.bat to install requirements.
        pause
        exit /b 1
    )
)
cls
REM Run Streamlit app
echo Running Streamlit app: %PYTHON_SCRIPT%
streamlit run %PYTHON_SCRIPT% %*
