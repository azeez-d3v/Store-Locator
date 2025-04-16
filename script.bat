@echo off
REM Batch script to run Python file with virtual environment check

set PYTHON_SCRIPT=app.py

REM Check if virtual environment exists
if not exist ".venv\" (
    echo Virtual environment not found. Creating new one...
    python -m venv .venv
    echo Installing required packages...
    call .venv\Scripts\activate.bat
    pip install --upgrade pip
    if exist requirements.txt (
        pip install -r requirements.txt
    ) else (
        pip install streamlit
    )
) else (
    echo Virtual environment found.
)

REM Activate virtual environment
call .venv\Scripts\activate.bat

REM Run Streamlit app
echo Running Streamlit app: %PYTHON_SCRIPT%
streamlit run %PYTHON_SCRIPT% %*
REM deactivate
