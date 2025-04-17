@echo off
REM Batch script to run Python file with virtual environment and package check

set PYTHON_SCRIPT=app.py

REM Check if virtual environment exists
if not exist ".venv\" (
    echo Virtual environment not found. Creating new one...
    python -m venv .venv
)

REM Activate virtual environment
call .venv\Scripts\activate.bat

echo Upgrading pip to the latest version...
python -m pip install --upgrade pip

REM Always install requirements or default packages
if exist requirements.txt (
    echo Installing dependencies from requirements.txt...
    pip install -r requirements.txt
) else (
    echo Installing default packages...
    pip install streamlit beautifulsoup4 curl_cffi pandas plotly lxml
)

REM Run Streamlit app
echo Running Streamlit app: %PYTHON_SCRIPT%
streamlit run %PYTHON_SCRIPT% %*