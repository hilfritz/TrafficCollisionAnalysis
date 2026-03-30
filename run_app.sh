# HOW TO RUN: ./run_app.sh

#!/bin/bash

LOG_FILE="logs/app.log"
VENV_PATH=".venv"
PYTHON_BIN="$VENV_PATH/bin/python"
STREAMLIT_BIN="$VENV_PATH/bin/streamlit"

# Ensure logs folder exists
mkdir -p logs

echo "===== RUN START $(date) =====" >> "$LOG_FILE"

# 🔹 Kill any existing Streamlit processes
echo "Stopping existing Streamlit processes..." | tee -a "$LOG_FILE"

if pgrep -f streamlit > /dev/null; then
    pkill -f streamlit
    echo "Existing Streamlit processes stopped." | tee -a "$LOG_FILE"
else
    echo "No running Streamlit processes found." | tee -a "$LOG_FILE"
fi

# 🔹 Ensure virtual environment exists
if [ ! -d "$VENV_PATH" ]; then
    echo "Virtual environment not found. Creating..." | tee -a "$LOG_FILE"
    python3 -m venv "$VENV_PATH"
fi

# Activate venv
echo "Activating virtual environment..." | tee -a "$LOG_FILE"
source "$VENV_PATH/bin/activate"

# Upgrade pip (optional)
pip install --upgrade pip >> "$LOG_FILE" 2>&1

# Install dependencies (if needed)
if [ -f "requirements.txt" ]; then
    echo "Installing dependencies..." | tee -a "$LOG_FILE"
    pip install -r requirements.txt >> "$LOG_FILE" 2>&1
fi

# 🔹 Run dataset preparation with timing
echo "Running dataset preparation..." | tee -a "$LOG_FILE"

START_TIME=$(date +%s)

PYTHONPATH=. "$PYTHON_BIN" src/prepare_dataset.py >> "$LOG_FILE" 2>&1

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "Dataset preparation finished in ${ELAPSED}s" | tee -a "$LOG_FILE"

# 🔹 Run Streamlit
echo "Starting Streamlit app..." | tee -a "$LOG_FILE"

PYTHONPATH=. "$STREAMLIT_BIN" run src/app.py >> "$LOG_FILE" 2>&1