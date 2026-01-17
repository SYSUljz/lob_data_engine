# Use Bash for robust command execution
SHELL := /bin/bash

# Define the path to your Conda environment for clarity
CONDA_ENV_PREFIX := /home/jack_li/python/LOB_research/.conda
PYTHON_MODULE := hyperliquid.main
BINANCE_SCRIPT := src/start_binance.py
BINANCE_PERP_SCRIPT := src/start_binance_perp.py

.PHONY: run start stop process_day start-binance stop-binance start-binance-perp stop-binance-perp status start-all stop-all

# --- Foreground Execution ---
# Runs the module directly in the foreground.
# We use a single bash command ('/bin/bash -c') to ensure 'source', 'activate', 
# and 'python' run in the same shell session.
run:
	@echo "Activating Conda environment and running LOB research module (Foreground)..."
	/bin/bash -c 'source $$(conda info --base)/etc/profile.d/conda.sh && conda activate $(CONDA_ENV_PREFIX) && python -m $(PYTHON_MODULE)'

# --- Background Execution ---
# Runs the module in the background using 'nohup'. The '>>' ensures appending.
# We use the same single-shell command approach to handle 'conda activate'.
start:
	@echo "Starting LOB research module in background (nohup). Output is APPENDED to output.log."
	# Command execution within a single shell instance, finding the Conda initialization script dynamically
	nohup /bin/bash -c 'source $$(conda info --base)/etc/profile.d/conda.sh && conda activate $(CONDA_ENV_PREFIX) && python -m $(PYTHON_MODULE)' >> output.log 2>&1 &

# --- Utility to Stop the Background Process ---
# Finds the process running the main module and terminates it gracefully (SIGTERM).
stop:
	@echo "Attempting to stop LOB research process..."
	# Search for the Python process running the specific module
	pkill -f "python -m $(PYTHON_MODULE)" || echo "Process not found or already stopped."

# --- Binance Spot ---
start-binance:
	@echo "Starting Binance Spot listener..."
	nohup /bin/bash -c 'source $$(conda info --base)/etc/profile.d/conda.sh && conda activate $(CONDA_ENV_PREFIX) && python $(BINANCE_SCRIPT)' > /dev/null 2>&1 &

stop-binance:
	@echo "Stopping Binance Spot listener..."
	pkill -f "python $(BINANCE_SCRIPT)" || echo "Binance Spot process not found."

# --- Binance Perp ---
start-binance-perp:
	@echo "Starting Binance Perp listener..."
	nohup /bin/bash -c 'source $$(conda info --base)/etc/profile.d/conda.sh && conda activate $(CONDA_ENV_PREFIX) && python $(BINANCE_PERP_SCRIPT)' > /dev/null 2>&1 &

stop-binance-perp:
	@echo "Stopping Binance Perp listener..."
	pkill -f "python $(BINANCE_PERP_SCRIPT)" || echo "Binance Perp process not found."

# --- All Processes ---
start-all: start-binance start-binance-perp
	@echo "All listeners started."

stop-all: stop-binance stop-binance-perp
	@echo "All listeners stopped."

# --- Status Check ---
status:
	@echo "Checking process status..."
	@if pgrep -f "[p]ython $(BINANCE_SCRIPT)" > /dev/null; then echo "✅ Binance Spot is RUNNING"; else echo "❌ Binance Spot is STOPPED"; fi
	@if pgrep -f "[p]ython $(BINANCE_PERP_SCRIPT)" > /dev/null; then echo "✅ Binance Perp is RUNNING"; else echo "❌ Binance Perp is STOPPED"; fi
	@if pgrep -f "[p]ython -m $(PYTHON_MODULE)" > /dev/null; then echo "✅ Hyperliquid (main) is RUNNING"; else echo "❌ Hyperliquid (main) is STOPPED"; fi


# --- Manual Processing Task ---
# Merges parquet files and syncs to cloud.
# Usage: make process_day [DATE=2025-12-19]
# If DATE is omitted, it defaults to yesterday (handled by the python script).
process_day:
	@echo "Running daily processing task..."
	/bin/bash -c 'source $$(conda info --base)/etc/profile.d/conda.sh && conda activate $(CONDA_ENV_PREFIX) && python src/process/process_lob.py $${DATE:+--date $$DATE} --sync'
