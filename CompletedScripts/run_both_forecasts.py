# ================================
# RUN BOTH SALES & QUANTITY FORECASTS
# ================================
import papermill as pm
import os
import sys
import subprocess
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================================
# CONFIG
# ================================
OUTPUT_DIR = "forecast_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Timestamp used for notebook outputs
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

# ================================
# FUNCTION: Run a notebook
# ================================
def run_notebook(notebook_path, output_name=None):
    """Execute a notebook using papermill and write output to OUTPUT_DIR."""
    try:
        if output_name is None:
            base = os.path.splitext(os.path.basename(notebook_path))[0]
            output_name = f"{base}"
        output_path = os.path.join(OUTPUT_DIR, f"{output_name}_{TIMESTAMP}.ipynb")
        print(f"Running notebook: {notebook_path} → {output_path}")
        pm.execute_notebook(
            notebook_path,
            output_path,
            progress_bar=True,
            log_output=True
        )
        print(f"Completed notebook: {notebook_path}")
    except Exception as e:
        print(f"Failed notebook {notebook_path}: {e}")

# ================================
# MAIN: Run both (PARALLEL or SEQUENTIAL)
# ================================
def run_python_script(py_path):
    """Run a python script with the same interpreter and capture output."""
    try:
        print(f"Running script: {py_path}")
        completed = subprocess.run([sys.executable, py_path], check=True, capture_output=True, text=True)
        if completed.stdout:
            print(f"[stdout] {py_path}:\n{completed.stdout}")
        if completed.stderr:
            print(f"[stderr] {py_path}:\n{completed.stderr}")
        print(f"Completed script: {py_path}")
    except subprocess.CalledProcessError as e:
        print(f"Script failed ({py_path}) returncode={e.returncode}:\n{e.stderr}")
    except Exception as e:
        print(f"Failed running script {py_path}: {e}")


def discover_completed_scripts(directory: str):
    """Return list of .py and .ipynb files in directory, excluding this runner."""
    files = []
    runner_name = os.path.basename(__file__)
    for f in sorted(os.listdir(directory)):
        if f == runner_name:
            continue
        if f.lower().endswith('.py') or f.lower().endswith('.ipynb'):
            files.append(os.path.join(directory, f))
    return files


def main(argv=None):
    parser = argparse.ArgumentParser(description='Run all completed scripts (py + ipynb) in this folder')
    parser.add_argument('--parallel', action='store_true', help='Run tasks in parallel (threads)')
    args = parser.parse_args(argv)

    print(f"Starting run at {TIMESTAMP}\n")
    cwd = os.path.dirname(__file__)
    scripts = discover_completed_scripts(cwd)
    if not scripts:
        print("No .py or .ipynb files found in CompletedScripts")
        return

    print(f"Found {len(scripts)} files to run:")
    for s in scripts:
        print(f" - {os.path.basename(s)}")

    if args.parallel:
        print("Running in parallel mode")
        with ThreadPoolExecutor(max_workers=min(8, len(scripts))) as ex:
            futures = {}
            for s in scripts:
                if s.lower().endswith('.ipynb'):
                    futures[ex.submit(run_notebook, s)] = s
                else:
                    futures[ex.submit(run_python_script, s)] = s
            for fut in as_completed(futures):
                # exceptions already printed inside run functions
                pass
    else:
        # sequential
        for s in scripts:
            if s.lower().endswith('.ipynb'):
                run_notebook(s)
            else:
                run_python_script(s)

    print(f"\nAll tasks completed. Notebook outputs saved in: {OUTPUT_DIR}")


if __name__ == '__main__':
    main()