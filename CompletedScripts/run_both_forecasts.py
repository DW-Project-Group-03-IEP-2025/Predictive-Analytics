# ================================
# RUN BOTH SALES & QUANTITY FORECASTS
# ================================
import papermill as pm
import os
from datetime import datetime
import threading

# ================================
# CONFIG
# ================================
SALES_NOTEBOOK = "Sales_Push.ipynb"
QUANTITY_NOTEBOOK = "Quantity_Push.ipynb"

OUTPUT_DIR = "forecast_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

# ================================
# FUNCTION: Run a notebook
# ================================
def run_notebook(notebook_path, output_name):
    try:
        output_path = os.path.join(OUTPUT_DIR, f"{output_name}_{TIMESTAMP}.ipynb")
        print(f"Running {notebook_path} → {output_path}")
        pm.execute_notebook(
            notebook_path,
            output_path,
            progress_bar=True,
            log_output=True
        )
        print(f"Completed: {notebook_path}")
    except Exception as e:
        print(f"Failed {notebook_path}: {e}")

# ================================
# MAIN: Run both (PARALLEL or SEQUENTIAL)
# ================================
if __name__ == "__main__":
    print(f"Starting both forecasts at {TIMESTAMP}\n")

    # OPTION 1: SEQUENTIAL (safer)
    run_notebook(SALES_NOTEBOOK, "sales_executed")
    run_notebook(QUANTITY_NOTEBOOK, "quantity_executed")

    # OPTION 2: PARALLEL (faster, if DB allows)
    # t1 = threading.Thread(target=run_notebook, args=(SALES_NOTEBOOK, "sales_executed"))
    # t2 = threading.Thread(target=run_notebook, args=(QUANTITY_NOTEBOOK, "quantity_executed"))
    # t1.start(); t2.start()
    # t1.join(); t2.join()

    print(f"\nBoth forecasts completed!")
    print(f"Outputs saved in: {OUTPUT_DIR}")