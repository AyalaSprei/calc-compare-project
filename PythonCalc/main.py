import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import time
import re
import os

server = 'DESKTOP-4G5A9E'
database = 'CalculationProject'
connection_string = f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
engine = create_engine(connection_string)

# --- Translate SQL-like formula to Python syntax ---
def translate_formula_to_python(formula):
    if formula is None or not isinstance(formula, str): return "0"
    formula = formula.strip()
    # POWER(x,y) -> (x)**(y)
    formula = re.sub(r'POWER\s*\(([^,]+)\s*,\s*([^)]+)\)', r'(\1)**(\2)', formula, flags=re.IGNORECASE)
    # Function normalization
    formula = formula.replace('SQRT', 'sqrt').replace('ABS', 'abs')
    return formula

try:
    print("--- Starting Final Turbo Engine ---")
    # --- Load data from DB ---
    df_data = pd.read_sql("SELECT data_id, a, b, c, d FROM t_data", engine)
    # --- Load formulas ---
    df_targil = pd.read_sql("SELECT targil_id, targil, tnai, targil_false FROM t_targil", engine)

    # Temp folder for CSV files (used for fast bulk insert)
    temp_path = r"C:\temp"

    # --- Create view for BULK INSERT (required workaround) ---
    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('v_results_bulk', 'V') IS NOT NULL DROP VIEW v_results_bulk"))
        conn.execute(text("CREATE VIEW v_results_bulk AS SELECT data_id, targil_id, method, result FROM t_results"))

    # --- Main loop: process each formula ---
    for _, row in df_targil.iterrows():
        t_id = row['targil_id']
        # Translate formulas to Python syntax
        formula = translate_formula_to_python(str(row['targil']) if pd.notnull(row['targil']) else "0")
        tnai = translate_formula_to_python(str(row['tnai']) if pd.notnull(row['tnai']) else "")
        t_false = translate_formula_to_python(str(row['targil_false']) if pd.notnull(row['targil_false']) else "0")

        print(f"Calculating Formula {t_id}...")
        start_time = time.time()

    # --- Evaluate formula ---
        try:
            # Condition-based calculation (vectorized)
            if tnai and tnai.strip() != "":
                mask = df_data.eval(tnai, engine='python')
                res_true = df_data.eval(formula, engine='python')
                res_false = df_data.eval(t_false, engine='python')
                df_data['result'] = np.where(mask, res_true, res_false)
            # Simple calculation
            else:
                df_data['result'] = df_data.eval(formula, engine='python')
        except Exception as e:
            print(f"Error in Formula {t_id}: {e}")
            continue

        # --- Prepare results DataFrame ---
        df_results = pd.DataFrame({
            'data_id': df_data['data_id'],
            'targil_id': int(t_id),
            'method': 'Python',
            'result': df_data['result'].astype(float)
        })

        # --- Save results to CSV (for BULK INSERT) ---
        csv_file = os.path.join(temp_path, f"res_{t_id}.csv")
        df_results.to_csv(csv_file, index=False, header=False, lineterminator='\n')

        # --- Bulk insert into SQL Server ---
        bulk_sql = f"""
        BULK INSERT v_results_bulk
        FROM '{csv_file}'
        WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
        );
        """
        
        print(f"Bulk loading Formula {t_id}...")
        with engine.begin() as conn:
            conn.execute(text(bulk_sql))
            
        # --- Log execution time ---
        run_time = time.time() - start_time
        
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO t_log (targil_id, method, run_time) VALUES (:tid, 'Python', :time)"), 
                         {"tid": t_id, "time": run_time})

        print(f"Formula {t_id} finished in {run_time:.2f}s")
        # --- Cleanup temp file ---
        if os.path.exists(csv_file): os.remove(csv_file)

except Exception as e:
    print(f"\nSomething went wrong: {e}")

print("\n--- Process Finished! ---")