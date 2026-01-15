from extract import extract_data, get_engine
from transform import calculate_distances_and_prime
from load import load_dev
from monitor import log_run
from generate import generate_activities
from notify import process_new_activities
from sqlalchemy import text
import config
from extract import extract_activities
from load import load_activities_dev
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print('Lancement du script main unifié')

def ensure_schemas_exist(engine):
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {config.RAW_SCHEMA};"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {config.DEV_SCHEMA};"))
        conn.commit()
    print(f"Schemas '{config.RAW_SCHEMA}' and '{config.DEV_SCHEMA}' ensured.")

def main():
    print('Lancement du pipeline unifié.')
    
    engine = get_engine()
    ensure_schemas_exist(engine)

    while True:
        print("Début du cycle ETL...")
        
        print("Running pre-cycle systematic verification...")
        try:
            import test_unified_pipeline
            test_unified_pipeline.run_test_pipeline()
            print("Verification successful. Proceeding to production ETL.")
        except Exception as e:
            print(f"CRITICAL: Verification failed. Saving log and retrying next cycle. Error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(300)
            continue

        run_info = {"status": "SUCCESS", "lines_processed": 0, "lines_invalid": 0}
        
        try:
            df_rh, df_sport = extract_data()
            
            df_new_activities = generate_activities(df_rh, df_sport, num_activities=5)
            
            process_new_activities(df_new_activities, engine)

            df_activities_all = extract_activities()

            df_silver, invalid_count = calculate_distances_and_prime(df_rh, df_sport, df_activities=df_activities_all)
            load_dev(df_silver)
            
            if not df_activities_all.empty:
                load_activities_dev(df_activities_all)
                print(f"Synced {len(df_activities_all)} activities to {config.DEV_SCHEMA}.dev_strava_activities")
            
            run_info["lines_processed"] = len(df_silver)
            run_info["lines_invalid"] = invalid_count
            print("Cycle ETL terminé avec succès.")
            
        except Exception as e:
            run_info["status"] = "FAILURE"
            run_info["error"] = str(e)
            print(f"Erreur lors du cycle ETL: {e}")
            import traceback
            traceback.print_exc()
        finally:
            log_run(run_info)
        
        print("Waiting for 1 day before next cycle...")
        time.sleep(86400)

if __name__ == "__main__":
    main()
