from extract import extract_data, get_engine
from generate import generate_activities
from notify import process_new_activities
from main import ensure_schemas_exist
from sqlalchemy import text
from config import RAW_SCHEMA

def init_data():
    print("STARTING INITIALIZATION: 1000 ACTIVITIES / 1 YEAR")
    
    engine = get_engine()
    ensure_schemas_exist(engine)
    
    try:
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {RAW_SCHEMA}.strava_activities"))
            conn.commit()
        print("Previous activities truncated.")
    except Exception as e:
        print(f"Truncate skipped (table might not exist): {e}")

    print("Extracting employee data...")
    df_rh, df_sport = extract_data()
    
    df_activities = generate_activities(df_rh, df_sport, num_activities=1000, days_back=365)

    process_new_activities(df_activities, engine)
    
    print("INITIALIZATION COMPLETED.")

if __name__ == "__main__":
    init_data()
