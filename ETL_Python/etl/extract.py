import pandas as pd
from sqlalchemy import create_engine
import config

def get_engine():
    url = f"postgresql+psycopg2://{config.POSTGRES['user']}:{config.POSTGRES['password']}@{config.POSTGRES['host']}:{config.POSTGRES['port']}/{config.POSTGRES['db']}"
    return create_engine(url)

def extract_data():
    engine = get_engine()
    df_rh = pd.read_sql(f"SELECT * FROM {config.RAW_SCHEMA}.rh_data", engine)
    df_sport = pd.read_sql(f"SELECT * FROM {config.RAW_SCHEMA}.sport_data", engine)
    return df_rh, df_sport

def extract_activities():
    engine = get_engine()
    try:
        df_activities = pd.read_sql(f"SELECT * FROM {config.RAW_SCHEMA}.strava_activities", engine)
        return df_activities
    except Exception as e:
        print(f"Warning: Could not extract activities: {e}")
        return pd.DataFrame()
