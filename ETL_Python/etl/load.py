from sqlalchemy import create_engine
import config
import pandas as pd

def get_engine():
    url = f"postgresql+psycopg2://{config.POSTGRES['user']}:{config.POSTGRES['password']}@{config.POSTGRES['host']}:{config.POSTGRES['port']}/{config.POSTGRES['db']}"
    return create_engine(url)

def load_analytics(df):
    engine = get_engine()
    df.to_sql("analytics_rh_sport", engine, schema=config.ANALYTICS_SCHEMA, if_exists="replace", index=False)

def load_activities_analytics(df):
    engine = get_engine()
    df.to_sql("analytics_strava_activities", engine, schema=config.ANALYTICS_SCHEMA, if_exists="replace", index=False)
