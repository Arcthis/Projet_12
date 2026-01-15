from sqlalchemy import create_engine
import config
import pandas as pd

def get_engine():
    url = f"postgresql+psycopg2://{config.POSTGRES['user']}:{config.POSTGRES['password']}@{config.POSTGRES['host']}:{config.POSTGRES['port']}/{config.POSTGRES['db']}"
    return create_engine(url)

def load_dev(df):
    engine = get_engine()
    df.to_sql("dev_rh_sport", engine, schema=config.DEV_SCHEMA, if_exists="replace", index=False)

def load_activities_dev(df):
    engine = get_engine()
    df.to_sql("dev_strava_activities", engine, schema=config.DEV_SCHEMA, if_exists="replace", index=False)
