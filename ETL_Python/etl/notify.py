import pandas as pd
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import config
from sqlalchemy import create_engine, text

def send_slack_message(row):
    client = WebClient(token=config.SLACK_TOKEN)
    
    activity_lower = str(row.activity_type).lower()
    if 'vélo' in activity_lower or 'cyclisme' in activity_lower:
        emoji = "🚴"
    elif 'marche' in activity_lower or 'randonnée' in activity_lower:
        emoji = "🚶"
    elif 'natation' in activity_lower:
        emoji = "🏊"
    else:
        emoji = "🏃"

    message = (
        f"{emoji} *Nouvelle activité enregistrée*\n"
        f"👤 Salarié: `{row.employee_name}`\n"
        f"📅 Début: {row.activity_start}\n"
        f"📅 Fin: {row.activity_end}\n"
        f"🏷 Type: {row.activity_type}\n"
        f"📏 Distance: {row.distance_km:.2f} km\n"
        f"💬 {row.comment}"
    )

    try:
        client.chat_postMessage(
            channel=config.SLACK_CHANNEL,
            text=message
        )
    except SlackApiError as e:
        print(f"Error sending message: {e.response['error']}")

def process_new_activities(df_activities, engine):
    print(f"Processing {len(df_activities)} activities for notification...")
    
    existing_ids = set()
    try:
        with engine.connect() as conn:
            pass 
            
            result = conn.execute(text(f"SELECT id FROM {config.RAW_SCHEMA}.strava_activities"))
            existing_ids = {row[0] for row in result.fetchall()}
    except Exception as e:
        print(f"Warning: Could not fetch existing IDs (Table might not exist yet): {e}")

    df_activities['id'] = df_activities['id'].astype(str)
    new_rows = df_activities[~df_activities['id'].isin(existing_ids)]

    if new_rows.empty:
        print("No new unique activities to insert.")
        return

    try:
        db_rows = new_rows.drop(columns=['employee_name'], errors='ignore')
        db_rows.to_sql('strava_activities', engine, schema=config.RAW_SCHEMA, if_exists='append', index=False)
        print(f"Inserted {len(new_rows)} rows into {config.RAW_SCHEMA}.strava_activities")
    except Exception as e:
        print(f"Error inserting activities: {e}")
        return

    print("Sending Slack notifications...")
    for index, row in new_rows.iterrows():
        send_slack_message(row)
        
