import os
from dotenv import load_dotenv

base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
env_path = os.path.join(base_dir, '.env')
load_dotenv(env_path)

POSTGRES = {
    'user': os.getenv('POSTGRES_USER', 'root'),
    'password': os.getenv('POSTGRES_PASSWORD', 'root'),
    'db': os.getenv('POSTGRES_DB', 'test_db'),
    'host': os.getenv('POSTGRES_HOST', os.getenv('POSTGRES_IP', 'localhost')),
    'port': os.getenv('POSTGRES_PORT', '5439')
}

SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL_ID")

CSV_PATH = os.getenv("CSV_PATH", os.path.join(base_dir, "generated_activities.csv"))

RAW_SCHEMA = os.getenv("RAW_SCHEMA", "raw")
ANALYTICS_SCHEMA = os.getenv("ANALYTICS_SCHEMA", "analytics")

MAX_DISTANCE = {
    'Vélo/Trottinette/Autres': 25,
    'Marche/running': 15
}
PRIME_PERCENT = 0.05

LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "etl_run.log")
