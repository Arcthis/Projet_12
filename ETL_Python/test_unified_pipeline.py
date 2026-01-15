import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'etl'))

import pandas as pd
from sqlalchemy import text
from extract import extract_data
from generate import generate_activities
from notify import process_new_activities
from transform import calculate_distances_and_prime
from load import load_dev
import config
from extract import get_engine
from main import ensure_schemas_exist

def mock_airbyte_data(engine):
    print("Mocking Airbyte Data...")
    
    df_rh = pd.DataFrame([
        {
            "id_salarie": "EMP001",
            "nom": "Doe",
            "prenom": "John",
            "adresse_du_domicile": "123 Main St, Paris",
            "adresse_de_l_entreprise": "10 Avenue des Champs-Élysées, Paris",
            "salaire_brut": 50000,
            "moyen_de_deplacement": "Vélo"
        },
        {
            "id_salarie": "EMP002",
            "nom": "Smith",
            "prenom": "Jane",
            "adresse_du_domicile": "456 Rue de Rivoli, Paris",
            "adresse_de_l_entreprise": "10 Avenue des Champs-Élysées, Paris",
            "salaire_brut": 55000,
            "moyen_de_deplacement": "Métro"
        },
        {
            "id_salarie": "EMP003",
            "nom": "CarUser",
            "prenom": "Bob",
            "adresse_du_domicile": "12 Avenue Montaigne, Paris", # Very close to Champs-Élysées
            "adresse_de_l_entreprise": "10 Avenue des Champs-Élysées, Paris",
            "salaire_brut": 60000,
            "moyen_de_deplacement": "véhicule thermique/électrique"
        }
    ])
    
    df_sport = pd.DataFrame([
        {
            "id_salarie": "EMP001",
            "pratique_d_un_sport": "Vélo"
        },
        {
            "id_salarie": "EMP002",
            "pratique_d_un_sport": None
        },
        {
            "id_salarie": "EMP003",
            "pratique_d_un_sport": "Tennis"
        }
    ])
    
    df_rh.to_sql("rh_data", engine, schema=config.RAW_SCHEMA, if_exists="replace", index=False)
    df_sport.to_sql("sport_data", engine, schema=config.RAW_SCHEMA, if_exists="replace", index=False)
    print(f"Mock data inserted into {config.RAW_SCHEMA}.")

def verify_pipeline():
    engine = get_engine()
    ensure_schemas_exist(engine)
    
    mock_airbyte_data(engine)
    
    print("--- 1. Extracting Data ---")
    df_rh, df_sport = extract_data()
    assert not df_rh.empty
    assert not df_sport.empty
    print("Extraction OK.")
    
    print("--- 2. Generating Activities ---")
    df_activities = generate_activities(df_rh, df_sport, num_activities=2)
    assert len(df_activities) == 2
    print("Generation OK.")
    
    print("--- 3. Processing & Notifying ---")
    process_new_activities(df_activities, engine)
    
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT count(*) FROM {config.RAW_SCHEMA}.strava_activities"))
        count = res.scalar()
        assert count >= 2
    print("Notification/Insertion OK.")
    
    print("--- 4. Transformation ---")
    from extract import extract_activities
    df_activities_all = extract_activities()
    
    df_dev, invalid_count = calculate_distances_and_prime(df_rh, df_sport, df_activities=df_activities_all)
    print("Transformation OK.")
    
    # Check for new column
    if "jours_bien_etre" not in df_dev.columns:
        raise AssertionError("ERROR: 'jours_bien_etre' column missing!")
    else:
        print("Wellness column check OK.")

    print("--- 5. Loading ---")
    load_dev(df_dev)
    
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT count(*) FROM {config.DEV_SCHEMA}.dev_rh_sport"))
        count = res.scalar()
        assert count > 0
    print("Loading OK.")
    
    emp3_status = df_dev[df_dev['id_salarie'] == 'EMP003']['prime_eligible'].iloc[0]
    if emp3_status != 0:
        print(f"DEBUG: EMP003 mean of transport: {df_dev[df_dev['id_salarie'] == 'EMP003']['moyen_de_deplacement'].iloc[0]}")
        raise AssertionError(f"ERROR: EMP003 (Car user) should NOT be eligible, but prime_eligible is {emp3_status}")
    else:
        print("EMP003 eligibility check (Car user -> Not Eligible) OK.")
    
    print("VERIFICATION SUCCESSFUL: Pipeline Logic Validated.")

def run_test_pipeline():
    print("=== STARTING ISOLATED TEST PIPELINE ===")
    
    orig_raw = config.RAW_SCHEMA
    orig_dev = config.DEV_SCHEMA
    
    import notify
    orig_send_slack = notify.send_slack_message
    
    def mock_send_slack(row):
        print(f"MOCKED SLACK: Would send notification for {row.get('employee_name', 'Unknown')}")
    
    try:
        # Patch schemas
        config.RAW_SCHEMA = "test_raw"
        config.DEV_SCHEMA = "test_dev"
        notify.send_slack_message = mock_send_slack
        
        print(f"Switched to test schemas: {config.RAW_SCHEMA}, {config.DEV_SCHEMA}")
        print("Mocked Slack notifications.")
        
        verify_pipeline()
        
    except Exception as e:
        print(f"=== TEST PIPELINE FAILED: {e} ===")
        raise e
    finally:
        config.RAW_SCHEMA = orig_raw
        config.DEV_SCHEMA = orig_dev
        notify.send_slack_message = orig_send_slack
        print(f"Restored production schemas: {config.RAW_SCHEMA}, {config.DEV_SCHEMA}")
        print("Restored real Slack notifications.")
        print("=== END ISOLATED TEST PIPELINE ===")

if __name__ == "__main__":
    run_test_pipeline()
