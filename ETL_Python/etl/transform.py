import pandas as pd
from geopy.distance import geodesic
from config import MAX_DISTANCE, PRIME_PERCENT
from datetime import datetime, timedelta

from geopy.geocoders import Nominatim
import time
import json

def geocode_address(address):
    geolocator = Nominatim(user_agent="etl_projet_12")
    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            return (location.latitude, location.longitude)
    except Exception as e:
        print(f"Erreur de géocodage pour {address}: {e}")
    return None

def calculate_wellness_eligibility(df, df_activities):
    """
    Determine eligibility for 'Jours de bien-être'.
    Condition: > 15 sport activities in the last year.
    """
    if df_activities is None or df_activities.empty:
        df["jours_bien_etre"] = False
        return df

    if not pd.api.types.is_datetime64_any_dtype(df_activities['activity_start']):
        df_activities['activity_start'] = pd.to_datetime(df_activities['activity_start'])

    one_year_ago = datetime.now() - timedelta(days=365)
    recent_activities = df_activities[df_activities['activity_start'] >= one_year_ago]
    
    activity_counts = recent_activities.groupby('employee_id').size().reset_index(name='activity_count')
    
    df = pd.merge(df, activity_counts, left_on="id_salarie", right_on="employee_id", how="left")
    
    df['activity_count'] = df['activity_count'].fillna(0)
    df['jours_bien_etre'] = df['activity_count'] >= 15
    
    return df

def calculate_distances_and_prime(df_rh, df_sport, df_activities=None):
    df = pd.merge(df_rh, df_sport, on="id_salarie", how="inner")

    distances = []
    eligible = []
    invalid_count = 0

    for index, row in df.iterrows():
        if index % 10 == 0:
            print(f"Traitement de la ligne {index}/{len(df)}")

        lat_lon_home = geocode_address(row["adresse_du_domicile"])
        time.sleep(1)
        
        lat_lon_office = geocode_address(row["adresse_de_l_entreprise"])
        time.sleep(1)

        if lat_lon_home and lat_lon_office:
            distance_km = geodesic(lat_lon_home, lat_lon_office).km
        else:
            distance_km = 9999

        distances.append(distance_km)

        if row.get("moyen_de_deplacement") == "véhicule thermique/électrique":
             eligible.append(False)
             invalid_count += 1
        else:
            max_dist = MAX_DISTANCE.get(row["pratique_d_un_sport"], 25)
            if distance_km <= max_dist:
                eligible.append(True)
            else:
                eligible.append(False)
                invalid_count += 1

    df["distance_km"] = distances
    df["prime_eligible"] = eligible
    
    df["salaire_brut"] = pd.to_numeric(df["salaire_brut"], errors='coerce').fillna(0).astype(float)
    df["prime_eligible"] = df["prime_eligible"].astype(int)
    df["distance_km"] = df["distance_km"].astype(float)
    
    df["prime_amount"] = (df["salaire_brut"] * PRIME_PERCENT * df["prime_eligible"]).astype(float)

    df = calculate_wellness_eligibility(df, df_activities)

    cols_to_keep = [c for c in df.columns if not c.startswith('_airbyte') and c != 'employee_id']
    df = df[cols_to_keep]

    for col in df.columns:
        if df[col].dtype == 'object':
             if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x) if x is not None else None)
    
    return df, invalid_count
