import pandas as pd
import uuid
import random
from datetime import datetime, timedelta
from mimesis import Generic
from mimesis.locales import Locale
import os

gen = Generic(locale=Locale.FR)

def get_activity_type(employee_id, df_sport, df_rh_row):
    id_col = 'ID salarié' if 'ID salarié' in df_sport.columns else 'id_salarie'
    
    sports_entry = df_sport[df_sport[id_col] == employee_id]
    
    activity = None
    
    sport_col = "Pratique d'un sport" if "Pratique d'un sport" in df_sport.columns else "pratique_d_un_sport"

    if not sports_entry.empty and pd.notna(sports_entry.iloc[0][sport_col]):
        activity = sports_entry.iloc[0][sport_col]
        
    return activity

def generate_duration_distance(activity):
    distance = None
    speed_kmh = 0
    
    if activity in ['Running', 'Course à pied']:
        distance = random.uniform(3, 21)
        speed_kmh = random.uniform(8, 14) 
    elif activity in ['Marche', 'Randonnée']:
        distance = random.uniform(2, 15)
        speed_kmh = random.uniform(3, 6)
    elif activity in ['Vélo', 'Cyclisme', 'VTT']:
        distance = random.uniform(10, 80)
        speed_kmh = random.uniform(15, 30)
    elif activity in ['Natation']:
        distance = random.uniform(0.5, 3)
        speed_kmh = random.uniform(1.5, 3) 
    elif activity in ['Tennis', 'Fitness', 'Escalade', 'Yoga', 'Crossfit']:
        distance = None 
        duration_minutes = random.randint(30, 120)
        return distance, duration_minutes

    if distance:
        duration_hours = distance / speed_kmh
        duration_minutes = int(duration_hours * 60)
        return round(distance, 2), duration_minutes
    else:
        return None, random.randint(30, 90)

def generate_comment(name, activity, distance, duration_min):
    comments = [
        f"Bravo {name} ! Quelle belle énergie !",
        f"Super session de {activity} pour {name}.",
        f"Continue comme ça {name} !",
        f"Wow, {name} est en forme !",
        f"Une belle activité terminée par {name}."
    ]
    
    if distance:
        comments.append(f"Bravo {name} ! Tu viens de faire {distance} km en {duration_min} min ! Quelle énergie !")
        comments.append(f"Solid performance {name}: {distance} km !")
    
    if activity == 'Randonnée':
        comments.append(f"Magnifique {name} ! Une randonnée terminée et un nouveau spot à découvrir !")
        
    return random.choice(comments)

def generate_activities(df_rh, df_sport, num_activities=10, days_back=30):
    print(f"Generating {num_activities} activities over {days_back} days...")
    
    activities = []
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    employees = df_rh.to_dict('records')
    
    # Pre-filter employees who have a sport to avoid infinite loops or inefficient retries
    eligible_employees = []
    for emp in employees:
        id_key = 'ID salarié' if 'ID salarié' in emp else 'id_salarie'
        emp_id = emp[id_key]
        if get_activity_type(emp_id, df_sport, emp):
            eligible_employees.append(emp)
            
    if not eligible_employees:
        print("Warning: No eligible employees with a declared sport found for activity generation.")
        return pd.DataFrame()

    while len(activities) < num_activities:
        employee = random.choice(eligible_employees)
        
        id_key = 'ID salarié' if 'ID salarié' in employee else 'id_salarie'
        nom_key = 'Nom' if 'Nom' in employee else 'nom'
        prenom_key = 'Prénom' if 'Prénom' in employee else 'prenom'
        
        emp_id = employee[id_key]
        name = f"{employee[prenom_key]} {employee[nom_key]}"
        
        activity_type = get_activity_type(emp_id, df_sport, employee)
        # Should not be None since we pre-filtered, but safe check
        if not activity_type:
            continue
            
        if hasattr(activity_type, 'capitalize'):
             activity_type = activity_type.capitalize()

        random_days = random.randint(0, days_back)
        act_date_start = start_date + timedelta(days=random_days)
        act_date_start = act_date_start.replace(hour=random.randint(6, 20), minute=random.randint(0, 59), second=0)
        
        distance, duration_min = generate_duration_distance(activity_type)
        
        act_date_end = act_date_start + timedelta(minutes=duration_min)
        
        comment = generate_comment(name, activity_type, distance, duration_min)
        
        dist_val = distance if distance else 0.0

        activities.append({
            'id': str(uuid.uuid4()),
            'employee_id': emp_id,
            'employee_name': name,
            'activity_start': act_date_start,
            'activity_type': activity_type,
            'distance_km': dist_val,
            'activity_end': act_date_end,
            'comment': comment
        })
        
    df_activities = pd.DataFrame(activities)
    
    if not df_activities.empty:
        df_activities.sort_values(by='activity_start', inplace=True)
    
    return df_activities
