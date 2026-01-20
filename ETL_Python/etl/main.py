from extract import extract_data, get_engine, extract_activities
from transform import calculate_distances_and_prime
from load import load_analytics, load_activities_analytics
from monitor import log_run
from generate import generate_activities
from notify import process_new_activities
from sqlalchemy import text
import config
import time
import sys
import os
import traceback
from metrics import (
    start_metrics_server,
    ETL_RUNS,
    ETL_CYCLE_DURATION,
    ETL_LINES_PROCESSED,
    ETL_LINES_NON_ELIGIBLE,
    ETL_NON_ELIGIBLE_REASON,
    ETL_INVALID_GEOCODE
)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("Lancement du script main unifié")


def ensure_schemas_exist(engine):
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {config.RAW_SCHEMA};"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {config.ANALYTICS_SCHEMA};"))
        conn.commit()
    print(f"Schemas '{config.RAW_SCHEMA}' and '{config.ANALYTICS_SCHEMA}' ensured.")


def main():
    print("Lancement du pipeline unifié.")

    start_metrics_server(8050)

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
            traceback.print_exc()
            ETL_RUNS.labels(status="failure").inc()
            time.sleep(300)
            continue

        run_info = {
            "status": "SUCCESS",
            "lines_processed": 0,
            "lines_non_eligible": 0,
        }
        status_metric = "success"

        with ETL_CYCLE_DURATION.time():
            try:
                df_rh, df_sport = extract_data()

                df_new_activities = generate_activities(df_rh, df_sport, num_activities=5)
                process_new_activities(df_new_activities, engine)

                df_activities_all = extract_activities()

                df_silver, invalid_count, invalid_reasons = calculate_distances_and_prime(
                    df_rh, df_sport, df_activities=df_activities_all
                )

                load_analytics(df_silver)

                if not df_activities_all.empty:
                    load_activities_analytics(df_activities_all)
                    print(
                        f"Synced {len(df_activities_all)} activities to "
                        f"{config.ANALYTICS_SCHEMA}.analytics_strava_activities"
                    )

                run_info["lines_processed"] = len(df_silver)
                run_info["lines_non_eligible"] = invalid_count

                ETL_LINES_PROCESSED.set(len(df_silver))
                ETL_LINES_NON_ELIGIBLE.set(invalid_count)

                for reason, count in invalid_reasons.items():
                    ETL_NON_ELIGIBLE_REASON.labels(reason=reason).set(count)

                ETL_INVALID_GEOCODE.set(invalid_reasons.get("geocode_failure", 0))

            except Exception as e:
                run_info["status"] = "FAILURE"
                run_info["error"] = str(e)
                status_metric = "failure"
                print(f"Erreur lors du cycle ETL: {e}")
                traceback.print_exc()
            finally:
                ETL_RUNS.labels(status=status_metric).inc()
                log_run(run_info)

        print("Waiting for 1 day before next cycle...")
        time.sleep(86400)


if __name__ == "__main__":
    main()
