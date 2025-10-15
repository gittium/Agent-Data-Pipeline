from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime
import os, requests, time

NIFI_BASE_URL = os.getenv("NIFI_BASE_URL", "https://nifi:8443")
NIFI_USERNAME = os.getenv("NIFI_USERNAME", "nifi")
NIFI_PASSWORD = os.getenv("NIFI_PASSWORD", "nifipassword")
NIFI_PG_ID = os.getenv("NIFI_PG_ID", "d85afe36-0199-1000-4bef-44a70978965e")

def nifi_debug_token():
    print("🔐 Requesting NiFi token...")
    r = requests.post(
        f"{NIFI_BASE_URL}/nifi-api/access/token",
        data=f"username={NIFI_USERNAME}&password={NIFI_PASSWORD}",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=False,
        timeout=15,
    )
    r.raise_for_status()
    token = r.text.strip()
    print(f"✅ Token received: {token[:15]}...")
    return token

def nifi_debug_start(**context):
    token = context["ti"].xcom_pull(task_ids="get_nifi_token")
    if not NIFI_PG_ID:
        print("⚠️ No process group ID set. Skipping start.")
        return "skipped"
    print(f"🚀 Starting NiFi process group: {NIFI_PG_ID}")
    r = requests.put(
        f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{NIFI_PG_ID}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"id": NIFI_PG_ID, "state": "RUNNING"},
        verify=False,
        timeout=15,
    )
    r.raise_for_status()
    print("✅ NiFi PG started successfully.")

with DAG(
    dag_id="nifi_debug_dag",
    start_date=datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["debug", "nifi"],
) as dag:
    get_token = PythonOperator(
        task_id="get_nifi_token",
        python_callable=nifi_debug_token,
    )

    start_pg = PythonOperator(
        task_id="start_nifi_pg",
        python_callable=nifi_debug_start,
    
    )

    get_token >> start_pg
