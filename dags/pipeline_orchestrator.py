from __future__ import annotations
import os, time, requests
from pendulum import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ---------- CONFIG ----------
CSV_PATH = os.getenv("CSV_PATH", "/opt/nifi/input_data/yellow_tripdata_2025-01.csv")
TABLE_NAME = os.getenv("DEST_TABLE_NAME", "raw.yellow_tripdata_2025_01")
AGENT_URL = os.getenv("AGENT_URL", "http://agent-schema:8000/nifi_table_create")

NIFI_BASE_URL = os.getenv("NIFI_BASE_URL", "https://nifi:8443")
NIFI_USERNAME = os.getenv("NIFI_USERNAME", "nifi")
NIFI_PASSWORD = os.getenv("NIFI_PASSWORD", "nifipassword")
NIFI_PG_ID = os.getenv("NIFI_PG_ID", "d8db7b2c-0199-1000-9b9c-7b0dcb5d32d2")
AGENT_API_KEY = os.getenv("AGENT_API_KEY", "my-secret-key")

# ---------- TASK 1: AGENT CREATION ----------
def call_agent_create_table(**context):
    payload = {
        "api_key": AGENT_API_KEY,
        "file_path": CSV_PATH,
        "table_name": TABLE_NAME,
    }
    print(f"📡 Calling Agent API: {AGENT_URL}")
    r = requests.post(AGENT_URL, json=payload, timeout=180)
    r.raise_for_status()
    resp = r.json()
    ddl = resp.get("ddl", "")
    context["ti"].xcom_push(key="ddl", value=ddl)
    print(f"✅ Agent responded: {resp}")
    return f"✅ Agent created or verified table {TABLE_NAME}"

# ---------- TASK 2: NIFI CONTROL ----------
def nifi_get_token():
    print("🔐 Requesting NiFi access token...")
    r = requests.post(
        f"{NIFI_BASE_URL}/nifi-api/access/token",
        data=f"username={NIFI_USERNAME}&password={NIFI_PASSWORD}",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=False,
        timeout=30,
    )
    r.raise_for_status()
    return r.text.strip()

def nifi_start(pg_id, token):
    print(f"🚀 Starting NiFi Process Group {pg_id}")
    r = requests.put(
        f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{pg_id}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"id": pg_id, "state": "RUNNING"},
        verify=False,
        timeout=30,
    )
    r.raise_for_status()

def nifi_wait(pg_id, token, seconds=600):
    print("⏳ Waiting for NiFi flow to complete...")
    end = time.time() + seconds
    retries = 0
    while time.time() < end:
        try:
            s = requests.get(
                f"{NIFI_BASE_URL}/nifi-api/flow/process-groups/{pg_id}/status",
                headers={"Authorization": f"Bearer {token}"},
                verify=False,
                timeout=30,
            )
            s.raise_for_status()
            queued = (
                s.json()
                .get("processGroupStatus", {})
                .get("aggregateSnapshot", {})
                .get("flowFilesQueued", 1)
            )
            print(f"🔄 Queued FlowFiles: {queued}")
            if queued == 0:
                print("✅ NiFi flow completed successfully.")
                return "done"
        except requests.RequestException as e:
            retries += 1
            print(f"⚠️ Retry {retries}: NiFi status check failed — {e}")
        time.sleep(min(10 * retries, 60))
    raise TimeoutError("❌ NiFi queue not empty after timeout.")

def maybe_run_nifi(**_):
    if not NIFI_PG_ID:
        print("⚠️ Skipping NiFi run — no PG ID provided.")
        return "skipped"
    token = nifi_get_token()
    nifi_start(NIFI_PG_ID, token)
    nifi_wait(NIFI_PG_ID, token)
    return "nifi_done"

# ---------- DAG ----------
with DAG(
    dag_id="s3_elt_orchestrator_v4",
    start_date=datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["agent", "nifi", "elt"],
) as dag:
    agent_task = PythonOperator(
        task_id="create_table_with_agent",
        python_callable=call_agent_create_table,
        
    )

    nifi_task = PythonOperator(
        task_id="trigger_nifi_flow",
        python_callable=maybe_run_nifi,
        
    )

    validate_pg = SQLExecuteQueryOperator(
        task_id="validate_postgres_connection",
        conn_id="postgres_default",
        sql="SELECT 1;",
    )

    agent_task >> nifi_task >> validate_pg
