from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime
import requests, os

AGENT_URL = os.getenv("AGENT_URL", "http://agent-schema:8000/health")
AGENT_API_KEY = os.getenv("AGENT_API_KEY", "my-secret-key")

def test_agent_health():
    print(f"🧠 Testing Agent health at {AGENT_URL}")
    r = requests.get(AGENT_URL, timeout=10)
    r.raise_for_status()
    print(f"✅ Agent responded: {r.text}")
    return r.text

def test_agent_create_table():
    payload = {
        "api_key": AGENT_API_KEY,
        "file_path": "/opt/nifi/input_data/yellow_tripdata_2025-01.csv",
        "table_name": "raw.agent_debug_table"
    }
    print("🧠 Sending test table creation request to Agent...")
    r = requests.post("http://agent-schema:8000/nifi_table_create", json=payload, timeout=60)
    r.raise_for_status()
    print(f"✅ Agent response: {r.json()}")
    return r.json()

with DAG(
    dag_id="agent_debug_dag",
    start_date=datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["debug", "agent"],
) as dag:
    check_health = PythonOperator(
        task_id="check_agent_health",
        python_callable=test_agent_health,
    )

    create_table = PythonOperator(
        task_id="test_agent_create_table",
        python_callable=test_agent_create_table,
    )

    check_health >> create_table
