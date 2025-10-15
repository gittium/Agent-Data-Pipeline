from __future__ import annotations
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
import os, requests, json, time

# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------
AGENT_URL = os.getenv("AGENT_URL", "http://agent-schema:8000/health")
NIFI_BASE_URL = os.getenv("NIFI_BASE_URL", "https://nifi:8443")
NIFI_USERNAME = os.getenv("NIFI_USERNAME", "nifi")
NIFI_PASSWORD = os.getenv("NIFI_PASSWORD", "nifipassword")
METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000/api/health")

# Airflow API info for pausing/unpausing DAGs
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-standalone:8080/api/v1")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")

PROD_DAGS = [
    "s3_elt_orchestrator_v4",
    "taxi_transform_dag_v2",
]

# ----------------------------------------------------------------------
# HEALTH CHECK FUNCTIONS
# ----------------------------------------------------------------------

def check_agent_health():
    print(f"🧠 Checking Agent health at {AGENT_URL}")
    r = requests.get(AGENT_URL, timeout=10)
    if r.status_code == 200:
        print(f"✅ Agent healthy: {r.text}")
        return True
    print(f"❌ Agent returned {r.status_code}")
    return False

def check_nifi():
    """Check NiFi health using single-user credentials (NiFi 1.25.0)."""
    base = os.getenv("NIFI_BASE_URL", "https://nifi:8443")
    username = os.getenv("NIFI_USERNAME", "nifi")
    password = os.getenv("NIFI_PASSWORD", "nifipassword")

    print(f"🔐 Checking NiFi connectivity at {base}")

    try:
        # Get access token
        token_resp = requests.post(
            f"{base}/nifi-api/access/token",
            data=f"username={username}&password={password}",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            verify=False,
            timeout=10,
        )
        if token_resp.status_code not in [200,201]:
            print(f"❌ NiFi login failed: {token_resp.status_code}")
            return False
        token = token_resp.text.strip()

        # Check system flow status
        status_resp = requests.get(
            f"{base}/nifi-api/flow/status",
            headers={"Authorization": f"Bearer {token}"},
            verify=False,
            timeout=10,
        )

        if status_resp.status_code in [200, 201]:
            print(f"✅ NiFi healthy ({status_resp.status_code})")
            return True

        print(f"❌ NiFi returned {status_resp.status_code}")
        return False

    except Exception as e:
        print(f"❌ NiFi check failed: {e}")
        return False


def check_metabase_health():
    print(f"📊 Checking Metabase at {METABASE_URL}")
    try:
        r = requests.get(METABASE_URL, timeout=10)
        if r.status_code == 200:
            print("✅ Metabase API healthy.")
            return True
        return False
    except Exception as e:
        print(f"⚠️ Metabase check failed: {e}")
        return False

# ----------------------------------------------------------------------
# AIRFLOW CONTROL FUNCTIONS
# ----------------------------------------------------------------------

def airflow_auth():
    """Authenticate to Airflow API and return session"""
    s = requests.Session()
    s.auth = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    s.headers.update({"Content-Type": "application/json"})
    return s



def evaluate_system_health(**context):
    agent_ok = check_agent_health()
    nifi_ok = check_nifi()
    postgres_ok = True  # The SQL task verifies this
    metabase_ok = check_metabase_health()

    all_ok = agent_ok and nifi_ok and postgres_ok and metabase_ok
    context["ti"].xcom_push(key="system_health_ok", value=all_ok)

    

# ----------------------------------------------------------------------
# DAG DEFINITION
# ----------------------------------------------------------------------

with DAG(
    dag_id="system_health_check_dag",
    description="Auto health monitor that pauses/unpauses DAGs based on component health.",
    start_date=datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",  # can change to "@hourly" for tighter control
    catchup=False,
    tags=["monitoring", "health", "system"],
) as dag:

    postgres_check = SQLExecuteQueryOperator(
        task_id="check_postgres_connection",
        conn_id="postgres_default",
        sql="SELECT 1;",
    )

    evaluate_health = PythonOperator(
        task_id="evaluate_system_health",
        python_callable=evaluate_system_health,
    
    )

    postgres_check >> evaluate_health
