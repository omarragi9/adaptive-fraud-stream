# infra/airflow/dags/trigger_nifi_job.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import os
import urllib3
import subprocess, shlex

# disable insecure warnings for self-signed certs (local dev only)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NIFI_URL = os.getenv("NIFI_URL", "https://localhost:8443/nifi-api")
NIFI_HOST_IP = os.getenv("NIFI_HOST_IP", None)
NIFI_USER = os.getenv("NIFI_USER", None)
NIFI_PASS = os.getenv("NIFI_PASS", None)
PROCESS_GROUP_ID = os.getenv("NIFI_PROCESSGROUP_ID", "")

default_args = {
    "owner": "my local machine",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2025, 11, 1),
}

dag = DAG(
    dag_id="trigger_nifi_process_group",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

def get_token(session: requests.Session):
    """
    Authenticate against NiFi and return bearer token.
    Strategy:
      1) Try Python requests (regular).
      2) If that fails (e.g. Invalid SNI from container), fallback to calling curl with --resolve
         so TLS SNI = 'localhost' while TCP connecting to the host IP provided via env NIFI_HOST_IP.
    """
    if not (NIFI_USER and NIFI_PASS):
        print("get_token: NIFI_USER/PASS not configured; skipping auth.")
        return None

    url = f"{NIFI_URL}/access/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        r = session.post(url, data={"username": NIFI_USER, "password": NIFI_PASS},
                         headers=headers, verify=False, timeout=15)
        print(f"get_token(requests): status={r.status_code}")
        if r.ok and r.text:
            token = r.text.strip().strip('"')
            print(f"get_token(requests): token size={len(token)}")
            return token
        else:
            print(f"get_token(requests): failed status={r.status_code} body={r.text}")
    except Exception as exc:
        print(f"get_token(requests) exception: {exc}")

    # Fallback to curl --resolve (force SNI hostname -> 'localhost', connect to host IP)
    host_ip = os.getenv("NIFI_HOST_IP", None)
    if not host_ip:
        print("get_token: no NIFI_HOST_IP in env; cannot run curl fallback.")
        return None

    curl_cmd = (
        f"curl -s -k --resolve 'localhost:8443:{host_ip}' "
        f"-X POST 'https://localhost:8443/nifi-api/access/token' "
        f"-d 'username={NIFI_USER}&password={NIFI_PASS}'"
    )
    print(f"get_token(curl fallback): running curl via shell (masked)")

    try:
        # run shell, capture stdout (the token) and status via returncode
        completed = subprocess.run(curl_cmd, shell=True, check=False, capture_output=True, text=True)
        out = completed.stdout.strip()
        rc = completed.returncode
        print(f"get_token(curl): returncode={rc} stdout_len={len(out)} stderr_len={len(completed.stderr)}")
        if rc == 0 and out:
            token = out.strip().strip('"')
            print(f"get_token(curl): token size={len(token)}")
            return token
        else:
            print(f"get_token(curl): failed. stderr: {completed.stderr}")
            return None
    except Exception as exc:
        print(f"get_token(curl) exception: {exc}")
        return None

def get_processors_in_group(session: requests.Session, token: str):
    """
    Returns list of processors in the process group.
    Always uses the canonical NIFI_URL (which must include scheme & /nifi-api).
    """
    # Ensure base URL has scheme and nifi-api path
    base = NIFI_URL.rstrip('/')  # e.g. https://localhost:8443/nifi-api
    url = f"{base}/process-groups/{PROCESS_GROUP_ID}/processors"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    print(f"get_processors_in_group: GET {url} (using token? {'yes' if token else 'no'})")
    r = session.get(url, headers=headers, verify=False, timeout=10)
    print(f"get_processors_in_group: status={r.status_code}")
    r.raise_for_status()
    data = r.json()
    return data.get("processors", [])

def set_processor_runstate(session: requests.Session, proc_id: str, state: str, token: str):
    """
    PUT /processors/{id}/run-status with revision and desired state (RUNNING / STOPPED)
    """
    # fetch current to get revision
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    r = session.get(f"{NIFI_URL}/processors/{proc_id}", headers=headers, verify=False, timeout=10)
    r.raise_for_status()
    proc = r.json()
    rev = proc.get("revision", {"version": 0})
    payload = {"revision": rev, "state": "RUNNING" if state == "RUN" else "STOPPED"}
    r2 = session.put(f"{NIFI_URL}/processors/{proc_id}/run-status", json=payload, headers=headers, verify=False, timeout=10)
    r2.raise_for_status()
    return r2.json()

def start_process_group(**kwargs):
    if not PROCESS_GROUP_ID:
        raise ValueError("NIFI_PROCESSGROUP_ID not configured in env")
    session = requests.Session()
    token = get_token(session)
    procs = get_processors_in_group(session, token)
    if not procs:
        # nothing to start
        return {"started": 0}
    started = 0
    for p in procs:
        pid = p.get("id")
        try:
            set_processor_runstate(session, pid, "RUN", token)
            started += 1
        except Exception as e:
            # record but continue
            print(f"failed starting processor {pid}: {e}")
    return {"started": started}

def stop_process_group(**kwargs):
    if not PROCESS_GROUP_ID:
        raise ValueError("NIFI_PROCESSGROUP_ID not configured in env")
    session = requests.Session()
    token = get_token(session)
    procs = get_processors_in_group(session, token)
    stopped = 0
    for p in procs:
        pid = p.get("id")
        try:
            set_processor_runstate(session, pid, "STOP", token)
            stopped += 1
        except Exception as e:
            print(f"failed stopping processor {pid}: {e}")
    return {"stopped": stopped}

start = PythonOperator(
    task_id="start_process_group",
    python_callable=start_process_group,
    dag=dag,
)

stop = PythonOperator(
    task_id="stop_process_group",
    python_callable=stop_process_group,
    dag=dag,
)

# linear pipeline: start -> (optionally wait) -> stop
start >> stop
