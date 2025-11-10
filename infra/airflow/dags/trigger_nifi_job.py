# infra/airflow/dags/trigger_nifi_job.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import os
import urllib3
import subprocess
import shlex

# disable insecure warnings for self-signed certs (local dev only)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# canonical base URL for NiFi REST API (should include /nifi-api)
NIFI_API_BASE = os.getenv("NIFI_URL", "https://localhost:8443/nifi-api").rstrip('/')
NIFI_HOST_IP = os.getenv("NIFI_HOST_IP", "").strip()
NIFI_USERNAME = os.getenv("NIFI_USERNAME", "omar@nifi.com")
NIFI_PASSWORD = os.getenv("NIFI_PASSWORD", "omarnificomm")
PROCESS_GROUP_ID = os.getenv("NIFI_PROCESSGROUP_ID", "").strip()
# run-seconds (if set and >0 the DAG will wait this many seconds between start and stop)
NIFI_RUN_SECONDS = int(os.getenv("NIFI_RUN_SECONDS", "15"))

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


def get_token(session: requests.Session) -> str | None:
    """
    Get NiFi access token.
    1) Try requests against NIFI_API_BASE (requests will send SNI matching the hostname in NIFI_API_BASE).
    2) If that fails and NIFI_HOST_IP provided, use curl with --resolve to force SNI 'localhost' while connecting to host IP.
    """
    print("get_token: NIFI_API_BASE:", NIFI_API_BASE, "NIFI_HOST_IP:", NIFI_HOST_IP)
    creds = {"username": NIFI_USERNAME, "password": NIFI_PASSWORD}

    # 1) try requests (plain)
    try:
        r = session.post(f"{NIFI_API_BASE}/access/token", data=creds, verify=False, timeout=5)
        r.raise_for_status()
        token = r.text.strip()
        if token:
            print("get_token(requests): token_len=", len(token))
            return token
    except Exception as e:
        print(f"get_token(requests) exception: {e}")

    # 2) curl fallback (force SNI 'localhost' while connecting to host IP)
    if not NIFI_HOST_IP:
        print("get_token: no NIFI_HOST_IP in env; cannot run curl fallback.")
        return None

    curl_cmd = [
        "curl", "-sk",
        "--resolve", f"localhost:8443:{NIFI_HOST_IP}",
        "-X", "POST",
        f"https://localhost:8443/nifi-api/access/token",
        "-d", f"username={NIFI_USERNAME}&password={NIFI_PASSWORD}"
    ]
    try:
        print("get_token(curl fallback): running curl (no shell)")
        proc = subprocess.run(curl_cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout = (proc.stdout or "").strip()
        if proc.returncode == 0 and stdout:
            print("get_token(curl): token_len=", len(stdout))
            return stdout
        else:
            print("get_token(curl): returncode=", proc.returncode, "stdout_len=", len(stdout), "stderr_len=", len(proc.stderr or ""))
            return None
    except Exception as e:
        print(f"get_token(curl) exception: {e}")
        return None


def get_processors_in_group(session: requests.Session, token: str | None):
    """
    Returns list of processors in the process group.
    Uses the canonical NIFI_API_BASE.
    """
    if not PROCESS_GROUP_ID:
        raise ValueError("NIFI_PROCESSGROUP_ID not configured in env")

    url = f"{NIFI_API_BASE}/process-groups/{PROCESS_GROUP_ID}/processors"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    print(f"get_processors_in_group: GET {url} (using token? {'yes' if token else 'no'})")
    r = session.get(url, headers=headers, verify=False, timeout=10)
    print(f"get_processors_in_group: status={r.status_code}")
    r.raise_for_status()
    data = r.json()
    return data.get("processors", [])

# To solve the recursion "Running processor group inside our project"

def get_process_groups_in_group(session: requests.Session, token: str | None, parent_pg_id: str):
    """
    Return list of child process-groups contained in parent_pg_id.
    """
    url = f"{NIFI_API_BASE}/process-groups/{parent_pg_id}/process-groups"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    print(f"get_process_groups_in_group: GET {url} (using token? {'yes' if token else 'no'})")
    r = session.get(url, headers=headers, verify=False, timeout=10)
    print(f"get_process_groups_in_group: status={r.status_code}")
    r.raise_for_status()
    data = r.json()
    # NiFi returns an object with "processGroups" (array) for this endpoint
    return data.get("processGroups", []) if isinstance(data, dict) else []
    
def start_processors_in_group(session: requests.Session, token: str | None, pg_id: str) -> int:
    """
    Start all processors directly under the given process-group ID.
    Returns number started.
    """
    procs = get_processors_in_group(session, token) if pg_id == PROCESS_GROUP_ID else get_processors_in_group(session, token_for_pg := token)
    # NOTE: re-using get_processors_in_group signature; call directly by building URL:
    url = f"{NIFI_API_BASE}/process-groups/{pg_id}/processors"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    r = session.get(url, headers=headers, verify=False, timeout=10)
    r.raise_for_status()
    data = r.json()
    processors = data.get("processors", [])
    started = 0
    for p in processors:
        pid = p.get("component", {}).get("id") or p.get("id")
        if not pid:
            continue
        try:
            set_processor_runstate(session, pid, "RUN", token)
            started += 1
        except Exception as e:
            print(f"start_processors_in_group: failed starting processor {pid}: {e}")
    return started
    
def stop_processors_in_group(session: requests.Session, token: str | None, pg_id: str) -> int:
    """
    Stop all processors directly under the given process-group ID.
    Returns number stopped.
    """
    url = f"{NIFI_API_BASE}/process-groups/{pg_id}/processors"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    r = session.get(url, headers=headers, verify=False, timeout=10)
    r.raise_for_status()
    data = r.json()
    processors = data.get("processors", [])
    stopped = 0
    for p in processors:
        pid = p.get("component", {}).get("id") or p.get("id")
        if not pid:
            continue
        try:
            set_processor_runstate(session, pid, "STOP", token)
            stopped += 1
        except Exception as e:
            print(f"stop_processors_in_group: failed stopping processor {pid}: {e}")
    return stopped
    
def recurse_start_group(session: requests.Session, token: str | None, pg_id: str) -> int:
    """
    Recursively start all processors in this process-group and all child process-groups.
    Returns total number of processors started.
    """
    total = 0
    print(f"recurse_start_group: starting processors directly under PG {pg_id}")
    total += start_processors_in_group(session, token, pg_id)

    # find child process-groups
    children = get_process_groups_in_group(session, token, pg_id)
    if not children:
        print(f"recurse_start_group: no child process-groups under PG {pg_id}")
    for child in children:
        child_id = child.get("id") or child.get("component", {}).get("id")
        if not child_id:
            continue
        print(f"recurse_start_group: recursing into child PG {child_id}")
        total += recurse_start_group(session, token, child_id)
    return total
    
def recurse_stop_group(session: requests.Session, token: str | None, pg_id: str) -> int:
    """
    Recursively stop all processors in this process-group and all child process-groups.
    Returns total number of processors stopped.
    """
    total = 0
    print(f"recurse_stop_group: stopping processors directly under PG {pg_id}")
    total += stop_processors_in_group(session, token, pg_id)

    children = get_process_groups_in_group(session, token, pg_id)
    for child in children:
        child_id = child.get("id") or child.get("component", {}).get("id")
        if not child_id:
            continue
        print(f"recurse_stop_group: recursing into child PG {child_id}")
        total += recurse_stop_group(session, token, child_id)
    return total
 
# -------------------- end added helpers --------------------


def set_processor_runstate(session: requests.Session, proc_id: str, state: str, token: str | None):
    """
    PUT /processors/{id}/run-status with revision and desired state (RUNNING / STOPPED)
    Also skip processors that appear to be disabled.
    """
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    get_url = f"{NIFI_API_BASE}/processors/{proc_id}"
    print("set_processor_runstate: GET", get_url)
    r = session.get(get_url, headers=headers, verify=False, timeout=10)
    r.raise_for_status()
    
    proc = r.json()
    comp = proc.get("component", {}) or {}

    # Skip disabled processors
    # Many NiFi versions expose either component.get("state") or component.get("disabled")
    comp_state = comp.get("state", "").upper() if isinstance(comp.get("state", ""), str) else ""
    comp_disabled_flag = comp.get("disabled", None)
    if comp_state == "DISABLED" or comp_disabled_flag is True:
        print(f"set_processor_runstate: skipping disabled processor {proc_id} (state={comp_state}, disabled={comp_disabled_flag})")
        return {"skipped": True, "id": proc_id}
        
    rev = proc.get("revision", {"version": 0})
    payload = {"revision": rev, "state": "RUNNING" if state == "RUN" else "STOPPED"}

    put_url = f"{NIFI_API_BASE}/processors/{proc_id}/run-status"
    print("set_processor_runstate: PUT", put_url, "payload rev:", rev)
    r2 = session.put(put_url, json=payload, headers=headers, verify=False, timeout=10)
    r2.raise_for_status()
    return r2.json()


def start_process_group(**kwargs):
    if not PROCESS_GROUP_ID:
        raise ValueError("NIFI_PROCESSGROUP_ID not configured in env")
    session = requests.Session()
    token = get_token(session)
    if not token:
        raise RuntimeError("Failed to obtain NiFi access token")
    total_started = recurse_start_group(session, token, PROCESS_GROUP_ID)
    print(f"start_process_group: total processors started = {total_started}")
    return {"started": total_started}


def wait_for_duration(**kwargs):
    """
    Sleep for NIFI_RUN_SECONDS (configurable in .env). If NIFI_RUN_SECONDS is 0 -> no wait.
    """
    secs = int(os.getenv("NIFI_RUN_SECONDS", "15"))
    print(f"wait_for_duration: NIFI_RUN_SECONDS={secs}")
    if secs and secs > 0:
        print(f"wait_for_duration: sleeping for {secs} seconds...")
        time.sleep(secs)
    else:
        print("wait_for_duration: no wait configured (0) - continuing immediately")
    return {"slept": secs}
    
    
def stop_process_group(**kwargs):
    if not PROCESS_GROUP_ID:
        raise ValueError("NIFI_PROCESSGROUP_ID not configured in env")
    session = requests.Session()
    token = get_token(session)
    if not token:
        raise RuntimeError("Failed to obtain NiFi access token")
    total_stopped = recurse_stop_group(session, token, PROCESS_GROUP_ID)
    print(f"stop_process_group: total processors stopped = {total_stopped}")
    return {"stopped": total_stopped}


start = PythonOperator(
    task_id="start_process_group",
    python_callable=start_process_group,
    dag=dag,
)

wait = PythonOperator(
    task_id="wait_nifi_run",
    python_callable=wait_for_duration,
    dag=dag,
)

stop = PythonOperator(
    task_id="stop_process_group",
    python_callable=stop_process_group,
    dag=dag,
)

# linear pipeline: start -> (optionally wait) -> stop
start >> wait >> stop