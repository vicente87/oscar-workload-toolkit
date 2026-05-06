import time
from datetime import datetime
from oscar_python.client import Client
import os
import csv
import threading
from concurrent.futures import ThreadPoolExecutor

"""
GENERAL DESCRIPTION:
This script acts as a telemetry system for OSCAR federated environments. Its main function is to periodically collect health and execution status metrics from all configured clusters.

KEY FEATURES:
1. CLUSTER RESOURCE MONITORING:
- Extracts critical hardware data: available CPU (total_free_cores) and free memory (total_free_bytes) from each node.
2. JOB STATUS TRACKING:
- Queryes the specific service to track the status of jobs: Succeeded, Failed, Running, and Pending.
3. REAL-TIME DATA PERSISTENCE:
- Automatically generates a CSV file with timestamps for later analysis.
Uses a thread lock (threading.Lock) to ensure that writing to The file is secure and there is no data corruption.
4. PARALLEL AND CYCLIC EXECUTION:
- Uses 'ThreadPoolExecutor' to query all clusters simultaneously in each sampling cycle, defined by a wait time (WAIT_TIME).
METRICS COLLECTED:
- Resources: CPU_Alloc, Mem_Alloc, CPU_Sched, Mem_Sched.
- States: Detailed count of job health for each cluster in the mesh.
"""


# --- Configuration ---
TOKEN = "your-token"
WAIT_TIME = 5
CLUSTERS = [
    {'cluster_id': 'oscar-primary', 'endpoint': 'https://pensive-wescoff8.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'},
    {'cluster_id': 'oscar-jetson', 'endpoint': 'https://jetson.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'},
    {'cluster_id': 'oscar-graspi', 'endpoint': 'https://graspi.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'}
]

SERVICE_NAME = "sleep-test-edge-mesh"
SCRIPT_START = datetime.now().strftime("%Y%m%d_%H%M")
LOG_FILE = f"edge_federation_metrics_{SCRIPT_START}.csv"

# Ensure the data directory exists
#os.makedirs("data", exist_ok=True)

# Lock for synchronized CSV writing
csv_lock = threading.Lock()

def initialize_csv():
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "Cluster", "CPU_Alloc", "Mem_Alloc", "CPU_Sched", "Mem_Sched", "Succeeded", "Failed", "Running", "Pending"])

def process_cluster(cluster_cfg):
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    cluster_id = cluster_cfg['cluster_id']
    
    # Initialize metric variables
    stats = {"Succeeded": 0, "Failed": 0, "Running": 0, "Pending": 0}
    cpu_Alloc = mem_Alloc = cpu_Sched = mem_Sched = 0

    try:
        client = Client(options=cluster_cfg)
        
        # 1. Get Cluster Status
        response = client.get_cluster_status()
        if response.status_code == 200:
            data = response.json()
            metrics = data['cluster']['metrics']
            cpu_Alloc = metrics['cpu']['total_free_cores']
            mem_Alloc = metrics['memory']['total_free_bytes']
            cpu_Sched = metrics['cpu']['total_schedulable_cores']
            mem_Sched = metrics['memory']['total_schedulable_bytes']
            
            print(f"📡 [{timestamp}] {cluster_id} OK")
        else:
            print(f"❌ [{timestamp}] Error {response.status_code} on {cluster_id}")

        # 2. Get Jobs
        jobs_list = client.list_jobs(SERVICE_NAME)
        if jobs_list.status_code == 200:
            jobs_data = jobs_list.json()
            for job in jobs_data.get('jobs', {}).values():
                status = job.get('status')
                if status in stats:
                    stats[status] += 1
        
        # 3. Save data (Thread-safe)
        with csv_lock:
            with open(LOG_FILE, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp, cluster_id, cpu_Alloc, mem_Alloc, cpu_Sched, mem_Sched,
                    stats["Succeeded"], stats["Failed"], stats["Running"], stats["Pending"]
                ])

    except Exception as e:
        print(f"⚠️ Error in cluster {cluster_id}: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    initialize_csv()
    print(f"Starting multithreaded monitoring in {LOG_FILE}...")
    
    while True:
        # Use ThreadPoolExecutor to launch requests in parallel
        with ThreadPoolExecutor(max_workers=len(CLUSTERS)) as executor:
            executor.map(process_cluster, CLUSTERS)
        
        print(f"--- Cycle completed. Waiting {WAIT_TIME} seconds... ---")
        time.sleep(WAIT_TIME)
