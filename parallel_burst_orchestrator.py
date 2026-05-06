import time
from concurrent.futures import ThreadPoolExecutor
from oscar_python.client import Client
from datetime import datetime

"""
GENERAL DESCRIPTION:
This script implements a parallel burst launch strategy. Unlike sequential launching, this aims to stress or load all clusters simultaneously.

KEY FEATURES:
1. SIMULTANEOUS LAUNCH (Parallel Burst):
- Uses a thread hierarchy (ThreadPoolExecutor) to fire requests to all clusters at the same time .
2. INTENSITY CONTROL:
- Defines the total number of bursts (Y_BURSTS).
- Defines how many jobs are sent per cluster within a burst (X_JOBS_PER_CLUSTER).
3. BURST TIMING:
- Sets a cooldown or waiting period (Z_WAIT_BETWEEN) between complete bursts.
4. RESPONSE MONITORING:
- Validates HTTP status codes (200, 201, 202) to confirm that the asynchronous service in OSCAR started successfully.
"""

# --- Configuration Parameters ---
Y_BURSTS = 2              # Number of bursts to send (N times)
X_JOBS_PER_CLUSTER = 2   # Number of jobs per cluster in each burst
Z_WAIT_BETWEEN = 10       # Time (seconds) to wait between bursts

TOKEN = "your-token"
SERVICE_NAME = "sleep-test-edge-mesh"
INPUT_DATA = {"message": "Parallel burst launch"}

CLUSTERS = [
    {'cluster_id': 'oscar-primary', 'endpoint': 'https://pensive-wescoff8.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'},
    {'cluster_id': 'oscar-jetson', 'endpoint': 'https://jetson.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'},
    {'cluster_id': 'oscar-graspi', 'endpoint': 'https://graspi.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'}
]

def send_single_request(cluster_cfg, job_id):
    """Performs the physical API call to a specific cluster"""
    cid = cluster_cfg['cluster_id']
    try:
        
        client = Client(options=cluster_cfg)
        response = client.run_service(SERVICE_NAME, input=INPUT_DATA, async_call=True) 
        ts = datetime.now().strftime("%H:%M:%S")
        if response.status_code in [200, 201, 202]:
            print(f"   ✅ [{ts}] {cid} | Job {job_id} launched (OK)")
        else:
            print(f"   ❌ [{ts}] {cid} | Job {job_id} error: {response.status_code}")
    except Exception as e:
        print(f"   ⚠️ Error in {cid} | Job {job_id}: {e}")

def process_burst_for_cluster(cluster_cfg):
    """Launches X parallel jobs for a single cluster"""
    with ThreadPoolExecutor(max_workers=X_JOBS_PER_CLUSTER) as executor:
        for i in range(1, X_JOBS_PER_CLUSTER + 1):
            executor.submit(send_single_request, cluster_cfg, i)

def main():
    print(f"🚀 Starting launcher: {Y_BURSTS} bursts of {X_JOBS_PER_CLUSTER} jobs per cluster.")
    print(f"Total target: {Y_BURSTS * X_JOBS_PER_CLUSTER * len(CLUSTERS)} jobs across {len(CLUSTERS)} clusters.\n")
    
    current_burst = 1
    try:
        # Execute until Y bursts are completed
        while current_burst <= Y_BURSTS:
            ts_start = datetime.now().strftime("%H:%M:%S")
            print(f"--- 📦 BURST {current_burst}/{Y_BURSTS} starting at {ts_start} ---")
            
            # Use ThreadPoolExecutor to launch all clusters in parallel
            with ThreadPoolExecutor(max_workers=len(CLUSTERS)) as cluster_executor:
                cluster_executor.map(process_burst_for_cluster, CLUSTERS)
            
            if current_burst < Y_BURSTS:
                print(f"--- ⏳ Waiting {Z_WAIT_BETWEEN} seconds for the next burst ---\n")
                time.sleep(Z_WAIT_BETWEEN)
            
            current_burst += 1
            
        print(f"\n✨ All {Y_BURSTS} bursts completed successfully.")
            
    except KeyboardInterrupt:
        print("\n🛑 Execution terminated by user.")

if __name__ == "__main__":
    main()
