import time
from concurrent.futures import ThreadPoolExecutor
from oscar_python.client import Client
from datetime import datetime

"""
GENERAL DESCRIPTION:
This script automates the staggered launch of jobs across multiple OSCAR clusters. It is designed for load testing or distributed processing in Edge-Mesh architectures.

KEY FEATURES:
1. MULTI-CLUSTER MANAGEMENT: Configures and authenticates requests to different endpoints sequentially.
2. STAGGERED LAUNCH:
- Controls the number of jobs per cluster (x).
- Defines time intervals between individual jobs (y) to prevent saturation.
- Sets wait times between the completion of one cluster and the start of the next (z).
3. ASYNCHRONOUS EXECUTION: Uses 'ThreadPoolExecutor' to send requests to the OSCAR API in a non-blocking manner within each cluster.
4. LOOP CONTROL: Allows defining a maximum number of repetitions. (MAX_CYCLES) for the entire cluster sequence or infinite execution.

"""

# --- Detailed Configuration per Cluster ---
# x = number_of_jobs
# y = interval_between_jobs (seconds)
# z = wait_after_cluster_completion (seconds)

MAX_CYCLES = 2

LAUNCH_CONFIG = [
    {
        'cluster_id': 'oscar-primary',
        'x': 10,  # Launch 10 jobs
        'y': 1,   # Spaced 1 second apart
        'z': 5   # Wait 5 seconds before moving to the next cluster
    },
    {
        'cluster_id': 'oscar-jetson',
        'x': 2,  # Launch 2 jobs
        'y': 1,   # Spaced 1 second apart
        'z': 10   # Wait 10 seconds before moving to the next cluster
    },
    {
        'cluster_id': 'oscar-graspi',
        'x': 1,  # Launch 1 jobs
        'y': 1,   # Spaced 1 second apart
        'z': 10   # Wait 10 seconds before restarting the full cycle
    }
]

# Common data
TOKEN = "your-token"
SERVICE_NAME = "sleep-test-edge-mesh"
INPUT_DATA = {"message": "Staggered launch"}

# Dictionary to map ID with its network configuration (endpoint, etc.)
CLUSTERS_AUTH = {
    'oscar-primary': {'endpoint': 'https://pensive-wescoff8.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'},
    'oscar-jetson': {'endpoint': 'https://jetson.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'},
    'oscar-graspi': {'endpoint': 'https://graspi.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'}
}

def individual_request(cluster_id, job_id):
    """Performs the API call"""
    auth = CLUSTERS_AUTH[cluster_id]
    auth['cluster_id'] = cluster_id # Add the ID for the client
    try:
        client = Client(options=auth)
        response = client.run_service(SERVICE_NAME, input=INPUT_DATA, async_call=True) 
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"   🚀 [{ts}] {cluster_id} | Job {job_id} sent (Status: {response.status_code})")
    except Exception as e:
        print(f"   ⚠️ Error in {cluster_id} Job {job_id}: {e}")

def process_cluster(config):
    """Handles the internal staggering of a cluster"""
    cid = config['cluster_id']
    x = config['x']
    y = config['y']
    z = config['z']
    
    print(f"\n--- 🛰️ Starting Cluster: {cid} ---")
    print(f"--- 🌀 Launching {x} jobs with {y}s separation ---")

    # Use ThreadPoolExecutor to avoid blocking the main thread during submission
    with ThreadPoolExecutor(max_workers=x) as executor:
        for i in range(1, x + 1):
            executor.submit(individual_request, cid, i)
            if i < x: # Do not wait after the last job of the current cluster
                time.sleep(y)
    
    print(f"--- ✅ {cid} completed. Waiting {z}s for the next cluster... ---")
    time.sleep(z)

def main():
    cycle = 1
    try:
        while (not MAX_CYCLES) or (cycle <= MAX_CYCLES):
            
            label = "INFINITE" if not MAX_CYCLES else f"{cycle} of {MAX_CYCLES}"
            print(f"\n\n======= 🔄 STARTING GLOBAL CYCLE {label} =======")
            for conf in LAUNCH_CONFIG:
                process_cluster(conf)
            cycle += 1
    except KeyboardInterrupt:
        print("\n🛑 Execution terminated by user.")

if __name__ == "__main__":
    main()
