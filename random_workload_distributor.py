import time
import random
from concurrent.futures import ThreadPoolExecutor
from oscar_python.client import Client
from datetime import datetime

"""
GENERAL DESCRIPTION:
This script runs a dynamic and random load experiment across multiple OSCAR clusters. Unlike linear or burst models,this one uses stochastic selection to simulate irregular, real-world traffic.

KEY FEATURES:
1. RANDOM NODE SELECTION:
- In each iteration, the script randomly chooses one of the available clusters to send a job to.
2. DYNAMIC INTERVALS (Jitter):
- Implements variable wait times between launches, defined by a minimum (MIN_INTERVAL) and maximum (MAX_INTERVAL) range.
3. DUAL LIMIT SYSTEM:
- Global Limit (MAX_TOTAL_JOBS): Stops the experiment when a cumulative total is reached.
- Cluster Limit (X_LIMIT_PER_CLUSTER): Prevents a specific node from exceeding its assigned individual capacity.
4. FINAL MONITORING:
- At the end, it generates a statistical breakdown of how many jobs were successfully processed by each node in the Edge-Mesh environment.

"""

# --- Parameter Configuration ---
# Set to None or 0 for INFINITE jobs
MAX_TOTAL_JOBS = 9        # None = No global limit
X_LIMIT_PER_CLUSTER = 3    # None = No individual cluster limit
# Range for the random interval between launches (in seconds)
MIN_INTERVAL = 0.5
MAX_INTERVAL = 3.0

TOKEN = "your-token"
CLUSTERS = [
    {'cluster_id': 'oscar-primary', 'endpoint': 'https://pensive-wescoff8.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'},
    {'cluster_id': 'oscar-jetson', 'endpoint': 'https://jetson.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'},
    {'cluster_id': 'oscar-graspi', 'endpoint': 'https://graspi.im.grycap.net', 'oidc_token': TOKEN, 'ssl': 'True'}
]

SERVICE_NAME = "sleep-test-edge-mesh"
INPUT_DATA = {"message": "Random jitter launch"}

def launch_job(cluster_cfg, current_count):
    """Performs the physical request to the cluster"""
    cid = cluster_cfg['cluster_id']
    try:
        # Reference for NVIDIA: Using documentation and base script
        client = Client(options=cluster_cfg)
        response = client.run_service(SERVICE_NAME, input=INPUT_DATA, async_call=True) 
        ts = datetime.now().strftime("%H:%M:%S")
        
        status = "OK" if response.status_code in [200, 201, 202] else f"ERR {response.status_code}"
        print(f"🎲 [{ts}] {cid} | Job #{current_count} | Status: {status}")
    except Exception as e:
        print(f"⚠️ Error in {cid}: {e}")

def run_flexible_experiment():
    counters = {c['cluster_id']: 0 for c in CLUSTERS}
    total_launched = 0
    
    # Startup informational messages
    total_str = MAX_TOTAL_JOBS if MAX_TOTAL_JOBS else "∞"
    limit_str = X_LIMIT_PER_CLUSTER if X_LIMIT_PER_CLUSTER else "∞"
    print(f"🚀 Starting Flexible Launch:")
    print(f"   - Global Limit: {total_str}")
    print(f"   - Cluster Limit: {limit_str}\n")

    with ThreadPoolExecutor(max_workers=15) as executor:
        try:
            while True:
                # 1. Check Global Stop Condition
                if MAX_TOTAL_JOBS and total_launched >= MAX_TOTAL_JOBS:
                    print(f"\n✅ Reached global limit of {MAX_TOTAL_JOBS} jobs.")
                    break
                
                # 2. Filter available clusters based on X_LIMIT_PER_CLUSTER
                if X_LIMIT_PER_CLUSTER:
                    available = [c for c in CLUSTERS if counters[c['cluster_id']] < X_LIMIT_PER_CLUSTER]
                else:
                    available = CLUSTERS
                
                # 3. If no clusters are available, terminate
                if not available:
                    print(f"\n✅ All clusters have reached their limit of {X_LIMIT_PER_CLUSTER}.")
                    break
                
                # 4. Random selection
                selected = random.choice(available)
                cid = selected['cluster_id']
                
                # 5. Increment and launch
                counters[cid] += 1
                total_launched += 1
                
                executor.submit(launch_job, selected, counters[cid])
                
                # 6. Jitter
                time.sleep(random.uniform(MIN_INTERVAL, MAX_INTERVAL))
                
        except KeyboardInterrupt:
            print("\n🛑 Execution stopped by user.")

    print(f"\n--- Final Results ---")
    print(f"Total launched: {total_launched}")
    for cid, count in counters.items():
        print(f"📊 {cid}: {count} jobs")

if __name__ == "__main__":
    run_flexible_experiment()
