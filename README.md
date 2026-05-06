# oscar-workload-toolkit

This repository contains a set of Python tools designed for experimentation in OSCAR Edge-Mesh environments.

## Tools

- __Sequential Orchestrator__ (pulse_handler.py)

Deterministic, staggered job dispatch for precise flow control.

- __Burst Injector__ (parallel_burst_orchestrator.py)
  
Simultaneous massive load generation for stress testing across all nodes.

- __Random Workload Distributor__ (random_workload_distributor.py)
  
Realistic traffic simulation with random jitter and dynamic limits.

- __Telemetry Monitor__ (edge_metrics_collector.py)
  
-  Real-time collection of hardware metrics (CPU/Memory) and job status.
