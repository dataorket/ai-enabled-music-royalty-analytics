#!/usr/bin/env python3
"""
One-command setup: generate data → run ETL → run DQ checks → launch dashboard.
"""
import subprocess, sys

steps = [
    ("Generating synthetic data …", [sys.executable, "data/generate_data.py"]),
    ("Running ETL pipeline …",      [sys.executable, "models/etl.py"]),
    ("Running data quality checks …",[sys.executable, "models/data_quality.py"]),
]

if __name__ == "__main__":
    for label, cmd in steps:
        print(f"\n{'═'*50}\n{label}\n{'═'*50}")
        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"✗ Step failed: {label}")
            sys.exit(1)

    print(f"\n{'═'*50}")
    print("🚀 All ready! Starting dashboard …")
    print(f"{'═'*50}\n")
    subprocess.run([sys.executable, "app/main.py"])
