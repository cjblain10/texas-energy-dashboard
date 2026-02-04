#!/usr/bin/env python3
"""
Main script to run all data collection pipelines.
Orchestrates fetching from ERCOT, RRC permits, and RRC enforcement.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

# Import our data fetchers
from fetch_ercot import main as fetch_ercot
from fetch_rrc_permits import main as fetch_permits
from fetch_rrc_enforcement import main as fetch_enforcement


def run_all_pipelines():
    """Run all data collection pipelines and generate status report."""

    print("=" * 60)
    print("Texas Energy Dashboard - Data Pipeline")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 60)

    results = {
        "run_timestamp": datetime.now().isoformat(),
        "pipelines": {}
    }

    # Run ERCOT pipeline
    print("\n[1/3] Running ERCOT pipeline...")
    try:
        ercot_success = fetch_ercot()
        results["pipelines"]["ercot"] = {
            "status": "success" if ercot_success else "failed",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        print(f"ERCOT pipeline error: {e}")
        results["pipelines"]["ercot"] = {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

    # Run RRC Permits pipeline
    print("\n[2/3] Running RRC Permits pipeline...")
    try:
        permits_success = fetch_permits()
        results["pipelines"]["rrc_permits"] = {
            "status": "success" if permits_success else "failed",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        print(f"RRC Permits pipeline error: {e}")
        results["pipelines"]["rrc_permits"] = {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

    # Run RRC Enforcement pipeline
    print("\n[3/3] Running RRC Enforcement pipeline...")
    try:
        enforcement_success = fetch_enforcement()
        results["pipelines"]["rrc_enforcement"] = {
            "status": "success" if enforcement_success else "failed",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        print(f"RRC Enforcement pipeline error: {e}")
        results["pipelines"]["rrc_enforcement"] = {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

    # Save pipeline status
    output_dir = Path(__file__).parent.parent / "public" / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    status_file = output_dir / "pipeline_status.json"
    with open(status_file, 'w') as f:
        json.dump(results, f, indent=2)

    # Summary
    print("\n" + "=" * 60)
    print("Pipeline Summary")
    print("=" * 60)

    all_success = True
    for pipeline, status in results["pipelines"].items():
        status_str = status["status"]
        icon = "✓" if status_str == "success" else "✗"
        print(f"  {icon} {pipeline}: {status_str}")
        if status_str != "success":
            all_success = False

    print(f"\nCompleted: {datetime.now().isoformat()}")
    print("=" * 60)

    return all_success


if __name__ == "__main__":
    success = run_all_pipelines()
    sys.exit(0 if success else 1)
