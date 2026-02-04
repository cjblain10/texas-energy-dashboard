#!/usr/bin/env python3
"""
Fetch ERCOT Generation Interconnection Queue data via gridstatus library.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO)


def fetch_ercot_queue():
    """Fetch ERCOT interconnection queue using gridstatus."""
    print("Fetching ERCOT interconnection queue via gridstatus...")

    try:
        import gridstatus
        ercot = gridstatus.Ercot()
        df = ercot.get_interconnection_queue()
        print(f"Retrieved {len(df)} projects from ERCOT GIS report")
        return df
    except ImportError:
        print("gridstatus not installed, trying direct ERCOT API...")
        return fetch_ercot_direct()
    except Exception as e:
        print(f"gridstatus error: {e}, trying direct ERCOT API...")
        return fetch_ercot_direct()


def fetch_ercot_direct():
    """Fallback: fetch GIS report directly from ERCOT API."""
    import requests
    import pandas as pd
    from io import BytesIO

    try:
        # Get document list
        list_url = "https://www.ercot.com/misapp/servlets/IceDocListJsonWS"
        params = {"reportTypeId": "15933"}
        r = requests.get(list_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        docs = data.get("ListDocsByRptTypeRes", {}).get("DocumentList", [])
        if not docs:
            print("No GIS report documents found")
            return None

        # Get the most recent document
        doc = docs[0]["Document"] if isinstance(docs[0], dict) and "Document" in docs[0] else docs[0]
        doc_id = doc["DocID"]
        friendly_name = doc.get("FriendlyName", "unknown")
        print(f"Downloading: {friendly_name} (DocID: {doc_id})")

        # Download the Excel file
        dl_url = f"https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId={doc_id}"
        r2 = requests.get(dl_url, timeout=120)
        r2.raise_for_status()

        # Read Project Details sheet (skip header rows)
        for skip in range(6):
            try:
                df = pd.read_excel(BytesIO(r2.content), sheet_name="Project Details - Large Gen",
                                   header=skip, engine="openpyxl")
                named_cols = [c for c in df.columns if "Unnamed" not in str(c)]
                if len(named_cols) > 5:
                    print(f"Parsed Excel with header at row {skip}: {len(df)} rows")
                    return df
            except Exception:
                continue

        print("Could not parse ERCOT Excel file")
        return None

    except Exception as e:
        print(f"Direct ERCOT API error: {e}")
        return None


def process_queue_data(df):
    """Process queue DataFrame into dashboard-ready JSON."""
    if df is None or df.empty:
        return None

    # Normalize fuel column
    fuel_col = None
    for col in df.columns:
        if col.lower() in ("fuel", "fuel type", "technology", "generation type"):
            fuel_col = col
            break

    cap_col = None
    for col in df.columns:
        if "capacity" in col.lower() and "mw" in col.lower():
            cap_col = col
            break
        if col.lower() == "capacity (mw)":
            cap_col = col
            break

    status_col = None
    for col in df.columns:
        if col.lower() in ("status", "gim study phase"):
            status_col = col
            break

    county_col = None
    for col in df.columns:
        if col.lower() == "county":
            county_col = col
            break

    result = {
        "updated_at": datetime.now().isoformat(),
        "total_projects": len(df),
        "total_capacity_gw": 0,
        "by_fuel_type": {},
        "by_status": {},
        "by_county": {},
    }

    # Normalize fuel types to dashboard categories
    fuel_map = {
        "solar": "Solar", "photovoltaic": "Solar", "photovoltaic solar": "Solar",
        "wind": "Wind", "wind turbine": "Wind",
        "gas": "Gas", "natural gas": "Gas", "gas-cc": "Gas", "gas-ct": "Gas",
        "gas-ic": "Gas", "gas-st": "Gas", "gas - cc": "Gas", "gas - ct": "Gas",
        "battery": "Battery Storage", "battery storage": "Battery Storage",
        "storage": "Battery Storage", "energy storage": "Battery Storage",
        "nuclear": "Nuclear", "coal": "Coal", "biomass": "Other",
        "hydrogen": "Other", "other": "Other",
    }

    if fuel_col and cap_col:
        import pandas as pd
        df[cap_col] = pd.to_numeric(df[cap_col], errors="coerce")

        # Map fuel types
        df["_fuel_category"] = df[fuel_col].astype(str).str.lower().str.strip().map(
            lambda x: next((v for k, v in fuel_map.items() if k in x), "Other")
        )

        fuel_summary = df.groupby("_fuel_category")[cap_col].agg(["sum", "count"]).reset_index()
        for _, row in fuel_summary.iterrows():
            cat = row["_fuel_category"]
            if cat and str(cat).lower() not in ("nan", "none", ""):
                result["by_fuel_type"][cat] = {
                    "capacity_mw": round(float(row["sum"]), 1) if not pd.isna(row["sum"]) else 0,
                    "capacity_gw": round(float(row["sum"]) / 1000, 2) if not pd.isna(row["sum"]) else 0,
                    "count": int(row["count"]),
                }

        result["total_capacity_gw"] = round(df[cap_col].sum() / 1000, 1)

    # Status breakdown
    if status_col:
        import pandas as pd
        counts = df[status_col].value_counts().to_dict()
        result["by_status"] = {str(k): int(v) for k, v in counts.items()
                               if pd.notna(k) and str(k).lower() not in ("nan", "none", "")}

    # County breakdown (top 15)
    if county_col and cap_col:
        import pandas as pd
        county_agg = df.groupby(county_col)[cap_col].agg(["sum", "count"]).reset_index()
        county_agg = county_agg.nlargest(15, "sum")
        for _, row in county_agg.iterrows():
            name = str(row[county_col]).strip()
            if name and name.lower() not in ("nan", "none", ""):
                result["by_county"][name] = {
                    "capacity_mw": round(float(row["sum"]), 1) if not pd.isna(row["sum"]) else 0,
                    "count": int(row["count"]),
                }

    return result


def main():
    """Main entry point."""
    output_dir = Path(__file__).parent.parent / "public" / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    df = fetch_ercot_queue()
    if df is not None and not df.empty:
        summary = process_queue_data(df)
        if summary:
            output_file = output_dir / "ercot_queue.json"
            with open(output_file, "w") as f:
                json.dump(summary, f, indent=2, default=str)
            print(f"ERCOT data saved to {output_file}")
            print(f"  Total projects: {summary['total_projects']}")
            print(f"  Total capacity: {summary['total_capacity_gw']} GW")
            print(f"  Fuel types: {len(summary['by_fuel_type'])}")
            return True

    print("Failed to fetch ERCOT data")
    return False


if __name__ == "__main__":
    main()
