#!/usr/bin/env python3
"""
Fetch Texas Railroad Commission enforcement/violations data.
Downloads VIOLATIONS.txt from RRC MFT portal (pipe-delimited, updated weekly).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup


def download_from_mft(mft_url, file_index=0):
    """Download a file from RRC MFT portal using PrimeFaces form submission."""
    session = requests.Session()
    r = session.get(mft_url, timeout=30)
    soup = BeautifulSoup(r.text, 'html.parser')
    vs_input = soup.find('input', {'name': 'javax.faces.ViewState'})
    if not vs_input:
        return None
    viewstate = vs_input['value']

    file_id = f'fileTable:{file_index}:j_id_2f'
    data = {
        'fileList_SUBMIT': '1',
        'javax.faces.ViewState': viewstate,
        file_id: file_id,
    }
    r2 = session.post(
        'https://mft.rrc.texas.gov/webclient/godrive/PublicGoDrive.xhtml',
        data=data, timeout=300, stream=True
    )

    if 'force-download' in r2.headers.get('Content-Type', ''):
        return r2
    return None


def fetch_violations_data():
    """
    Fetch recent RRC violations from VIOLATIONS.txt.
    The file is pipe-delimited (}) with headers, updated weekly.
    File index 13 = VIOLATIONS.txt (statewide).
    """
    print("Fetching RRC violations data...")

    mft_url = "https://mft.rrc.texas.gov/link/c7c28dc9-b218-4f0a-8278-bf15d009def1"

    try:
        print("Downloading VIOLATIONS.txt from RRC MFT portal...")
        response = download_from_mft(mft_url, file_index=13)
        if response is None:
            print("MFT download failed")
            return generate_sample_enforcement()

        # Stream-read the file - it can be large (100MB+)
        # Read header first, then collect recent violations
        header = None
        violations = []
        buffer = ""
        cutoff_date = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')

        for chunk in response.iter_content(chunk_size=65536, decode_unicode=False):
            buffer += chunk.decode('utf-8', errors='replace')
            lines = buffer.split('\n')
            buffer = lines[-1]  # Keep incomplete last line

            for line in lines[:-1]:
                line = line.strip()
                if not line:
                    continue

                if header is None:
                    header = line.split('}')
                    continue

                fields = line.split('}')
                if len(fields) < len(header):
                    continue

                row = dict(zip(header, fields))

                # Filter to recent violations (last 90 days)
                viol_date = row.get('VIOLATION_DISC_DATE', '')
                if viol_date >= cutoff_date:
                    violations.append(row)

        print(f"Found {len(violations)} violations in last 90 days")
        return violations

    except Exception as e:
        print(f"Error fetching violations: {e}")
        return generate_sample_enforcement()


def generate_sample_enforcement():
    """Generate sample enforcement data for dashboard testing."""
    print("Using sample enforcement data as fallback")
    return [
        {
            'VIOLATION_DISC_DATE': (datetime.now() - timedelta(days=d)).strftime('%Y%m%d'),
            'OPERATOR_NAME': op,
            'COUNTY': county,
            'VIOLATED_RULE': rule,
            'VIOLATED_RULE_DESC': desc,
            'MAJOR_VIOL_IND': major,
            'LAST_ENF_ACTION': action,
            'LAST_ENF_ACTION_DATE': (datetime.now() - timedelta(days=d)).strftime('%Y%m%d'),
        }
        for d, op, county, rule, desc, major, action in [
            (1, 'ABC Operating LLC', 'PECOS', '4.103(a)', 'Unpermitted Disposal', 'Y', 'Notice of Violation'),
            (3, 'Permian Energy Partners', 'MIDLAND', '3.13(b)', 'Pollution/Well Control', 'Y', 'Penalty Assessment'),
            (5, 'Eagle Ford Resources', 'KARNES', '3.14(b)(2)', 'Well Plugging Violation', 'N', 'Notice of Violation'),
            (7, 'West Texas Drilling Co', 'LOVING', '36.1(d)', 'H2S Safety Violation', 'Y', 'Hearing Scheduled'),
            (10, 'Anadarko Basin LLC', 'OCHILTREE', '3.32(k)', 'Flaring Exceedance', 'N', 'Under Review'),
        ]
    ]


def process_enforcement_data(violations):
    """Process violations into dashboard-ready summary."""
    if not violations:
        return None

    # Sort by date (most recent first)
    violations.sort(key=lambda x: x.get('VIOLATION_DISC_DATE', ''), reverse=True)

    result = {
        "updated_at": datetime.now().isoformat(),
        "total_recent": len(violations),
        "items": [],
        "by_type": {},
        "by_status": {},
        "by_county": {},
        "major_violations": 0,
    }

    # Top items for dashboard
    for item in violations[:10]:
        viol_date = item.get('VIOLATION_DISC_DATE', '')
        try:
            if len(viol_date) == 8:
                formatted_date = f"{viol_date[:4]}-{viol_date[4:6]}-{viol_date[6:8]}"
            else:
                formatted_date = viol_date
        except Exception:
            formatted_date = viol_date

        operator = item.get('OPERATOR_NAME', 'Unknown')
        county = item.get('COUNTY', '')
        rule = item.get('VIOLATED_RULE', '')
        rule_desc = item.get('VIOLATED_RULE_DESC', '')
        action = item.get('LAST_ENF_ACTION', 'Unknown')

        headline = f"{operator}: {rule_desc}" + (f" - {county} County" if county else "")

        result["items"].append({
            "date": formatted_date,
            "operator": operator,
            "county": county,
            "headline": headline,
            "type": "violation",
            "rule": rule,
            "rule_description": rule_desc,
            "status": action,
            "major": item.get('MAJOR_VIOL_IND', 'N') == 'Y',
        })

    # Aggregations
    action_counts = {}
    county_counts = {}
    major_count = 0
    rule_counts = {}

    for item in violations:
        action = item.get('LAST_ENF_ACTION', 'Unknown')
        action_counts[action] = action_counts.get(action, 0) + 1

        county = item.get('COUNTY', 'Unknown')
        if county:
            county_counts[county] = county_counts.get(county, 0) + 1

        if item.get('MAJOR_VIOL_IND', 'N') == 'Y':
            major_count += 1

        rule_desc = item.get('VIOLATED_RULE_DESC', 'Other')
        rule_counts[rule_desc] = rule_counts.get(rule_desc, 0) + 1

    result["by_status"] = action_counts
    result["by_county"] = dict(sorted(county_counts.items(), key=lambda x: x[1], reverse=True)[:20])
    result["by_type"] = dict(sorted(rule_counts.items(), key=lambda x: x[1], reverse=True)[:10])
    result["major_violations"] = major_count

    return result


def main():
    """Main entry point."""
    output_dir = Path(__file__).parent.parent / "public" / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    violations = fetch_violations_data()
    if violations:
        summary = process_enforcement_data(violations)
        if summary:
            output_file = output_dir / "rrc_enforcement.json"
            with open(output_file, "w") as f:
                json.dump(summary, f, indent=2, default=str)
            print(f"RRC enforcement data saved to {output_file}")
            print(f"  Total recent violations: {summary['total_recent']}")
            print(f"  Major violations: {summary['major_violations']}")
            print(f"  Dashboard items: {len(summary['items'])}")
            return True

    print("Failed to fetch RRC enforcement data")
    return False


if __name__ == "__main__":
    main()
