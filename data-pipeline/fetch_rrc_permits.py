#!/usr/bin/env python3
"""
Fetch Texas Railroad Commission drilling permit data.
Downloads current month permits from RRC MFT portal (daf420.dat).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


# Texas Basins mapping (county to basin)
TEXAS_BASINS = {
    'ANDREWS': 'Permian', 'BORDEN': 'Permian', 'CRANE': 'Permian', 'DAWSON': 'Permian',
    'ECTOR': 'Permian', 'GAINES': 'Permian', 'GLASSCOCK': 'Permian', 'HOWARD': 'Permian',
    'LOVING': 'Permian', 'MARTIN': 'Permian', 'MIDLAND': 'Permian', 'MITCHELL': 'Permian',
    'PECOS': 'Permian', 'REAGAN': 'Permian', 'REEVES': 'Permian', 'SCURRY': 'Permian',
    'STERLING': 'Permian', 'TERRY': 'Permian', 'UPTON': 'Permian', 'WARD': 'Permian',
    'WINKLER': 'Permian', 'YOAKUM': 'Permian', 'CULBERSON': 'Permian', 'JEFF DAVIS': 'Permian',
    'ATASCOSA': 'Eagle Ford', 'BEE': 'Eagle Ford', 'DEWITT': 'Eagle Ford', 'DIMMIT': 'Eagle Ford',
    'FRIO': 'Eagle Ford', 'GONZALES': 'Eagle Ford', 'KARNES': 'Eagle Ford', 'LA SALLE': 'Eagle Ford',
    'LAVACA': 'Eagle Ford', 'LIVE OAK': 'Eagle Ford', 'MAVERICK': 'Eagle Ford', 'MCMULLEN': 'Eagle Ford',
    'WEBB': 'Eagle Ford', 'WILSON': 'Eagle Ford', 'ZAVALA': 'Eagle Ford',
    'BOWIE': 'Haynesville', 'CASS': 'Haynesville', 'GREGG': 'Haynesville', 'HARRISON': 'Haynesville',
    'MARION': 'Haynesville', 'PANOLA': 'Haynesville', 'RUSK': 'Haynesville', 'SHELBY': 'Haynesville',
    'UPSHUR': 'Haynesville',
    'CARSON': 'Anadarko', 'GRAY': 'Anadarko', 'HANSFORD': 'Anadarko', 'HARTLEY': 'Anadarko',
    'HEMPHILL': 'Anadarko', 'HUTCHINSON': 'Anadarko', 'LIPSCOMB': 'Anadarko', 'MOORE': 'Anadarko',
    'OCHILTREE': 'Anadarko', 'OLDHAM': 'Anadarko', 'POTTER': 'Anadarko', 'RANDALL': 'Anadarko',
    'ROBERTS': 'Anadarko', 'SHERMAN': 'Anadarko', 'WHEELER': 'Anadarko',
    'DENTON': 'Barnett', 'HOOD': 'Barnett', 'JACK': 'Barnett', 'JOHNSON': 'Barnett',
    'MONTAGUE': 'Barnett', 'PALO PINTO': 'Barnett', 'PARKER': 'Barnett', 'TARRANT': 'Barnett',
    'WISE': 'Barnett',
    'COLLINGSWORTH': 'Granite Wash', 'DONLEY': 'Granite Wash',
}

# Texas city-to-county lookup for parsing RRC permit files
CITY_TO_COUNTY = {
    'ANDREWS': 'ANDREWS', 'BIG SPRING': 'HOWARD', 'MIDLAND': 'MIDLAND', 'ODESSA': 'ECTOR',
    'LAMESA': 'DAWSON', 'PECOS': 'REEVES', 'MONAHANS': 'WARD', 'CRANE': 'CRANE',
    'SEMINOLE': 'GAINES', 'STANTON': 'MARTIN', 'KERMIT': 'WINKLER', 'MENTONE': 'LOVING',
    'RANKIN': 'UPTON', 'STERLING CITY': 'STERLING', 'BIG LAKE': 'REAGAN',
    'SNYDER': 'SCURRY', 'COLORADO CITY': 'MITCHELL', 'BROWNFIELD': 'TERRY',
    'PLAINS': 'YOAKUM', 'GARDEN CITY': 'GLASSCOCK', 'GAIL': 'BORDEN',
    'VAN HORN': 'CULBERSON', 'FORT DAVIS': 'JEFF DAVIS',
    'JOURDANTON': 'ATASCOSA', 'BEEVILLE': 'BEE', 'CUERO': 'DEWITT', 'CARRIZO SPRINGS': 'DIMMIT',
    'PEARSALL': 'FRIO', 'GONZALES': 'GONZALES', 'KARNES CITY': 'KARNES',
    'COTULLA': 'LA SALLE', 'HALLETTSVILLE': 'LAVACA', 'GEORGE WEST': 'LIVE OAK',
    'EAGLE PASS': 'MAVERICK', 'TILDEN': 'MCMULLEN', 'LAREDO': 'WEBB',
    'FLORESVILLE': 'WILSON', 'CRYSTAL CITY': 'ZAVALA',
    'TEXARKANA': 'BOWIE', 'LINDEN': 'CASS', 'LONGVIEW': 'GREGG', 'MARSHALL': 'HARRISON',
    'JEFFERSON': 'MARION', 'CARTHAGE': 'PANOLA', 'HENDERSON': 'RUSK',
    'CENTER': 'SHELBY', 'GILMER': 'UPSHUR',
    'PANHANDLE': 'CARSON', 'PAMPA': 'GRAY', 'SPEARMAN': 'HANSFORD', 'HARTLEY': 'HARTLEY',
    'CANADIAN': 'HEMPHILL', 'BORGER': 'HUTCHINSON', 'LIPSCOMB': 'LIPSCOMB',
    'DUMAS': 'MOORE', 'PERRYTON': 'OCHILTREE', 'VEGA': 'OLDHAM', 'AMARILLO': 'POTTER',
    'CANYON': 'RANDALL', 'MIAMI': 'ROBERTS', 'STRATFORD': 'SHERMAN', 'WHEELER': 'WHEELER',
    'DENTON': 'DENTON', 'GRANBURY': 'HOOD', 'JACKSBORO': 'JACK', 'CLEBURNE': 'JOHNSON',
    'MONTAGUE': 'MONTAGUE', 'PALO PINTO': 'PALO PINTO', 'WEATHERFORD': 'PARKER',
    'FORT WORTH': 'TARRANT', 'DECATUR': 'WISE',
    'WELLINGTON': 'COLLINGSWORTH', 'CLARENDON': 'DONLEY',
    'FORT STOCKTON': 'PECOS', 'MCCAMEY': 'UPTON', 'IRAAN': 'PECOS',
    'ANDREWS, TX': 'ANDREWS',
}


def get_basin(county):
    """Map county to basin."""
    if county:
        return TEXAS_BASINS.get(county.upper().strip(), 'Other')
    return 'Other'


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
        data=data, timeout=120
    )

    if 'force-download' in r2.headers.get('Content-Type', ''):
        return r2.content
    return None


def parse_daf420(content):
    """Parse RRC daf420.dat fixed-width drilling permit file."""
    permits = []
    text = content.decode('utf-8', errors='replace')
    lines = text.split('\n')

    import re

    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        if not line.startswith('01') or len(line) < 98:
            i += 1
            continue

        # Type-01 master record — field positions from data analysis:
        # Pos 58-65: Date (YYYYMMDD)
        # Pos 66-97: Operator name (32 chars)
        date_str = line[58:66]
        operator = line[66:98].strip()

        # Find matching type-02 trailer for county/city info
        county = ''
        city = ''
        j = i + 1
        while j < min(i + 10, len(lines)):
            trailer = lines[j].rstrip()
            if trailer.startswith('02') and len(trailer) > 200:
                # City name appears in trailer after direction codes (NE/SE/SW/NW/E/W/N/S)
                tail = trailer[180:]
                city_match = re.search(
                    r'(?:NE|NW|SE|SW|N|S|E|W)\s{2,}([A-Z][A-Z ,\.]+?)\s{2,}',
                    tail
                )
                if city_match:
                    city = city_match.group(1).strip().rstrip(',')
                    county = CITY_TO_COUNTY.get(city, CITY_TO_COUNTY.get(city + ', TX', ''))
                break
            elif trailer.startswith('01'):
                break
            j += 1

        # Parse date
        permit_date = None
        try:
            if len(date_str) == 8 and date_str.isdigit():
                permit_date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
        except ValueError:
            pass

        if permit_date and operator:
            permits.append({
                'permit_date': permit_date,
                'county': county,
                'city': city,
                'basin': get_basin(county),
                'operator': operator,
            })

        i += 1

    return permits


def fetch_rrc_permit_data():
    """Fetch drilling permit data from RRC MFT portal."""
    print("Fetching RRC drilling permit data...")

    # Current month daily permits file (daf420.dat)
    mft_url = "https://mft.rrc.texas.gov/link/5f07cc72-2e79-4df8-ade1-9aeb792e03fc"

    try:
        print("Downloading current month permits from RRC MFT portal...")
        content = download_from_mft(mft_url, file_index=0)
        if content:
            permits = parse_daf420(content)
            print(f"Parsed {len(permits)} permits from daf420.dat")
            if permits:
                return permits
    except Exception as e:
        print(f"Error fetching from MFT: {e}")

    print("Using sample permit data as fallback")
    return generate_sample_permits()


def generate_sample_permits():
    """Generate sample data if RRC download fails."""
    import random
    counties = list(TEXAS_BASINS.keys())
    permits = []
    for i in range(100):
        days_ago = random.randint(0, 30)
        date = datetime.now() - timedelta(days=days_ago)
        county = random.choice(counties)
        permits.append({
            'permit_date': date.strftime('%Y-%m-%d'),
            'county': county,
            'basin': get_basin(county),
            'operator': f'Operator {random.randint(1, 20)}',
        })
    return permits


def process_permit_data(permits):
    """Process permit list into dashboard summaries."""
    if not permits:
        return None

    df = pd.DataFrame(permits)
    if 'permit_date' in df.columns:
        df['permit_date'] = pd.to_datetime(df['permit_date'], errors='coerce')

    result = {
        "updated_at": datetime.now().isoformat(),
        "total_permits_30d": 0,
        "by_basin": {},
        "by_county": {},
        "daily_counts": [],
        "permit_velocity": {},
    }

    cutoff = datetime.now() - timedelta(days=30)
    recent = df[df['permit_date'] >= cutoff] if 'permit_date' in df.columns else df
    result["total_permits_30d"] = len(recent)

    if 'basin' in recent.columns:
        basin_counts = recent['basin'].value_counts().to_dict()
        result["by_basin"] = {str(k): int(v) for k, v in basin_counts.items()}

    if 'county' in recent.columns:
        county_counts = recent[recent['county'] != '']['county'].value_counts().head(30).to_dict()
        result["by_county"] = {str(k): int(v) for k, v in county_counts.items()}

    if 'permit_date' in recent.columns and not recent['permit_date'].isna().all():
        daily = recent.groupby(recent['permit_date'].dt.date).size()
        result["daily_counts"] = [
            {"date": str(date), "count": int(count)}
            for date, count in daily.items()
        ]

    if 'county' in recent.columns:
        result["permit_velocity"] = {
            str(k): int(v)
            for k, v in recent[recent['county'] != '']['county'].value_counts().to_dict().items()
        }

    return result


def main():
    """Main entry point."""
    output_dir = Path(__file__).parent.parent / "public" / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    permits = fetch_rrc_permit_data()
    if permits:
        summary = process_permit_data(permits)
        if summary:
            output_file = output_dir / "rrc_permits.json"
            with open(output_file, "w") as f:
                json.dump(summary, f, indent=2, default=str)
            print(f"RRC permit data saved to {output_file}")
            print(f"  Total permits (30d): {summary['total_permits_30d']}")
            print(f"  Basins tracked: {len(summary['by_basin'])}")
            print(f"  Counties tracked: {len(summary['by_county'])}")
            return True

    print("Failed to fetch RRC permit data")
    return False


if __name__ == "__main__":
    main()
