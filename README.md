# Texas Energy Regulatory Pulse

A proof-of-concept dashboard tracking Texas energy regulatory activity, including drilling permits, ERCOT grid interconnections, and RRC enforcement actions.

**Live Demo**: `energy.localinsights.ai` (once deployed)

## Features

- **ERCOT Interconnection Queue**: Pie chart showing generation projects by fuel type (Solar, Wind, Gas, Battery Storage)
- **Drilling Permits (30-day)**: Bar chart of permit activity by major Texas basin (Permian, Eagle Ford, etc.)
- **RRC Enforcement Actions**: Recent enforcement headlines with docket numbers and status
- **County Permit Velocity**: Top counties by permit filing activity

## Architecture

```
texas-energy-dashboard/
├── data-pipeline/           # Python scripts for data collection
│   ├── fetch_ercot.py       # ERCOT interconnection queue
│   ├── fetch_rrc_permits.py # RRC drilling permits
│   ├── fetch_rrc_enforcement.py # RRC enforcement actions
│   ├── run_all.py           # Orchestrator script
│   └── requirements.txt
├── dashboard/               # React app (Vite + TailwindCSS)
│   └── src/
├── public/data/             # JSON output (consumed by dashboard)
├── .github/workflows/       # GitHub Actions for automated data refresh
└── netlify.toml             # Deployment config
```

## Data Sources

| Source | Data | Update Frequency |
|--------|------|------------------|
| ERCOT MIS | Generation Interconnection Queue | Monthly |
| TX Railroad Commission | Drilling Permits | Daily |
| TX Railroad Commission | Enforcement Actions | Weekly |

## Local Development

### Data Pipeline

```bash
cd data-pipeline
pip install -r requirements.txt
python run_all.py
```

### Dashboard

```bash
cd dashboard
npm install
npm run dev
```

## Deployment

### Netlify (Recommended)

1. Connect your GitHub repo to Netlify
2. Set build command: `npm install && npm run build`
3. Set publish directory: `dashboard/dist`
4. Enable auto-deploy on push

### Automated Data Refresh

The GitHub Actions workflow runs daily at 6 AM UTC to:
1. Fetch latest data from ERCOT and RRC
2. Commit updated JSON files to repo
3. Trigger Netlify rebuild

You can also manually trigger the workflow from the Actions tab.

## Customization

### Adding New Data Sources

1. Create a new fetch script in `data-pipeline/`
2. Add it to `run_all.py`
3. Create a new component in `dashboard/src/App.jsx`

### Styling

The dashboard uses TailwindCSS. Modify `dashboard/src/index.css` for global styles or use Tailwind classes directly in components.

## License

MIT

---

*Built by Local Insights AI as a proof of concept.*
