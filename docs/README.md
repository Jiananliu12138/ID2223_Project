# GitHub Pages Deployment

This folder contains the static HTML version of the SE3 Electricity Price Prediction dashboard, deployable to GitHub Pages without any backend infrastructure.

## Structure

```
docs/
├── index.html                    # Main dashboard (Plotly.js + vanilla JavaScript)
├── predictions/
│   └── latest_predictions.json   # Prediction data (synced from project root)
└── README.md                     # This file
```

## How It Works

The `index.html` file is a standalone, responsive dashboard that:

1. **Loads data dynamically** from `./predictions/latest_predictions.json`
2. **Renders interactive charts** using Plotly.js library (CDN)
3. **Displays metrics, heatmaps, and laundry timer** (same as Streamlit app)
4. **Works with zero backend** - pure client-side JavaScript

## Deployment to GitHub Pages

### Method 1: Automatic (GitHub Actions)

Create `.github/workflows/deploy-pages.yml`:

```yaml
name: Update GitHub Pages

on:
  push:
    paths:
      - 'predictions/latest_predictions.json'
    branches:
      - main
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Copy predictions to docs
        run: cp predictions/latest_predictions.json docs/predictions/latest_predictions.json

      - name: Commit and push
        run: |
          git config --local user.email "action@github.com"
          git config --local user.name "GitHub Action"
          git add docs/predictions/
          git commit -m "Auto-update predictions" || true
          git push
```

Then enable GitHub Pages in repository Settings:
- Branch: `main`
- Folder: `docs/`

### Method 2: Manual

```bash
# From project root
cp predictions/latest_predictions.json docs/predictions/latest_predictions.json
git add docs/predictions/
git commit -m "Update predictions"
git push origin main
```

Then enable GitHub Pages in Settings → Pages.

## Features

- ✅ **Interactive Charts**: Hover tooltips, zoom, pan
- ✅ **Mode-Based Coloring**: Blue (backtest) + Red (forecast) + Green (actual)
- ✅ **Laundry Timer**: Identifies 4 cheapest periods
- ✅ **Metrics**: Average, min, max, MAE
- ✅ **Heatmap**: Hourly price distribution by date
- ✅ **Data Table**: Full prediction results (first 50 rows)
- ✅ **Responsive Design**: Works on mobile/tablet/desktop
- ✅ **No Dependencies**: Only CDN-based Plotly.js

## Troubleshooting

**"No data loaded" error:**
1. Ensure `docs/predictions/latest_predictions.json` exists
2. Check file has valid JSON (run `cat docs/predictions/latest_predictions.json`)
3. Open browser console (F12) for detailed error messages

**Charts not rendering:**
1. Verify Plotly.js CDN is accessible (check browser console for 404 errors)
2. Ensure JSON timestamps are ISO 8601 format

**Old data showing:**
1. Hard refresh (Cmd+Shift+R on Mac, Ctrl+Shift+R on Windows)
2. Clear browser cache

## Data Format

Expected JSON structure in `latest_predictions.json`:

```json
[
  {
    "timestamp": "2026-01-05 17:00:00+00:00",
    "predicted_price": 200.74,
    "mode": "forecast",           // "forecast" or "backtest"
    "actual_price": 225.01,       // (optional, for backtest only)
    "error": 24.27,               // (optional)
    "abs_error": 24.27            // (optional)
  },
  ...
]
```

## Updates

To update predictions on GitHub Pages:

```bash
# After running inference pipeline
python pipelines/4_inference_pipeline.py

# Copy to docs folder
cp predictions/latest_predictions.json docs/predictions/

# Commit and push
git add docs/predictions/
git commit -m "Update electricity price predictions"
git push origin main
```

Or use the GitHub Actions workflow above for automatic syncing.

## Local Testing

To test locally without pushing to GitHub:

```bash
# Start a simple HTTP server
cd docs
python3 -m http.server 8000

# Open browser to http://localhost:8000
```

## References

- [GitHub Pages Documentation](https://pages.github.com/)
- [Plotly.js Documentation](https://plotly.com/javascript/)
- [HTML5 Fetch API](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API)
