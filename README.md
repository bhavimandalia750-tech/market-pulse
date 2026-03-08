# 📊 Market Pulse — NSE Live Intelligence Dashboard

A free, self-hosted NSE market intelligence website running entirely on GitHub (no server costs!).

## How it works

```
GitHub Actions (free)          GitHub Pages (free)
┌─────────────────────┐        ┌─────────────────────┐
│ Runs Python script  │──────▶ │ Hosts your website  │
│ every 5 minutes     │        │ for free forever    │
│ Fetches NSE data    │        │                     │
│ Saves JSON files    │        │ Shows live charts,  │
└─────────────────────┘        │ OI table, FII/DII   │
                               └─────────────────────┘
```

## Setup (10 minutes)

### Step 1 — Create GitHub repo
1. Go to github.com → click **New repository**
2. Name: `market-pulse`  
3. Set to **Public**
4. ✅ Add README
5. Click **Create repository**

### Step 2 — Upload these files
Upload ALL files maintaining this structure:
```
market-pulse/
├── index.html
├── fetch_data.py
├── .github/
│   └── workflows/
│       └── fetch-data.yml
└── data/
    ├── indices.json
    ├── oc_nifty.json
    ├── oc_banknifty.json
    ├── oc_finnifty.json
    └── fii_dii.json
```

**Important:** For `.github/workflows/fetch-data.yml`, you must create the folders manually on GitHub:
- Click **Add file → Create new file**
- Type: `.github/workflows/fetch-data.yml` (GitHub creates the folders automatically)
- Paste the content from the file

### Step 3 — Enable GitHub Pages
1. Go to your repo → **Settings** → **Pages** (left sidebar)
2. Source: **Deploy from a branch**
3. Branch: **main** / root `/`
4. Click **Save**
5. Your site will be live at: `https://YOUR-USERNAME.github.io/market-pulse`

### Step 4 — Enable GitHub Actions to write data
1. Go to repo → **Settings** → **Actions** → **General**
2. Scroll to **Workflow permissions**
3. Select: **Read and write permissions**
4. Click **Save**

### Step 5 — Run first data fetch
1. Go to **Actions** tab in your repo
2. Click **Fetch NSE Market Data**
3. Click **Run workflow** → **Run workflow**
4. Watch it run (takes ~30 seconds)
5. After it completes, your site shows real NSE data!

## After setup

- Data auto-refreshes every **5 minutes** during market hours (Mon–Fri, 9:00 AM – 3:30 PM IST)
- Website auto-refreshes every **5 minutes**
- You can trigger a manual fetch anytime from the Actions tab

## What you get

✅ **Option Chain** — Full OI table for NIFTY, BANKNIFTY, FINNIFTY  
✅ **OI Charts** — Bar charts, buildup/unwind, trend  
✅ **PCR Gauge** — Put-Call Ratio with visual indicator  
✅ **Max Pain** — Calculated strike where option writers profit most  
✅ **IV Smile** — Implied volatility curve across strikes  
✅ **Support/Resistance** — Top CE and PE OI strikes  
✅ **FII/DII** — 10-day flow charts and table  
✅ **Index Prices** — NIFTY, BANKNIFTY, FINNIFTY, VIX  

## Troubleshooting

**Actions workflow fails?**  
→ NSE may block GitHub's IP temporarily. Try running again in 5 minutes.

**Data shows zeros?**  
→ Run the workflow manually from Actions tab.

**GitHub Pages shows 404?**  
→ Wait 2–3 minutes after enabling Pages for it to deploy.
