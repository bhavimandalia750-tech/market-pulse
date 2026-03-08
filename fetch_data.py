"""
NSE Data Fetcher - runs on GitHub Actions servers (Indian IPs bypass Cloudflare)
Fetches: Option Chain OI, Index prices, FII/DII data
Saves as JSON files read by the website
"""

import requests
import json
import time
import os
import math
from datetime import datetime, timezone

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
})

def prime_session():
    """Visit NSE pages in order to get valid session cookies"""
    urls = [
        'https://www.nseindia.com',
        'https://www.nseindia.com/market-data/live-equity-market',
        'https://www.nseindia.com/option-chain',
    ]
    for url in urls:
        try:
            SESSION.get(url, timeout=15)
            time.sleep(0.8)
        except Exception as e:
            print(f"Prime warning: {url} -> {e}")

def nse_get(path):
    """Call NSE API with proper headers"""
    SESSION.headers.update({
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.nseindia.com/option-chain',
        'X-Requested-With': 'XMLHttpRequest',
    })
    r = SESSION.get(f'https://www.nseindia.com/api{path}', timeout=20)
    r.raise_for_status()
    return r.json()

def save_json(filename, data):
    os.makedirs('data', exist_ok=True)
    with open(f'data/{filename}', 'w') as f:
        json.dump(data, f)
    print(f"✅ Saved data/{filename}")

def calc_max_pain(strikes):
    """Calculate max pain strike"""
    min_loss = float('inf')
    max_pain = 0
    for target in strikes:
        loss = 0
        for s in strikes:
            tp = target['strike']
            sp = s['strike']
            if tp > sp:
                loss += (tp - sp) * s['ceOI']
            elif tp < sp:
                loss += (sp - tp) * s['peOI']
        if loss < min_loss:
            min_loss = loss
            max_pain = target['strike']
    return max_pain

def fetch_option_chain(symbol='NIFTY'):
    try:
        data = nse_get(f'/option-chain-indices?symbol={symbol}')
        records = data['records']
        expiries = records['expiryDates']
        spot = float(records['underlyingValue'])
        sel_expiry = expiries[0]

        strikes = []
        total_ce_oi = 0
        total_pe_oi = 0

        for row in records['data']:
            if row.get('expiryDate') != sel_expiry:
                continue
            strike = float(row['strikePrice'])
            ce = row.get('CE', {})
            pe = row.get('PE', {})
            ce_oi = float(ce.get('openInterest', 0))
            pe_oi = float(pe.get('openInterest', 0))
            ce_chg = float(ce.get('changeinOpenInterest', 0))
            pe_chg = float(pe.get('changeinOpenInterest', 0))
            total_ce_oi += ce_oi
            total_pe_oi += pe_oi
            strikes.append({
                'strike': strike,
                'ceOI': ce_oi,
                'ceChgOI': ce_chg,
                'ceVol': float(ce.get('totalTradedVolume', 0)),
                'ceIV': float(ce.get('impliedVolatility', 0)),
                'ceLTP': float(ce.get('lastPrice', 0)),
                'peOI': pe_oi,
                'peChgOI': pe_chg,
                'peVol': float(pe.get('totalTradedVolume', 0)),
                'peIV': float(pe.get('impliedVolatility', 0)),
                'peLTP': float(pe.get('lastPrice', 0)),
            })

        strikes.sort(key=lambda x: x['strike'])
        max_pain = calc_max_pain(strikes)
        pcr = round(total_pe_oi / total_ce_oi, 3) if total_ce_oi > 0 else 1.0

        # Find ATM IV
        atm = min(strikes, key=lambda s: abs(s['strike'] - spot), default={})
        atm_iv = round((atm.get('ceIV', 0) + atm.get('peIV', 0)) / 2, 2)

        result = {
            'symbol': symbol,
            'spot': spot,
            'expiry': sel_expiry,
            'expiries': expiries[:8],
            'pcr': pcr,
            'maxPain': max_pain,
            'atmIV': atm_iv,
            'totalCeOI': total_ce_oi,
            'totalPeOI': total_pe_oi,
            'strikes': strikes,
            'updatedAt': datetime.now(timezone.utc).isoformat(),
        }
        save_json(f'oc_{symbol.lower()}.json', result)
        return True
    except Exception as e:
        print(f"❌ Option chain {symbol}: {e}")
        return False

def fetch_indices():
    try:
        data = nse_get('/allIndices')
        indices = data['data']
        result = {}
        keys = {
            'NIFTY 50': 'nifty',
            'NIFTY BANK': 'banknifty',
            'NIFTY FIN SERVICE': 'finnifty',
            'INDIA VIX': 'vix',
            'NIFTY MIDCAP SELECT': 'midcap',
        }
        for idx in indices:
            name = idx.get('index', '')
            key = keys.get(name)
            if key:
                result[key] = {
                    'name': name,
                    'last': float(str(idx.get('last', 0)).replace(',', '')),
                    'change': float(str(idx.get('change', 0)).replace(',', '')),
                    'pChange': float(str(idx.get('percentChange', 0)).replace(',', '')),
                }
        result['updatedAt'] = datetime.now(timezone.utc).isoformat()
        save_json('indices.json', result)
        return True
    except Exception as e:
        print(f"❌ Indices: {e}")
        return False

def fetch_fii_dii():
    try:
        data = nse_get('/fiidiiTradeReact')
        rows = []
        for row in data[:10]:
            rows.append({
                'date': row.get('date', ''),
                'fiiBuy': float(str(row.get('buySell', {}).get('fii', {}).get('buy', 0)).replace(',', '')),
                'fiiSell': float(str(row.get('buySell', {}).get('fii', {}).get('sell', 0)).replace(',', '')),
                'fiiNet': float(str(row.get('buySell', {}).get('fii', {}).get('net', 0)).replace(',', '')),
                'diiBuy': float(str(row.get('buySell', {}).get('dii', {}).get('buy', 0)).replace(',', '')),
                'diiSell': float(str(row.get('buySell', {}).get('dii', {}).get('sell', 0)).replace(',', '')),
                'diiNet': float(str(row.get('buySell', {}).get('dii', {}).get('net', 0)).replace(',', '')),
            })
        save_json('fii_dii.json', {'data': rows, 'updatedAt': datetime.now(timezone.utc).isoformat()})
        return True
    except Exception as e:
        print(f"❌ FII/DII: {e}")
        # Try alternate endpoint format
        try:
            data = nse_get('/fiidiiTradeReact')
            # Try flat format
            rows = []
            for row in data[:10]:
                buy_val = 0
                sell_val = 0
                net_val = 0
                for k, v in row.items():
                    if 'buy' in k.lower(): buy_val = float(str(v).replace(',','').replace('-','0') or 0)
                    if 'sell' in k.lower(): sell_val = float(str(v).replace(',','').replace('-','0') or 0)
                    if 'net' in k.lower(): net_val = float(str(v).replace(',','').replace('-','0') or 0)
                rows.append({'date': row.get('date',''), 'fiiNet': net_val, 'diiNet': 0})
            save_json('fii_dii.json', {'data': rows, 'updatedAt': datetime.now(timezone.utc).isoformat()})
        except:
            pass
        return False

def fetch_market_status():
    try:
        data = nse_get('/market-status')
        save_json('market_status.json', {**data, 'updatedAt': datetime.now(timezone.utc).isoformat()})
        return True
    except Exception as e:
        print(f"❌ Market status: {e}")
        return False

if __name__ == '__main__':
    print("🚀 Starting NSE data fetch...")
    prime_session()
    print("✅ Session primed")

    fetch_market_status()
    time.sleep(1)
    fetch_indices()
    time.sleep(1)
    fetch_option_chain('NIFTY')
    time.sleep(3)
    fetch_option_chain('BANKNIFTY')
    time.sleep(3)
    fetch_option_chain('FINNIFTY')
    time.sleep(1)
    fetch_fii_dii()

    print("✅ All done!")
