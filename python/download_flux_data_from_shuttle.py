# Fluxnet shuttle git -> https://github.com/fluxnet/shuttle/tree/main
# python download_fluxnet.py fluxnet_shuttle_snapshot_20260306.csv --hub TERN
# python download_fluxnet.py fluxnet_shuttle_snapshot_20260306.csv --site AU-Cum


import pandas as pd
import requests
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("snapshot_file")
parser.add_argument("--hub", help="Filter by data hub (e.g. TERN, ICOS, AmeriFlux)")
parser.add_argument("--site", help="Filter by site ID")
parser.add_argument("--outdir", default="fluxnet_downloads")

args = parser.parse_args()

df = pd.read_csv(args.snapshot_file)

# apply filters
if args.hub:
    df = df[df["data_hub"] == args.hub]

if args.site:
    df = df[df["site_id"] == args.site]

outdir = Path(args.outdir)
outdir.mkdir(exist_ok=True)

print(f"Downloading {len(df)} datasets")

for _, row in df.iterrows():
    url = row["download_link"]
    fname = row["fluxnet_product_name"]
    outfile = outdir / fname

    if outfile.exists():
        print(f"Skipping existing {fname}")
        continue

    print(f"Downloading {fname}")

    r = requests.get(url, stream=True)
    r.raise_for_status()

    with open(outfile, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
