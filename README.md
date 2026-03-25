# Road Network ETL Pipeline

A multi-cloud data engineering pipeline that ingests public road and traffic data, transforms it through the **Medallion architecture** (bronze → silver → gold), and serves a star schema ready for analysis.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Ingestion (AWS)                                                     │
│                                                                      │
│  upload_to_s3.py                                                     │
│  ├── OpenStreetMap (live via osmnx)      ──►  S3 raw landing zone   │
│  └── Waka Kotahi TMS (manual download)  ──►  nz-road-pipeline-raw  │
│                                                    │                 │
│                                          EventBridge + Lambda        │
└─────────────────────────────────────────────────────────────────────┘
                                                     │
                                                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Storage & Transformation (Azure)                                    │
│                                                                      │
│  ADLS Gen2 — medallion container                                     │
│  ├── bronze/   raw parquet (date-partitioned, append-only)          │
│  ├── silver/   cleaned Delta tables                                  │
│  └── gold/     star schema Delta tables                              │
│                                                                      │
│  Databricks Workflow: silver_layer → gold_layer (auto-triggered)    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Data sources

| Source | Dataset | Format | Refresh cadence |
|--------|---------|--------|-----------------|
| [OpenStreetMap](https://www.openstreetmap.org) via `osmnx` | Christchurch drive network | GeoParquet | Fetched live each pipeline run |
| [Waka Kotahi NZ Transport Agency](https://opendata-nzta.opendata.arcgis.com) | TMS daily traffic counts | CSV → Parquet | Manual download — see `DATA_SOURCES.md` |
| [Waka Kotahi NZ Transport Agency](https://opendata-nzta.opendata.arcgis.com) | State highway monitoring sites | CSV → Parquet | Manual download — see `DATA_SOURCES.md` |

> **Note on Waka Kotahi data:** The ArcGIS Hub download URLs change periodically
> when Waka Kotahi updates their open data portal. Rather than hardcoding a URL
> that may break, the pipeline treats the Waka Kotahi CSVs as manually-managed
> inputs. Download instructions and the exact datasets used are documented in
> [`DATA_SOURCES.md`](DATA_SOURCES.md).

---

## Medallion layers

### Bronze
Raw data landed as-is — no transformations, no cleaning. Date-partitioned so
every pipeline run is preserved and reprocessable.

- `bronze/osm/christchurch/<date>/road_segments.parquet`
- `bronze/osm/christchurch/<date>/road_nodes.parquet`
- `bronze/waka_kotahi/<date>/tms_daily_traffic_counts.parquet`
- `bronze/waka_kotahi/<date>/tms_monitoring_sites.parquet`

### Silver
Cleaned and standardised Delta tables. Key transformations:

- OSM road segments: `maxspeed` parsed to integer km/h, highway type classified
  into road classes (motorway, arterial, local etc.), geometry reprojected from
  WGS84 to NZTM2000 (EPSG:2193)
- TMS counts: filtered to Canterbury region, dates parsed, aggregated to
  site/date/vehicle class grain
- TMS sites: filtered to active sites with valid coordinates, joined to active
  count sites

### Gold
Star schema optimised for analytical queries.

```
fact_traffic_counts
├── date_id      → dim_time         (date, year, month, day_of_week, is_weekend)
├── site_id      → dim_location     (site, region, easting, northing, segment_id)
├── segment_id   → dim_road_segment (name, road_class, maxspeed_kmh, length)
├── vehicle_class
└── daily_count
```

---

## Sample analyses

### Top 10 busiest road segments
![Busiest roads](sample_analyses_images/01_busiest_roads.png)

### Monthly traffic trend — Canterbury
![Monthly trend](sample_analyses_images/02_monthly_trend.png)

### Heavy vehicle share by road class
![Heavy vehicles](sample_analyses_images/03_heavy_vehicle_pct.png)

### Average traffic by day of week
![Day of week](sample_analyses_images/04_dow_traffic.png)

### Top 10 monitoring sites by AADT
![Top sites](sample_analyses_images/05_top_sites_aadt.png)

---

## Project structure

```
nzta_etl_project/
├── cloud_related_files/
│   ├── upload_to_s3.py              # Ingestion script — OSM (live) + Waka Kotahi (local)
│   ├── s3_to_adls_lambda.py         # AWS Lambda — copies S3 objects to ADLS bronze,
│   │                                #   then triggers the Databricks workflow automatically
│   ├── s3_trigger_setup.tf          # Terraform — S3 bucket + EventBridge + Lambda + IAM
│   ├── build_lambda.sh              # Packages Lambda for deployment
│   └── README_aws_ingestion.md      # AWS setup instructions
├── notebooks/
│   ├── silver_layer_databricks.ipynb
│   ├── gold_layer_databricks.ipynb
│   └── gold_analysis.py             # SQL analysis queries + charts
├── downloads/                       # Raw source files (not committed — see .gitignore)
├── DATA_SOURCES.md                  # Data provenance and refresh instructions
└── README.md
```

---

## Running the pipeline

### Prerequisites
- AWS CLI configured (`aws configure`)
- Databricks secret scope `adls` containing the ADLS storage key
- Terraform installed
- Python environment with `osmnx`, `boto3`, `geopandas`, `pandas`, `pyarrow`
- Waka Kotahi CSVs downloaded into `downloads/` (see `DATA_SOURCES.md`)

### First-time setup
```bash
cd cloud_related_files
./build_lambda.sh
terraform init
terraform apply -var="azure_sas_token=<your-sas-token>"
```

### Running the pipeline

```bash
python upload_to_s3.py
```

That's it — one command sets the entire pipeline in motion:

1. Fetches the latest Christchurch road network live from OpenStreetMap
2. Reads the Waka Kotahi CSVs from `downloads/`
3. Uploads everything to S3 under today's date partition
4. EventBridge detects each new S3 object and invokes the Lambda
5. Lambda copies each file to ADLS `bronze/`
6. When the last file lands, Lambda automatically triggers the Databricks
   workflow (`road_pipeline_silver_gold`) with today's date as the
   `bronze_date` parameter
7. Databricks runs silver → gold in sequence, updating all Delta tables

> **Refreshing Waka Kotahi data:** When Waka Kotahi publishes a new export,
> download it, replace the files in `downloads/`, and re-run
> `python upload_to_s3.py`. The OSM road network is always fetched live so
> no action is needed for that source. See `DATA_SOURCES.md` for download
> instructions.

### Skipping individual sources
```bash
python upload_to_s3.py --skip-osm            # Waka Kotahi only
python upload_to_s3.py --skip-waka-kotahi    # OSM only
python upload_to_s3.py --date 2026-03-20     # Reprocess a specific date partition
```

> **Note on `--skip-waka-kotahi`:** If Waka Kotahi files are skipped, the
> Lambda will not find `tms_daily_traffic_counts.parquet` and will not trigger
> the Databricks workflow automatically. In that case, trigger the job manually
> in Databricks with the appropriate `bronze_date`.

---

## Security

- ADLS storage key stored in a **Databricks Secret Scope** — never in notebook code
- Azure SAS token passed to Lambda via environment variable set in Terraform — never committed to Git
- Databricks personal access token stored as a Lambda environment variable — never committed to Git
- AWS credentials managed via `~/.aws/credentials` — never hardcoded
- `downloads/` directory excluded from Git (see `.gitignore`)

---

## Known limitations & future work

- **Silver/gold overwrite on each run** — currently uses `mode="overwrite"` so
  only the latest run's data is retained in silver and gold. A production
  pipeline would use Delta Lake `MERGE INTO` (upsert) to accumulate history
  across runs.
- **Waka Kotahi data is a static snapshot** — the 2018–2022 CSV is a
  point-in-time export. The pipeline is designed to be re-run when new exports
  are published but does not poll for updates automatically.
- **Canterbury-only silver filter** — the silver layer filters TMS counts to
  Canterbury region to match the Christchurch OSM road network. This could be
  parameterised to support other regions.

---

## Tech stack

| Layer | Technology |
|-------|-----------|
| Raw ingestion | Python (`osmnx`, `boto3`, `geopandas`) |
| Raw storage | AWS S3 |
| Event trigger | AWS EventBridge + Lambda |
| Cloud transfer | Azure Blob REST API (via `requests`) |
| Data lake | Azure Data Lake Storage Gen2 |
| Transformation | Databricks (PySpark + pandas UDFs) |
| Table format | Delta Lake |
| Orchestration | Databricks Workflows (Jobs) — auto-triggered by Lambda |
| Infrastructure | Terraform |
| Spatial ops | geopandas, shapely, pyproj (NZTM2000) |