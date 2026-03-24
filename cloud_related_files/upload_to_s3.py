"""
upload_to_s3.py — Local ingestion script
==========================================
Fetches OSM road network data live via osmnx, and converts pre-downloaded
Waka Kotahi CSV files to Parquet before uploading everything to the S3
raw landing zone.

Waka Kotahi data is not fetched live because the ArcGIS Hub URLs change
periodically. Download the files manually (see DATA_SOURCES.md) and place
them in the downloads/ directory of the project before running this script.

Usage:
  pip install osmnx boto3 pandas pyarrow
  export AWS_ACCESS_KEY_ID=...
  export AWS_SECRET_ACCESS_KEY=...
  export S3_BUCKET=nz-road-pipeline-raw
  python upload_to_s3.py --date 2026-03-20

Expected local files for Waka Kotahi (relative to project root):
  downloads/State_highway_traffic_monitoring_sites.csv
  downloads/TMS_traffic_counts/TMS_traffic_counts.csv
"""

import os
import io
import argparse
import logging
from datetime import date
from pathlib import Path

import boto3
import osmnx as ox
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

S3_BUCKET    = os.environ.get("S3_BUCKET", "nz-road-pipeline-raw")
s3           = boto3.client("s3")
PROJECT_ROOT = Path(__file__).parent.parent   # nzta_etl_project/
DOWNLOADS    = PROJECT_ROOT / "downloads"

# Expected local CSV paths
COUNTS_CSV = DOWNLOADS / "TMS_traffic_counts" / "TMS_traffic_counts.csv"
SITES_CSV  = DOWNLOADS / "State_highway_traffic_monitoring_sites.csv"


def upload_parquet(df: pd.DataFrame, s3_key: str) -> None:
    """Serialise a DataFrame to Parquet in memory and upload to S3."""
    buffer = io.BytesIO()
    table  = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)
    buffer.seek(0)
    s3.upload_fileobj(buffer, S3_BUCKET, s3_key)
    logger.info(f"Uploaded {len(df):,} rows -> s3://{S3_BUCKET}/{s3_key}")



def sanitise_mixed_columns(gdf) -> None:
    """
    Stringify any non-geometry object column containing mixed types
    (lists, ints, strings mixed together). Geometry column is always
    preserved as native geopandas geometry.
    OSM commonly returns osmid, ref, lanes, maxspeed etc. with mixed types.
    """
    import geopandas as gpd
    for col in gdf.columns:
        if col == "geometry":
            continue
        if gdf[col].dtype == object:
            non_null = gdf[col].dropna()
            types = set(type(x).__name__ for x in non_null)
            has_list = non_null.apply(lambda x: isinstance(x, list)).any()
            if has_list or len(types) > 1:
                gdf[col] = gdf[col].apply(
                    lambda v: str(v) if v is not None else None
                )
    return gdf


def ingest_osm(run_date: str) -> None:
    """
    Download Christchurch road network live from OSM and upload to S3.
    Saves as GeoParquet (native geometry) matching the local bronze notebook
    approach — no manual WKB conversion.
    """
    import geopandas as gpd

    logger.info("Fetching OSM road network for Christchurch...")
    G = ox.graph_from_place("Christchurch, New Zealand", network_type="drive")
    nodes, edges = ox.graph_to_gdfs(G)

    # Edges = road segments — sanitise mixed columns, keep geometry native
    edges_gdf = edges.reset_index()
    edges_gdf = sanitise_mixed_columns(edges_gdf)

    buffer = io.BytesIO()
    edges_gdf.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.upload_fileobj(buffer, S3_BUCKET,
                      f"osm/christchurch/{run_date}/road_segments.parquet")
    logger.info(f"Uploaded {len(edges_gdf):,} rows -> "
                f"s3://{S3_BUCKET}/osm/christchurch/{run_date}/road_segments.parquet")

    # Nodes
    nodes_gdf = nodes.reset_index()
    nodes_gdf = sanitise_mixed_columns(nodes_gdf)

    buffer = io.BytesIO()
    nodes_gdf.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.upload_fileobj(buffer, S3_BUCKET,
                      f"osm/christchurch/{run_date}/road_nodes.parquet")
    logger.info(f"Uploaded {len(nodes_gdf):,} rows -> "
                f"s3://{S3_BUCKET}/osm/christchurch/{run_date}/road_nodes.parquet")


def csv_to_parquet_buffer(csv_path: Path, chunksize: int = 100_000) -> io.BytesIO:
    """
    Read a CSV in chunks and write to a single in-memory Parquet buffer.
    Avoids loading the entire file into memory at once.
    """
    writer = None
    buffer = io.BytesIO()
    total  = 0

    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(buffer, table.schema)
        writer.write_table(table)
        total += len(chunk)
        logger.info(f"  ...processed {total:,} rows")

    if writer:
        writer.close()

    buffer.seek(0)
    return buffer, total


def ingest_waka_kotahi(run_date: str) -> None:
    """
    Read Waka Kotahi CSVs from local downloads/ in chunks, convert to
    Parquet, and upload to S3. See DATA_SOURCES.md for where to get these files.
    """
    for csv_path in [COUNTS_CSV, SITES_CSV]:
        if not csv_path.exists():
            raise FileNotFoundError(
                f"Expected local file not found: {csv_path}\n"
                f"See DATA_SOURCES.md for download instructions."
            )

    logger.info(f"Reading traffic counts from {COUNTS_CSV} (chunked)...")
    buffer, total = csv_to_parquet_buffer(COUNTS_CSV)
    s3.upload_fileobj(buffer, S3_BUCKET,
                      f"waka_kotahi/{run_date}/tms_daily_traffic_counts.parquet")
    logger.info(f"Uploaded {total:,} rows -> s3://{S3_BUCKET}/waka_kotahi/{run_date}/tms_daily_traffic_counts.parquet")

    logger.info(f"Reading monitoring sites from {SITES_CSV} (chunked)...")
    buffer, total = csv_to_parquet_buffer(SITES_CSV)
    s3.upload_fileobj(buffer, S3_BUCKET,
                      f"waka_kotahi/{run_date}/tms_monitoring_sites.parquet")
    logger.info(f"Uploaded {total:,} rows -> s3://{S3_BUCKET}/waka_kotahi/{run_date}/tms_monitoring_sites.parquet")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw data to S3")
    parser.add_argument(
        "--date",
        default=str(date.today()),
        help="Run date partition (YYYY-MM-DD), default today",
    )
    parser.add_argument(
        "--skip-osm",
        action="store_true",
        help="Skip OSM download (useful if only Waka Kotahi files changed)",
    )
    parser.add_argument(
        "--skip-waka-kotahi",
        action="store_true",
        help="Skip Waka Kotahi upload (useful if only OSM data changed)",
    )
    args = parser.parse_args()
    run_date = args.date

    logger.info(f"Starting ingestion run for {run_date} -> s3://{S3_BUCKET}")

    if not args.skip_osm:
        ingest_osm(run_date)
    else:
        logger.info("Skipping OSM ingestion (--skip-osm set)")

    if not args.skip_waka_kotahi:
        ingest_waka_kotahi(run_date)
    else:
        logger.info("Skipping Waka Kotahi ingestion (--skip-waka-kotahi set)")

    logger.info("Ingestion complete. Lambda will copy new S3 objects to ADLS bronze.")


if __name__ == "__main__":
    main()