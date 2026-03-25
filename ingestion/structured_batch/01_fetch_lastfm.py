# ingestion/structured_batch/01_fetch_lastfm.py
#
# This script:
# 1. Fetches Last.fm global chart tracks
# 2. Fetches Last.fm top tracks by selected tags
# 3. Fetches Last.fm top tracks by selected countries
# 4. Stores raw JSON responses locally
# 5. Builds a raw/seed CSV with provenance preserved
# 6. Builds a clean unique-track CSV for inspection
# 7. Builds a candidate CSV for downstream pipeline use
#
# Usage:
#   python3 ingestion/structured_batch/01_fetch_lastfm.py
#
# Optional:
#   TEST_MODE=true python3 ingestion/structured_batch/01_fetch_lastfm.py

import os
import re
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

# ============================================================
# Environment / config
# ============================================================

load_dotenv()

LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
if not LASTFM_API_KEY:
    raise ValueError("Missing LASTFM_API_KEY in .env")

API_URL = "http://ws.audioscrobbler.com/2.0/"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_ROOT = PROJECT_ROOT / "data" / "lastfm"
RAW_ROOT = OUTPUT_ROOT / "raw"
CURATED_ROOT = OUTPUT_ROOT / "curated"

RAW_ROOT.mkdir(parents=True, exist_ok=True)
CURATED_ROOT.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------
# Runtime mode
# ------------------------------------------------------------

TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

FULL_TAGS = [
    "pop", "rock", "jazz", "electronic", "classical", "hip-hop", "reggae",
    "indie", "metal", "blues", "folk", "soul", "dance", "ambient",
    "techno", "house", "punk", "latin", "rnb", "country"
]

FULL_COUNTRIES = [
    "spain", "united states", "united kingdom", "germany", "france",
    "italy", "japan", "south korea", "brazil", "mexico"
]

TEST_TAGS = ["pop", "rock", "hip-hop"]
TEST_COUNTRIES = ["spain", "united states"]

if TEST_MODE:
    TAGS = TEST_TAGS
    COUNTRIES = TEST_COUNTRIES
    PER_PAGE_LIMIT = 20
    CHART_PAGES = 2
    TAG_PAGES = 2
    GEO_PAGES = 2
    REQUEST_SLEEP_SECONDS = 0.10
else:
    TAGS = FULL_TAGS
    COUNTRIES = FULL_COUNTRIES
    PER_PAGE_LIMIT = 50
    CHART_PAGES = 20
    TAG_PAGES = 20
    GEO_PAGES = 20
    REQUEST_SLEEP_SECONDS = 0.25

RETRY_SLEEP_SECONDS = 2.0
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

# ============================================================
# Run metadata
# ============================================================

RUN_TS = datetime.now(timezone.utc)
RUN_ID = RUN_TS.strftime("%Y%m%dT%H%M%SZ")
RUN_DATE = RUN_TS.strftime("%Y-%m-%d")

RUN_RAW_ROOT = RAW_ROOT / f"run_id={RUN_ID}"
RUN_RAW_ROOT.mkdir(parents=True, exist_ok=True)

# ============================================================
# Helpers
# ============================================================

def normalize_text(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    text = str(text).strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text if text else None


def build_track_key(artist_name: Optional[str], track_name: Optional[str]) -> Optional[str]:
    artist_norm = normalize_text(artist_name)
    track_norm = normalize_text(track_name)
    if not artist_norm or not track_norm:
        return None
    return f"{artist_norm}|||{track_norm}"


def build_artist_key(artist_name: Optional[str]) -> Optional[str]:
    return normalize_text(artist_name)


def safe_int(value: Any) -> Optional[int]:
    if value in (None, "", "None"):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def save_json(payload: Dict[str, Any], endpoint_name: str, filename: str) -> Path:
    endpoint_dir = RUN_RAW_ROOT / endpoint_name
    endpoint_dir.mkdir(parents=True, exist_ok=True)
    out_path = endpoint_dir / filename
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return out_path


def call_lastfm(method: str, extra_params: Dict[str, Any]) -> Dict[str, Any]:
    params = {
        "method": method,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        **extra_params,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and "error" in data:
                raise RuntimeError(
                    f"Last.fm API error for method={method}: "
                    f"{data.get('error')} - {data.get('message')}"
                )

            return data

        except Exception as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Failed Last.fm call after {MAX_RETRIES} retries. "
                    f"method={method}, params={extra_params}, error={e}"
                ) from e
            print(f"[retry {attempt}/{MAX_RETRIES}] method={method} error={e}")
            time.sleep(RETRY_SLEEP_SECONDS)


def extract_image_url(image_list: Any) -> Optional[str]:
    if not isinstance(image_list, list):
        return None
    for img in reversed(image_list):
        if isinstance(img, dict) and img.get("#text"):
            return img["#text"]
    return None


def parse_track_item(track: Dict[str, Any], source_type: str, source_value: str, page: int) -> Dict[str, Any]:
    artist_field = track.get("artist")
    artist_name = None
    artist_mbid = None

    if isinstance(artist_field, dict):
        artist_name = artist_field.get("name")
        artist_mbid = artist_field.get("mbid")
    else:
        artist_name = artist_field

    track_name = track.get("name")

    streamable = track.get("streamable")
    if isinstance(streamable, dict):
        streamable = streamable.get("fulltrack") or streamable.get("#text")

    return {
        "run_id": RUN_ID,
        "run_date": RUN_DATE,
        "source_type": source_type,          # chart / tag / geo
        "source_value": source_value,        # global / pop / spain ...
        "source_page": page,
        "lastfm_track_name": track_name,
        "lastfm_track_mbid": track.get("mbid"),
        "lastfm_artist_name": artist_name,
        "lastfm_artist_mbid": artist_mbid,
        "lastfm_url": track.get("url"),
        "lastfm_duration": safe_int(track.get("duration")),
        "lastfm_listeners": safe_int(track.get("listeners")),
        "lastfm_playcount": safe_int(track.get("playcount")),
        "lastfm_streamable": streamable,
        "lastfm_rank": safe_int(
            track.get("@attr", {}).get("rank")
            if isinstance(track.get("@attr"), dict) else None
        ),
        "lastfm_image_url": extract_image_url(track.get("image")),
        "track_key": build_track_key(artist_name, track_name),
        "artist_key": build_artist_key(artist_name),
    }

# ============================================================
# Fetchers
# ============================================================

def fetch_chart_tracks(max_pages: int, limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        print(f"[chart.getTopTracks] page {page}")
        data = call_lastfm("chart.getTopTracks", {"page": page, "limit": limit})
        save_json(data, "chart_top_tracks", f"page_{page:03d}.json")

        tracks = data.get("tracks", {}).get("track", [])
        if not tracks:
            break

        for track in tracks:
            rows.append(parse_track_item(track, "chart", "global", page))

        time.sleep(REQUEST_SLEEP_SECONDS)

    return rows


def fetch_tag_tracks(tag: str, max_pages: int, limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        print(f"[tag.getTopTracks][tag={tag}] page {page}")
        data = call_lastfm("tag.getTopTracks", {"tag": tag, "page": page, "limit": limit})
        save_json(data, "tag_top_tracks", f"tag={tag}_page_{page:03d}.json")

        tracks = data.get("tracks", {}).get("track", [])
        if not tracks:
            break

        for track in tracks:
            rows.append(parse_track_item(track, "tag", tag, page))

        time.sleep(REQUEST_SLEEP_SECONDS)

    return rows


def fetch_geo_tracks(country: str, max_pages: int, limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        print(f"[geo.getTopTracks][country={country}] page {page}")
        data = call_lastfm("geo.getTopTracks", {"country": country, "page": page, "limit": limit})
        save_json(data, "geo_top_tracks", f"country={safe_filename(country)}_page_{page:03d}.json")

        tracks = data.get("tracks", {}).get("track", [])
        if not tracks:
            break

        for track in tracks:
            rows.append(parse_track_item(track, "geo", country, page))

        time.sleep(REQUEST_SLEEP_SECONDS)

    return rows


def safe_filename(text: str, max_len: int = 120) -> str:
    text = re.sub(r"[^a-zA-Z0-9._-]+", "_", text)
    return text[:max_len]

# ============================================================
# Builders
# ============================================================

def build_candidate_tracks(seed_df: pd.DataFrame) -> pd.DataFrame:
    if seed_df.empty:
        return seed_df.copy()

    candidate_df = seed_df.dropna(subset=["track_key"]).copy()

    grouped = (
        candidate_df.groupby("track_key", dropna=False)
        .agg({
            "lastfm_artist_name": "first",
            "lastfm_track_name": "first",
            "lastfm_artist_mbid": "first",
            "lastfm_track_mbid": "first",
            "lastfm_url": "first",
            "lastfm_duration": "max",
            "artist_key": "first",
            "lastfm_playcount": "max",
            "lastfm_listeners": "max",
            "lastfm_rank": "min",
            "lastfm_image_url": "first",
            "source_page": "min",
        })
        .reset_index()
    )

    source_types = (
        candidate_df.groupby("track_key")["source_type"]
        .apply(lambda s: sorted(set(x for x in s if pd.notna(x))))
        .to_dict()
    )

    source_values = (
        candidate_df.groupby("track_key")["source_value"]
        .apply(lambda s: sorted(set(x for x in s if pd.notna(x))))
        .to_dict()
    )

    occurrences = candidate_df.groupby("track_key").size().to_dict()

    grouped["seed_source_types"] = grouped["track_key"].map(
        lambda x: json.dumps(source_types.get(x, []), ensure_ascii=False)
    )
    grouped["seed_source_values"] = grouped["track_key"].map(
        lambda x: json.dumps(source_values.get(x, []), ensure_ascii=False)
    )
    grouped["seed_occurrences"] = grouped["track_key"].map(lambda x: occurrences.get(x, 0))

    return grouped


def build_pretty_unique_tracks(candidate_df: pd.DataFrame) -> pd.DataFrame:
    pretty_cols = [
        "lastfm_track_name",
        "lastfm_track_mbid",
        "lastfm_artist_name",
        "lastfm_artist_mbid",
        "lastfm_url",
        "lastfm_duration",
        "lastfm_listeners",
        "lastfm_playcount",
        "lastfm_rank",
        "lastfm_image_url",
        "track_key",
        "artist_key",
        "seed_occurrences",
        "seed_source_types",
        "seed_source_values",
    ]
    existing_cols = [c for c in pretty_cols if c in candidate_df.columns]
    return candidate_df[existing_cols].copy()

# ============================================================
# Main
# ============================================================

def main() -> None:
    mode_label = "TEST_MODE" if TEST_MODE else "FULL_MODE"
    print(f"Starting Last.fm ingestion run: {RUN_ID} [{mode_label}]")

    all_rows: List[Dict[str, Any]] = []

    # 1) Global chart
    all_rows.extend(fetch_chart_tracks(max_pages=CHART_PAGES, limit=PER_PAGE_LIMIT))

    # 2) Tags
    for tag in TAGS:
        try:
            all_rows.extend(fetch_tag_tracks(tag, max_pages=TAG_PAGES, limit=PER_PAGE_LIMIT))
        except Exception as e:
            print(f"Skipping tag={tag} due to error: {e}")
            time.sleep(RETRY_SLEEP_SECONDS)

    # 3) Countries
    for country in COUNTRIES:
        try:
            all_rows.extend(fetch_geo_tracks(country, max_pages=GEO_PAGES, limit=PER_PAGE_LIMIT))
        except Exception as e:
            print(f"Skipping country={country} due to error: {e}")
            time.sleep(RETRY_SLEEP_SECONDS)

    seed_df = pd.DataFrame(all_rows)

    seed_path = CURATED_ROOT / f"seed_tracks_run_{RUN_ID}.csv"
    seed_df.to_csv(seed_path, index=False)

    print(f"Seed rows collected: {len(seed_df)}")
    print(f"Saved seed track table to: {seed_path}")

    candidate_df = build_candidate_tracks(seed_df)

    candidate_path = CURATED_ROOT / f"candidate_tracks_run_{RUN_ID}.csv"
    candidate_df.to_csv(candidate_path, index=False)

    print(f"Candidate artist-track pairs: {len(candidate_df)}")
    print(f"Saved candidate tracks to: {candidate_path}")

    pretty_df = build_pretty_unique_tracks(candidate_df)
    pretty_path = CURATED_ROOT / f"lastfm_tracks_unique_run_{RUN_ID}.csv"
    pretty_df.to_csv(pretty_path, index=False)

    print(f"Saved human-readable unique-track dataset to: {pretty_path}")

    non_null_playcount = int(candidate_df["lastfm_playcount"].notna().sum()) if "lastfm_playcount" in candidate_df.columns else 0
    non_null_listeners = int(candidate_df["lastfm_listeners"].notna().sum()) if "lastfm_listeners" in candidate_df.columns else 0
    total_candidates = len(candidate_df)

    print(
        f"Coverage: playcount={non_null_playcount}/{total_candidates}, "
        f"listeners={non_null_listeners}/{total_candidates}"
    )

    print("\nLast.fm ingestion finished successfully.")


if __name__ == "__main__":
    main()