# ingestion/structured_batch/01_fetch_lastfm.py
#
# This script:
# 1. Fetches Last.fm global chart tracks
# 2. Fetches Last.fm top tracks by selected tags
# 3. Stores raw JSON responses locally
# 4. Builds a seed track table (with provenance preserved)
# 5. Builds a candidate track table (deduplicated by artist-track)
#
# Later, in Airflow we use to schedule API calls:
#   python3 ingestion/structured_batch/01_fetch_lastfm.py

import os
import re
import json
import time
from datetime import datetime, timezone
from pathlib import Path
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

# Base output folders (local for now)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_ROOT = PROJECT_ROOT / "data" / "lastfm"

RAW_ROOT = OUTPUT_ROOT / "raw"
CURATED_ROOT = OUTPUT_ROOT / "curated"

RAW_ROOT.mkdir(parents=True, exist_ok=True)
CURATED_ROOT.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------
# Sampling strategy
# ------------------------------------------------------------
# Bigger dataset:
# - more chart pages
# - more tag pages
# Still much faster than per-track enrichment.

TAGS = [
    "pop", "rock", "jazz", "electronic", "classical", "hip-hop", "reggae",
    "indie", "metal", "blues", "folk", "soul", "dance", "ambient",
    "techno", "house", "punk", "latin", "rnb", "country"
]

PER_PAGE_LIMIT = 50
CHART_PAGES = 20
TAG_PAGES = 20

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
    """Normalize artist / track names for matching and deduplication."""
    if text is None:
        return None
    text = str(text).strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text if text else None


def build_track_key(artist_name: Optional[str], track_name: Optional[str]) -> Optional[str]:
    """Canonical natural key for a track."""
    artist_norm = normalize_text(artist_name)
    track_norm = normalize_text(track_name)
    if not artist_norm or not track_norm:
        return None
    return f"{artist_norm}|||{track_norm}"


def build_artist_key(artist_name: Optional[str]) -> Optional[str]:
    artist_norm = normalize_text(artist_name)
    return artist_norm if artist_norm else None


def safe_int(value: Any) -> Optional[int]:
    if value in (None, "", "None"):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def save_json(payload: Dict[str, Any], endpoint_name: str, filename: str) -> Path:
    """
    Save raw JSON by endpoint into the run folder.
    Example:
      data/lastfm/raw/run_id=.../chart_top_tracks/page_001.json
    """
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


# ============================================================
# Parsing seed tracks (charts / tags)
# ============================================================

def parse_seed_track(
    track: Dict[str, Any],
    source_type: str,
    source_value: str,
    page: int,
) -> Dict[str, Any]:
    """
    Normalize track rows coming from:
      - chart.getTopTracks
      - tag.getTopTracks
    """
    artist_field = track.get("artist")
    artist_name = None
    artist_mbid = None

    if isinstance(artist_field, dict):
        artist_name = artist_field.get("name")
        artist_mbid = artist_field.get("mbid")
    else:
        artist_name = artist_field

    track_name = track.get("name")
    track_key = build_track_key(artist_name, track_name)

    return {
        "run_id": RUN_ID,
        "run_date": RUN_DATE,
        "source_type": source_type,      # chart / tag
        "source_value": source_value,    # global / rock / pop ...
        "source_page": page,
        "lastfm_track_name": track_name,
        "lastfm_track_mbid": track.get("mbid"),
        "lastfm_artist_name": artist_name,
        "lastfm_artist_mbid": artist_mbid,
        "lastfm_url": track.get("url"),
        "lastfm_duration": safe_int(track.get("duration")),
        "lastfm_listeners": safe_int(track.get("listeners")),
        "lastfm_playcount": safe_int(track.get("playcount")),
        "lastfm_rank": safe_int(
            track.get("@attr", {}).get("rank")
            if isinstance(track.get("@attr"), dict) else None
        ),
        "lastfm_image_url": extract_image_url(track.get("image")),
        "track_key": track_key,
        "artist_key": build_artist_key(artist_name),
    }


def fetch_chart_tracks(max_pages: int = CHART_PAGES, limit: int = PER_PAGE_LIMIT) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        print(f"[chart.getTopTracks] page {page}")

        data = call_lastfm("chart.getTopTracks", {"page": page, "limit": limit})
        save_json(data, "chart_top_tracks", f"page_{page:03d}.json")

        tracks = data.get("tracks", {}).get("track", [])
        if not tracks:
            break

        for track in tracks:
            rows.append(
                parse_seed_track(
                    track,
                    source_type="chart",
                    source_value="global",
                    page=page
                )
            )

        time.sleep(REQUEST_SLEEP_SECONDS)

    return rows


def fetch_tag_tracks(tag: str, max_pages: int = TAG_PAGES, limit: int = PER_PAGE_LIMIT) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        print(f"[tag.getTopTracks][tag={tag}] page {page}")

        data = call_lastfm("tag.getTopTracks", {"tag": tag, "page": page, "limit": limit})
        save_json(data, "tag_top_tracks", f"tag={tag}_page_{page:03d}.json")

        tracks = data.get("tracks", {}).get("track", [])
        if not tracks:
            break

        for track in tracks:
            rows.append(
                parse_seed_track(
                    track,
                    source_type="tag",
                    source_value=tag,
                    page=page
                )
            )

        time.sleep(REQUEST_SLEEP_SECONDS)

    return rows


# ============================================================
# Candidate track building
# ============================================================

def build_candidate_tracks(seed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create one canonical candidate row per artist-track pair.
    We keep provenance in the seed table, but here we deduplicate to prepare
    later matching with ReccoBeats.
    """
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
            "source_type": lambda s: sorted(set(x for x in s if pd.notna(x))),
            "source_value": lambda s: sorted(set(x for x in s if pd.notna(x))),
            "lastfm_playcount": "max",
            "lastfm_listeners": "max",
            "source_page": "min",
        })
        .reset_index()
    )

    grouped["seed_source_types"] = grouped["source_type"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    grouped["seed_source_values"] = grouped["source_value"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    grouped.drop(columns=["source_type", "source_value"], inplace=True)

    grouped["seed_occurrences"] = (
        candidate_df.groupby("track_key").size().reindex(grouped["track_key"]).values
    )

    return grouped


# ============================================================
# Main
# ============================================================

def main() -> None:
    print(f"Starting Last.fm ingestion run: {RUN_ID}")

    # --------------------------------------------------------
    # 1) Seed extraction: chart + tag charts
    # --------------------------------------------------------
    seed_rows: List[Dict[str, Any]] = []

    seed_rows.extend(fetch_chart_tracks(max_pages=CHART_PAGES, limit=PER_PAGE_LIMIT))

    for tag in TAGS:
        try:
            seed_rows.extend(fetch_tag_tracks(tag=tag, max_pages=TAG_PAGES, limit=PER_PAGE_LIMIT))
        except Exception as e:
            print(f"Skipping tag={tag} due to error: {e}")
            time.sleep(RETRY_SLEEP_SECONDS)

    seed_df = pd.DataFrame(seed_rows)

    # Save full seed table (duplicates/provenance preserved)
    seed_path = CURATED_ROOT / f"seed_tracks_run_{RUN_ID}.csv"
    seed_df.to_csv(seed_path, index=False)

    print(f"Seed rows collected: {len(seed_df)}")
    print(f"Saved seed track table to: {seed_path}")

    # --------------------------------------------------------
    # 2) Candidate track list for later ReccoBeats matching
    # --------------------------------------------------------
    candidate_df = build_candidate_tracks(seed_df)
    candidate_path = CURATED_ROOT / f"candidate_tracks_run_{RUN_ID}.csv"
    candidate_df.to_csv(candidate_path, index=False)

    print(f"Candidate artist-track pairs: {len(candidate_df)}")
    print(f"Saved candidate tracks to: {candidate_path}")

    print("\nLast.fm seed ingestion finished successfully.")


if __name__ == "__main__":
    main()