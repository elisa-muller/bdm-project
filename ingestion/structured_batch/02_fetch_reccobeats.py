# ingestion/structured_batch/02_fetch_reccobeats.py
#
# Purpose:
# - Read candidate artist-track pairs from Last.fm candidate CSV
# - Search each Last.fm artist in ReccoBeats
# - Fetch that artist's ReccoBeats tracks
# - Match Last.fm track names locally against the ReccoBeats track catalog
# - Fetch audio features for matched ReccoBeats track IDs
# - Save raw JSON responses locally
# - Save CSV manifests, including matched %
#
# Later, in Airflow we use to schedule API calls:
#   python3 ingestion/structured_batch/02_fetch_reccobeats.py

import os
import re
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

# ============================================================
# Environment / config
# ============================================================

load_dotenv()

RECCOBEATS_BASE_URL = "https://api.reccobeats.com/v1"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PROJECT_ROOT / "data"

LASTFM_CURATED_ROOT = DATA_ROOT / "lastfm" / "curated"
RECCOBEATS_ROOT = DATA_ROOT / "reccobeats"
RAW_ROOT = RECCOBEATS_ROOT / "raw"
CURATED_ROOT = RECCOBEATS_ROOT / "curated"

RAW_ROOT.mkdir(parents=True, exist_ok=True)
CURATED_ROOT.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------
# Runtime config
# ------------------------------------------------------------

REQUEST_TIMEOUT = 20
REQUEST_SLEEP_SECONDS = 0.20
RETRY_SLEEP_SECONDS = 2.0
MAX_RETRIES = 3

# Optional cap for testing only
MAX_CANDIDATES = None   # e.g. 200

# Matching thresholds
MIN_TRACK_MATCH_SCORE = 0.75

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


def clean_track_title_for_match(text: Optional[str]) -> Optional[str]:
    """
    Normalize track names to improve matching between Last.fm and ReccoBeats.
    Removes common noisy suffixes but keeps the original title elsewhere.
    """
    if text is None:
        return None

    text = str(text).lower().strip()

    # Remove content in brackets/parentheses often used for versions/features
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"\[[^\]]*\]", " ", text)

    # Remove common version markers / descriptors
    noise_patterns = [
        r"\bfeat\b.*",
        r"\bfeaturing\b.*",
        r"\bft\b.*",
        r"\bremaster(ed)?\b.*",
        r"\blive\b.*",
        r"\bmono\b.*",
        r"\bstereo\b.*",
        r"\bradio edit\b.*",
        r"\bversion\b.*",
        r"\bedit\b.*",
        r"\bmix\b.*",
        r"\bdeluxe\b.*",
    ]
    for pattern in noise_patterns:
        text = re.sub(pattern, " ", text)

    # Normalize punctuation/spaces
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text if text else None


def build_track_key(artist_name: Optional[str], track_name: Optional[str]) -> Optional[str]:
    artist_norm = normalize_text(artist_name)
    track_norm = normalize_text(track_name)
    if not artist_norm or not track_norm:
        return None
    return f"{artist_norm}|||{track_norm}"


def build_artist_key(artist_name: Optional[str]) -> Optional[str]:
    return normalize_text(artist_name)


def safe_filename(text: str, max_len: int = 120) -> str:
    text = re.sub(r"[^a-zA-Z0-9._-]+", "_", text)
    return text[:max_len]


def save_json(payload: Any, endpoint_name: str, filename: str) -> Path:
    endpoint_dir = RUN_RAW_ROOT / endpoint_name
    endpoint_dir.mkdir(parents=True, exist_ok=True)
    out_path = endpoint_dir / filename
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return out_path


def find_latest_candidate_csv() -> Path:
    candidates = sorted(LASTFM_CURATED_ROOT.glob("candidate_tracks_run_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No candidate_tracks_run_*.csv found in {LASTFM_CURATED_ROOT}. "
            f"Run 01_fetch_lastfm.py first."
        )
    return candidates[-1]


def similarity_score(a: Optional[str], b: Optional[str]) -> float:
    """
    Lightweight local string similarity without extra dependencies.
    """
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0

    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if not a_tokens or not b_tokens:
        return 0.0

    jaccard = len(a_tokens & b_tokens) / len(a_tokens | b_tokens)

    # Prefix/containment bonus
    containment = 0.0
    if a in b or b in a:
        containment = 0.2

    return min(1.0, jaccard + containment)


# ============================================================
# ReccoBeats API
# ============================================================

def request_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if response.status_code == 200:
                return response.json()

            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Request failed with status {response.status_code}. "
                    f"URL={url}, params={params}, response={response.text[:300]}"
                )

            time.sleep(RETRY_SLEEP_SECONDS)

        except Exception as e:
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Request failed after {MAX_RETRIES} retries. "
                    f"URL={url}, params={params}, error={e}"
                ) from e
            time.sleep(RETRY_SLEEP_SECONDS)

    return {}


def extract_content_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict) and "content" in payload and isinstance(payload["content"], list):
        return payload["content"]
    if isinstance(payload, list):
        return payload
    return []


def search_artist_reccobeats(artist_name: str) -> Any:
    url = f"{RECCOBEATS_BASE_URL}/artist/search"
    params = {"name": artist_name}
    return request_json(url, params=params)


def get_artist_tracks_reccobeats(artist_id: str) -> Any:
    url = f"{RECCOBEATS_BASE_URL}/artist/{artist_id}/track"
    return request_json(url)


def get_track_audio_features_reccobeats(track_id: str) -> Any:
    url = f"{RECCOBEATS_BASE_URL}/track/{track_id}/audio-features"
    return request_json(url)


# ============================================================
# Artist matching
# ============================================================

def choose_best_artist_match(lastfm_artist_name: str, artist_candidates: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], float]:
    target = normalize_text(lastfm_artist_name)
    best_item = None
    best_score = -1.0

    for item in artist_candidates:
        candidate_name = item.get("name") or item.get("title") or item.get("artist")
        score = similarity_score(target, normalize_text(candidate_name))
        if score > best_score:
            best_item = item
            best_score = score

    return best_item, max(0.0, best_score)


def build_artist_catalog(candidate_df: pd.DataFrame) -> pd.DataFrame:
    artist_df = (
        candidate_df[["lastfm_artist_name"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
        .copy()
    )
    artist_df["artist_key"] = artist_df["lastfm_artist_name"].apply(build_artist_key)
    return artist_df


def match_artists_to_reccobeats(artist_df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    total = len(artist_df)
    for idx, row in artist_df.iterrows():
        artist_name = row["lastfm_artist_name"]
        artist_key = row["artist_key"]

        print(f"[artist search] {idx + 1}/{total} :: {artist_name}")

        try:
            payload = search_artist_reccobeats(artist_name)
            safe_name = safe_filename(artist_name)
            save_json(payload, "artist_search", f"{safe_name}.json")

            candidates = extract_content_list(payload)
            best_item, best_score = choose_best_artist_match(artist_name, candidates)

            if best_item is None:
                rows.append({
                    "run_id": RUN_ID,
                    "run_date": RUN_DATE,
                    "artist_key": artist_key,
                    "lastfm_artist_name": artist_name,
                    "artist_match_found": False,
                    "reccobeats_artist_id": None,
                    "reccobeats_artist_name": None,
                    "artist_match_score": 0.0,
                })
            else:
                rows.append({
                    "run_id": RUN_ID,
                    "run_date": RUN_DATE,
                    "artist_key": artist_key,
                    "lastfm_artist_name": artist_name,
                    "artist_match_found": True,
                    "reccobeats_artist_id": best_item.get("id"),
                    "reccobeats_artist_name": best_item.get("name") or best_item.get("title"),
                    "artist_match_score": best_score,
                })

        except Exception as e:
            print(f"  -> artist search failed for {artist_name}: {e}")
            rows.append({
                "run_id": RUN_ID,
                "run_date": RUN_DATE,
                "artist_key": artist_key,
                "lastfm_artist_name": artist_name,
                "artist_match_found": False,
                "reccobeats_artist_id": None,
                "reccobeats_artist_name": None,
                "artist_match_score": 0.0,
            })

        time.sleep(REQUEST_SLEEP_SECONDS)

    return pd.DataFrame(rows)


# ============================================================
# Track matching inside artist catalog
# ============================================================

def choose_best_track_match(lastfm_track_name: str, reccobeats_tracks: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], float]:
    target = clean_track_title_for_match(lastfm_track_name)
    best_item = None
    best_score = -1.0

    for item in reccobeats_tracks:
        candidate_name = item.get("name") or item.get("title")
        candidate_clean = clean_track_title_for_match(candidate_name)
        score = similarity_score(target, candidate_clean)

        if score > best_score:
            best_item = item
            best_score = score

    if best_score < MIN_TRACK_MATCH_SCORE:
        return None, max(0.0, best_score)

    return best_item, max(0.0, best_score)


def fetch_artist_track_catalogs(artist_match_df: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    """
    Returns a dict:
      reccobeats_artist_id -> list of track dicts
    """
    catalogs: Dict[str, List[Dict[str, Any]]] = {}

    matched_artists = artist_match_df[
        artist_match_df["artist_match_found"] == True
    ].dropna(subset=["reccobeats_artist_id"]).copy()

    total = len(matched_artists)
    for idx, row in matched_artists.iterrows():
        artist_id = str(row["reccobeats_artist_id"])
        artist_name = row["lastfm_artist_name"]

        print(f"[artist tracks] {idx + 1}/{total} :: {artist_name} ({artist_id})")

        try:
            payload = get_artist_tracks_reccobeats(artist_id)
            save_json(payload, "artist_tracks", f"artist_{artist_id}.json")
            catalogs[artist_id] = extract_content_list(payload)
        except Exception as e:
            print(f"  -> artist track fetch failed for {artist_name}: {e}")
            catalogs[artist_id] = []

        time.sleep(REQUEST_SLEEP_SECONDS)

    return catalogs


def match_candidate_tracks(
    candidate_df: pd.DataFrame,
    artist_match_df: pd.DataFrame,
    artist_track_catalogs: Dict[str, List[Dict[str, Any]]],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    artist_lookup = artist_match_df.set_index("artist_key").to_dict(orient="index")

    total = len(candidate_df)
    for idx, row in candidate_df.iterrows():
        artist_name = row["lastfm_artist_name"]
        track_name = row["lastfm_track_name"]
        track_key = row["track_key"]
        artist_key = row["artist_key"]

        artist_match = artist_lookup.get(artist_key, {})
        artist_found = bool(artist_match.get("artist_match_found"))
        reccobeats_artist_id = artist_match.get("reccobeats_artist_id")

        if not artist_found or pd.isna(reccobeats_artist_id):
            rows.append({
                "run_id": RUN_ID,
                "run_date": RUN_DATE,
                "track_key": track_key,
                "artist_key": artist_key,
                "lastfm_artist_name": artist_name,
                "lastfm_track_name": track_name,
                "artist_match_found": False,
                "track_match_found": False,
                "track_match_score": 0.0,
                "reccobeats_artist_id": None,
                "reccobeats_artist_name": None,
                "reccobeats_track_id": None,
                "reccobeats_track_name": None,
            })
            continue

        catalog = artist_track_catalogs.get(str(reccobeats_artist_id), [])
        best_track, best_score = choose_best_track_match(track_name, catalog)

        if best_track is None:
            rows.append({
                "run_id": RUN_ID,
                "run_date": RUN_DATE,
                "track_key": track_key,
                "artist_key": artist_key,
                "lastfm_artist_name": artist_name,
                "lastfm_track_name": track_name,
                "artist_match_found": True,
                "track_match_found": False,
                "track_match_score": best_score,
                "reccobeats_artist_id": reccobeats_artist_id,
                "reccobeats_artist_name": artist_match.get("reccobeats_artist_name"),
                "reccobeats_track_id": None,
                "reccobeats_track_name": None,
            })
        else:
            rows.append({
                "run_id": RUN_ID,
                "run_date": RUN_DATE,
                "track_key": track_key,
                "artist_key": artist_key,
                "lastfm_artist_name": artist_name,
                "lastfm_track_name": track_name,
                "artist_match_found": True,
                "track_match_found": True,
                "track_match_score": best_score,
                "reccobeats_artist_id": reccobeats_artist_id,
                "reccobeats_artist_name": artist_match.get("reccobeats_artist_name"),
                "reccobeats_track_id": best_track.get("id"),
                "reccobeats_track_name": best_track.get("name") or best_track.get("title"),
            })

        if (idx + 1) % 500 == 0 or idx + 1 == total:
            print(f"[track matching] {idx + 1}/{total}")

    return pd.DataFrame(rows)


# ============================================================
# Audio features fetch
# ============================================================

def fetch_audio_features(track_match_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows: List[Dict[str, Any]] = []
    request_rows: List[Dict[str, Any]] = []

    matched_tracks = (
        track_match_df[track_match_df["track_match_found"] == True]
        .dropna(subset=["reccobeats_track_id"])
        .drop_duplicates(subset=["reccobeats_track_id"])
        .reset_index(drop=True)
    )

    total = len(matched_tracks)
    for idx, row in matched_tracks.iterrows():
        track_id = str(row["reccobeats_track_id"])

        print(f"[audio features] {idx + 1}/{total} :: {row['lastfm_artist_name']} - {row['lastfm_track_name']}")

        try:
            payload = get_track_audio_features_reccobeats(track_id)
            save_json(payload, "audio_features", f"track_{track_id}.json")

            request_rows.append({
                "run_id": RUN_ID,
                "run_date": RUN_DATE,
                "reccobeats_track_id": track_id,
                "status": "success",
            })

            item = payload if isinstance(payload, dict) else {}

            summary_rows.append({
                "run_id": RUN_ID,
                "run_date": RUN_DATE,
                "track_key": row["track_key"],
                "lastfm_artist_name": row["lastfm_artist_name"],
                "lastfm_track_name": row["lastfm_track_name"],
                "reccobeats_artist_id": row["reccobeats_artist_id"],
                "reccobeats_artist_name": row["reccobeats_artist_name"],
                "reccobeats_track_id": track_id,
                "reccobeats_track_name": row["reccobeats_track_name"],
                "danceability": item.get("danceability"),
                "energy": item.get("energy"),
                "valence": item.get("valence"),
                "tempo": item.get("tempo"),
                "acousticness": item.get("acousticness"),
                "instrumentalness": item.get("instrumentalness"),
                "liveness": item.get("liveness"),
                "speechiness": item.get("speechiness"),
            })

        except Exception as e:
            print(f"  -> audio features failed for track_id={track_id}: {e}")

            error_payload = {
                "run_id": RUN_ID,
                "run_date": RUN_DATE,
                "reccobeats_track_id": track_id,
                "error": str(e),
            }
            save_json(error_payload, "audio_features_errors", f"track_{track_id}_error.json")

            request_rows.append({
                "run_id": RUN_ID,
                "run_date": RUN_DATE,
                "reccobeats_track_id": track_id,
                "status": "failed",
            })

        time.sleep(REQUEST_SLEEP_SECONDS)

    return pd.DataFrame(summary_rows), pd.DataFrame(request_rows)


# ============================================================
# Main
# ============================================================

def main() -> None:
    print(f"Starting ReccoBeats fetch run: {RUN_ID}")

    # --------------------------------------------------------
    # 1) Load Last.fm candidates
    # --------------------------------------------------------
    candidate_csv = find_latest_candidate_csv()
    print(f"Using candidate file: {candidate_csv}")

    candidate_df = pd.read_csv(candidate_csv)

    required_cols = {"track_key", "artist_key", "lastfm_artist_name", "lastfm_track_name"}
    missing_cols = required_cols - set(candidate_df.columns)
    if missing_cols:
        raise ValueError(f"Candidate file is missing required columns: {missing_cols}")

    candidate_df = candidate_df.dropna(subset=["lastfm_artist_name", "lastfm_track_name"]).copy()

    if MAX_CANDIDATES is not None:
        candidate_df = candidate_df.head(MAX_CANDIDATES).copy()

    total_candidates = len(candidate_df)
    print(f"Candidate rows to process: {total_candidates}")

    # --------------------------------------------------------
    # 2) Match artists in ReccoBeats
    # --------------------------------------------------------
    artist_df = build_artist_catalog(candidate_df)
    artist_match_df = match_artists_to_reccobeats(artist_df)

    artist_manifest_path = CURATED_ROOT / f"artist_match_manifest_run_{RUN_ID}.csv"
    artist_match_df.to_csv(artist_manifest_path, index=False)
    print(f"Saved artist match manifest to: {artist_manifest_path}")

    artist_matched_count = int(artist_match_df["artist_match_found"].fillna(False).sum())
    artist_total = len(artist_match_df)
    artist_matched_pct = (artist_matched_count / artist_total * 100.0) if artist_total else 0.0

    print(f"Artist matched: {artist_matched_count}/{artist_total} ({artist_matched_pct:.2f}%)")

    # --------------------------------------------------------
    # 3) Fetch artist track catalogs
    # --------------------------------------------------------
    artist_track_catalogs = fetch_artist_track_catalogs(artist_match_df)

    # --------------------------------------------------------
    # 4) Match Last.fm candidate tracks against ReccoBeats tracks
    # --------------------------------------------------------
    track_match_df = match_candidate_tracks(candidate_df, artist_match_df, artist_track_catalogs)

    track_manifest_path = CURATED_ROOT / f"track_match_manifest_run_{RUN_ID}.csv"
    track_match_df.to_csv(track_manifest_path, index=False)
    print(f"Saved track match manifest to: {track_manifest_path}")

    track_matched_count = int(track_match_df["track_match_found"].fillna(False).sum())
    track_matched_pct = (track_matched_count / total_candidates * 100.0) if total_candidates else 0.0

    print(f"Track matched: {track_matched_count}/{total_candidates} ({track_matched_pct:.2f}%)")

    if track_matched_count == 0:
        print("No matched ReccoBeats tracks found. Stopping before audio features.")
        return

    # --------------------------------------------------------
    # 5) Fetch audio features for matched tracks
    # --------------------------------------------------------
    audio_features_df, audio_request_df = fetch_audio_features(track_match_df)

    audio_summary_path = CURATED_ROOT / f"reccobeats_audio_features_summary_run_{RUN_ID}.csv"
    audio_requests_path = CURATED_ROOT / f"reccobeats_audio_feature_requests_run_{RUN_ID}.csv"

    audio_features_df.to_csv(audio_summary_path, index=False)
    audio_request_df.to_csv(audio_requests_path, index=False)

    print(f"Saved audio features summary to: {audio_summary_path}")
    print(f"Saved audio feature request log to: {audio_requests_path}")

    # --------------------------------------------------------
    # 6) Run metrics
    # --------------------------------------------------------
    audio_success_count = int((audio_request_df["status"] == "success").sum()) if not audio_request_df.empty else 0
    audio_request_total = len(audio_request_df)
    audio_success_pct = (audio_success_count / audio_request_total * 100.0) if audio_request_total else 0.0

    metrics = {
        "run_id": RUN_ID,
        "run_date": RUN_DATE,
        "candidate_tracks_total": total_candidates,
        "artists_total": artist_total,
        "artists_matched": artist_matched_count,
        "artists_matched_pct": round(artist_matched_pct, 2),
        "tracks_matched": track_matched_count,
        "tracks_matched_pct": round(track_matched_pct, 2),
        "audio_requests_total": audio_request_total,
        "audio_requests_success": audio_success_count,
        "audio_requests_success_pct": round(audio_success_pct, 2),
    }

    metrics_path = CURATED_ROOT / f"run_metrics_run_{RUN_ID}.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    print("\nRun metrics:")
    print(json.dumps(metrics, indent=2, ensure_ascii=False))
    print(f"Saved run metrics to: {metrics_path}")

    print("\nReccoBeats fetch finished successfully.")


if __name__ == "__main__":
    main()