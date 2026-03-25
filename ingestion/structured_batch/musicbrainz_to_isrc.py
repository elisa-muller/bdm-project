# pip install requests pandas

import os
import time
import requests
import pandas as pd
from typing import Optional

# -----------------------------
# Configuration
# -----------------------------
MUSICBRAINZ_BASE = "https://musicbrainz.org/ws/2"

MUSICBRAINZ_HEADERS = {
    "User-Agent": "bdm-project/1.0 (student-project; contact: albaqiu03@gmail.com)"
}

DATA_DIR = "data"
INPUT_PATH = os.path.join(DATA_DIR, "lastfm_tracks_unique.csv")
OUTPUT_PATH = os.path.join(DATA_DIR, "musicbrainz_isrc_cache_test.csv")

TEST_MODE = True
TEST_SIZE = 60

# Keep this conservative for MusicBrainz
MB_SLEEP = 1.1
MAX_RETRIES_503 = 3


# -----------------------------
# Utility functions
# -----------------------------
def safe_str(x) -> str:
    """Safely convert a value to string and strip whitespace."""
    if pd.isna(x):
        return ""
    return str(x).strip()


def extract_first_isrc(data: dict) -> Optional[str]:
    """Extract the first ISRC from a MusicBrainz response."""
    isrcs = data.get("isrcs", [])
    if isinstance(isrcs, list) and len(isrcs) > 0:
        return isrcs[0]
    return None


# -----------------------------
# MusicBrainz resolution method
# -----------------------------
def get_isrc_from_mbid(mbid: str, max_retries: int = MAX_RETRIES_503):
    """
    Resolve ISRC using a MusicBrainz recording MBID.

    Returns:
        isrc: resolved ISRC or None
        status_label: one of
            - "ok"
            - "not_found_404"
            - "rate_limited_503"
            - "other_http_error"
            - "request_exception"
            - "invalid_mbid"
            - "no_isrc"
    """
    mbid = safe_str(mbid)

    if not mbid or len(mbid) < 10:
        return None, "invalid_mbid"

    for attempt in range(max_retries + 1):
        try:
            r = requests.get(
                f"{MUSICBRAINZ_BASE}/recording/{mbid}",
                params={"inc": "isrcs", "fmt": "json"},
                headers=MUSICBRAINZ_HEADERS,
                timeout=15,
            )

            if r.status_code == 200:
                data = r.json()
                isrc = extract_first_isrc(data)
                if isrc:
                    return isrc, "ok"
                return None, "no_isrc"

            if r.status_code == 503:
                if attempt < max_retries:
                    wait_time = 2 * (attempt + 1)
                    print(f"[MBID] status=503 mbid={mbid} | retry {attempt + 1}/{max_retries} in {wait_time}s")
                    time.sleep(wait_time)
                    continue
                return None, "rate_limited_503"

            if r.status_code == 404:
                return None, "not_found_404"

            print(f"[MBID] status={r.status_code} mbid={mbid}")
            return None, "other_http_error"

        except Exception as e:
            print(f"[MBID] error mbid={mbid}: {e}")
            if attempt < max_retries:
                wait_time = 2 * (attempt + 1)
                time.sleep(wait_time)
                continue
            return None, "request_exception"

    return None, "request_exception"


# -----------------------------
# Test set builder
# -----------------------------
def build_test_set(df_tracks: pd.DataFrame, test_size: int) -> pd.DataFrame:
    """
    Build a test set only with tracks that have MBID.
    """
    df_with_mbid = df_tracks[df_tracks["lastfm_track_mbid"].notna()].copy()
    df_test = df_with_mbid.head(test_size).reset_index(drop=True)
    return df_test


# -----------------------------
# Main pipeline
# -----------------------------
def main():
    print(f"Loading input: {INPUT_PATH}")
    df_tracks = pd.read_csv(INPUT_PATH)

    # Keep only tracks with MBID
    df_tracks = df_tracks[df_tracks["lastfm_track_mbid"].notna()].copy()
    print(f"After filtering MBID: {len(df_tracks)} rows")

    # Optional: remove duplicate artist-track-MBID rows before processing
    df_tracks = df_tracks.drop_duplicates(
        subset=["lastfm_artist_name", "lastfm_track_name", "lastfm_track_mbid"]
    ).reset_index(drop=True)
    print(f"After dedup artist-track-mbid: {len(df_tracks)} rows")

    if TEST_MODE:
        df_tracks = build_test_set(df_tracks, TEST_SIZE)
        print(f"TEST MODE enabled: using {len(df_tracks)} rows")
        print(f"Rows with MBID: {df_tracks['lastfm_track_mbid'].notna().sum()}")

    if TEST_MODE and os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)
        print(f"Removed previous test output: {OUTPUT_PATH}")

    if os.path.exists(OUTPUT_PATH):
        df_cache = pd.read_csv(OUTPUT_PATH)
        print(f"Resuming from checkpoint: {len(df_cache)} rows already processed")
    else:
        df_cache = pd.DataFrame(columns=[
            "lastfm_artist_name",
            "lastfm_track_name",
            "lastfm_track_mbid",
            "resolution_method",
            "resolved_mbid",
            "isrc",
            "mb_status"
        ])

    resolved_keys = set(
        zip(
            df_cache["lastfm_artist_name"],
            df_cache["lastfm_track_name"],
            df_cache["lastfm_track_mbid"]
        )
    )

    to_process = [
        row for _, row in df_tracks.iterrows()
        if (
            safe_str(row["lastfm_artist_name"]),
            safe_str(row["lastfm_track_name"]),
            safe_str(row["lastfm_track_mbid"])
        ) not in resolved_keys
    ]

    print(f"Tracks to process: {len(to_process)}")

    new_rows = []
    stats = {
        "ok": 0,
        "no_isrc": 0,
        "not_found_404": 0,
        "rate_limited_503": 0,
        "other_http_error": 0,
        "request_exception": 0,
        "invalid_mbid": 0
    }

    for i, row in enumerate(to_process):
        artist = safe_str(row["lastfm_artist_name"])
        track = safe_str(row["lastfm_track_name"])
        mbid = safe_str(row["lastfm_track_mbid"])

        if i % 50 == 0 and i > 0:
            print(f"{i}/{len(to_process)} processed...")
            pd.concat([df_cache, pd.DataFrame(new_rows)], ignore_index=True).to_csv(
                OUTPUT_PATH, index=False
            )

        isrc, mb_status = get_isrc_from_mbid(mbid)
        stats[mb_status] = stats.get(mb_status, 0) + 1

        if isrc:
            resolution_method = "mbid"
        else:
            resolution_method = f"mbid_{mb_status}"

        new_rows.append({
            "lastfm_artist_name": artist,
            "lastfm_track_name": track,
            "lastfm_track_mbid": mbid,
            "resolution_method": resolution_method,
            "resolved_mbid": mbid,
            "isrc": isrc,
            "mb_status": mb_status
        })

        print(
            f"[{i + 1}/{len(to_process)}] "
            f"artist='{artist}' | track='{track}' | "
            f"method={resolution_method} | isrc={isrc}"
        )

        time.sleep(MB_SLEEP)

    df_final = pd.concat([df_cache, pd.DataFrame(new_rows)], ignore_index=True)

    # Final deduplication in case of reruns/checkpoints
    df_final = df_final.drop_duplicates(
        subset=["lastfm_artist_name", "lastfm_track_name", "lastfm_track_mbid"],
        keep="last"
    ).reset_index(drop=True)

    df_final.to_csv(OUTPUT_PATH, index=False)

    print("\nDone.")
    print(f"Total processed: {len(df_final)}")
    print(f"ISRC found: {df_final['isrc'].notna().sum()}")
    print(f"Missing ISRC: {df_final['isrc'].isna().sum()}")

    print("\nMusicBrainz status counts:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    print("\nResolution method counts:")
    print(df_final["resolution_method"].value_counts(dropna=False))

    print(f"\nSaved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()