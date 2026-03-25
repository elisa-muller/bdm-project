# pip install requests pandas

import os
import time
import requests
import pandas as pd
from typing import List, Dict

# -----------------------------
# Configuration
# -----------------------------
RECCOBEATS_BASE = "https://api.reccobeats.com/v1"

DATA_DIR = "data"
INPUT_PATH = os.path.join(DATA_DIR, "musicbrainz_isrc_cache_test.csv")
OUTPUT_PATH = os.path.join(DATA_DIR, "reccobeats_audio_features_test.csv")

BATCH_SIZE = 5
RB_SLEEP = 0.25


# -----------------------------
# Utility functions
# -----------------------------
def safe_str(x) -> str:
    """Safely convert a value to string and strip whitespace."""
    if pd.isna(x):
        return ""
    return str(x).strip()


def parse_feature_item(item: Dict) -> Dict:
    """
    Normalize a single ReccoBeats audio feature item.
    """
    href = item.get("href", "")
    rb_track_id = item.get("id")

    return {
        "rb_track_id": rb_track_id,
        "rb_href": href if href else None,
        "rb_name": item.get("name"),
        "rb_artist": item.get("artist"),
        "rb_isrc": item.get("isrc"),
        "rb_danceability": item.get("danceability"),
        "rb_energy": item.get("energy"),
        "rb_valence": item.get("valence"),
        "rb_tempo": item.get("tempo"),
        "rb_acousticness": item.get("acousticness"),
        "rb_instrumentalness": item.get("instrumentalness"),
        "rb_liveness": item.get("liveness"),
        "rb_loudness": item.get("loudness"),
        "rb_speechiness": item.get("speechiness"),
        "rb_mode": item.get("mode"),
        "rb_key": item.get("key"),
        "rb_time_signature": item.get("time_signature"),
        "rb_duration_ms": item.get("duration_ms"),
    }


# -----------------------------
# API call
# -----------------------------
def get_audio_features_batch(ids_batch: List[str]) -> List[Dict]:
    """
    Fetch audio features from ReccoBeats for a batch of ISRCs.
    """
    try:
        r = requests.get(
            f"{RECCOBEATS_BASE}/audio-features",
            params={"ids": ",".join(ids_batch)},
            timeout=15
        )

        print(f"[RB] status={r.status_code} ids={ids_batch}")

        if r.status_code == 200:
            data = r.json()

            if isinstance(data, dict) and "content" in data:
                return data["content"]

            if isinstance(data, list):
                return data

            print(f"[RB] Unexpected response format: {type(data)}")
            return []

        print(f"[RB] body={r.text[:300]}")
        return []

    except Exception as e:
        print(f"[RB] request error: {e}")
        return []


# -----------------------------
# Main pipeline
# -----------------------------
def main():
    print(f"Loading input: {INPUT_PATH}")
    df_isrc = pd.read_csv(INPUT_PATH)

    # Keep only rows with valid ISRC
    df_valid = df_isrc[df_isrc["isrc"].notna()].copy()
    df_valid["isrc"] = df_valid["isrc"].apply(safe_str)
    df_valid = df_valid[df_valid["isrc"] != ""].copy()

    print(f"Rows in input file: {len(df_isrc)}")
    print(f"Rows with valid ISRC before dedup: {len(df_valid)}")

    # Deduplicate the original rows so the final output does not explode
    df_valid = df_valid.drop_duplicates(
        subset=["lastfm_artist_name", "lastfm_track_name", "isrc"]
    ).reset_index(drop=True)

    print(f"Rows with valid ISRC after dedup: {len(df_valid)}")

    if df_valid.empty:
        print("No valid ISRC values found. Exiting.")
        return

    # Unique ISRCs to query
    ids_to_fetch = df_valid["isrc"].drop_duplicates().tolist()

    print(f"Unique ISRCs to fetch: {len(ids_to_fetch)}")

    all_features = []

    for i in range(0, len(ids_to_fetch), BATCH_SIZE):
        batch = ids_to_fetch[i:i + BATCH_SIZE]
        print(f"Fetching batch {i // BATCH_SIZE + 1}: {batch}")

        items = get_audio_features_batch(batch)

        if not items:
            print("  No items returned for this batch.")
        else:
            for item in items:
                all_features.append(parse_feature_item(item))

        time.sleep(RB_SLEEP)

    if not all_features:
        print("No audio features were returned by ReccoBeats.")
        return

    df_features = pd.DataFrame(all_features)

    # Clean and deduplicate features by ISRC
    if "rb_isrc" in df_features.columns:
        df_features["rb_isrc"] = df_features["rb_isrc"].apply(safe_str)
        df_features = df_features[df_features["rb_isrc"] != ""].copy()

    print("\n--- DEBUG COUNTS BEFORE FEATURE DEDUP ---")
    print(f"df_features rows: {len(df_features)}")
    print(f"df_features unique rb_isrc: {df_features['rb_isrc'].nunique(dropna=True)}")
    print(df_features["rb_isrc"].value_counts().head(10))

    df_features = df_features.drop_duplicates(subset=["rb_isrc"]).reset_index(drop=True)

    print("\n--- DEBUG COUNTS AFTER FEATURE DEDUP ---")
    print(f"df_valid rows: {len(df_valid)}")
    print(f"df_valid unique isrc: {df_valid['isrc'].nunique(dropna=True)}")
    print(f"df_features rows: {len(df_features)}")
    print(f"df_features unique rb_isrc: {df_features['rb_isrc'].nunique(dropna=True)}")

    # Safe many-to-one merge
    df_final = df_valid.merge(
        df_features,
        left_on="isrc",
        right_on="rb_isrc",
        how="left",
        validate="many_to_one"
    )

    df_final.to_csv(OUTPUT_PATH, index=False)

    print("\nDone.")
    print(f"Total rows in final output: {len(df_final)}")
    print(f"Rows with audio features: {df_final['rb_danceability'].notna().sum()}")
    print(f"Rows without audio features: {df_final['rb_danceability'].isna().sum()}")
    print(f"Saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()