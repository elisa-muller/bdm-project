from minio import Minio
import os

# -------------------------
# CONNECT TO MINIO
# -------------------------

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "landing-zone"

# -------------------------
# CREATE BUCKET IF NEEDED
# -------------------------

if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' created")
else:
    print(f"Bucket '{bucket_name}' already exists")

# -------------------------
# CSV FILES TO UPLOAD
# -------------------------

files_to_upload = [

    # Last.fm structured raw data
    # {
    #     "local_path": "ingestion/spotify_batch/data/lastfm_tracks_unique.csv",
    #     "object_name": "temporal/structured/lastfm/raw/lastfm_tracks_unique.csv"
    # },

    # Spotify structured raw data
    # Uncomment when the file is available
    # {
    #     "local_path": "ingestion/spotify_batch/data/spotify_tracks.csv",
    #     "object_name": "temporal/structured/spotify/raw/spotify_tracks.csv"
    # },

    # Spotify artists structured raw data
    # Uncomment when the file is available
    # {
    #     "local_path": "ingestion/spotify_batch/data/spotify_artists.csv",
    #     "object_name": "temporal/structured/spotify/raw/spotify_artists.csv"
    # },

    # Mastodon semi-structured raw data
    # Uncomment when the file is available
    # {
    #     "local_path": "ingestion/mastodon_stream/data/mastodon_posts.json",
    #     "object_name": "temporal/semi_structured/mastodon/raw/mastodon_posts.json"
    # },

    # Images are intentionally not uploaded at this stage.
    # According to the current project design, the focus is on making the image
    # streaming pipeline work. Image labels or aggregates may be stored later in P2.
]

# -------------------------
# UPLOAD FILES
# -------------------------

for file in files_to_upload:

    local_path = file["local_path"]
    object_name = file["object_name"]

    if os.path.exists(local_path):

        client.fput_object(
            bucket_name,
            object_name,
            local_path
        )

        print(f"Uploaded file -> {object_name}")

    else:
        print(f"File not found -> {local_path}")