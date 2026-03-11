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

    {
        "local_path": r"C:\Users\usuario\Desktop\Màster\2nd semester\BDM\Project 1 BDM\bdm-project\ingestion\spotify_batch\data\lastfm_tracks_unique.csv",
        "object_name": "lastfm/raw/tracks_and_artists/lastfm_tracks_unique.csv"
    }

]

# -------------------------
# UPLOAD CSV FILES
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


# -------------------------
# UPLOAD IMAGE DATASET
# -------------------------

images_folder = r"C:\Users\usuario\Desktop\Màster\2nd semester\BDM\Project 1 BDM\valid_samples"

for img in os.listdir(images_folder):

    img_path = os.path.join(images_folder, img)

    if os.path.isfile(img_path):

        object_name = f"images/raw/{img}"

        client.fput_object(
            bucket_name,
            object_name,
            img_path
        )