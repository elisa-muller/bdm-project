import duckdb

# no va de moment xd
con = duckdb.connect()

con.execute("INSTALL delta;")
con.execute("LOAD delta;")
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

con.execute("SET s3_region='us-east-1';")
con.execute("SET s3_endpoint='localhost:9000';")
con.execute("SET s3_access_key_id='minioadmin';")
con.execute("SET s3_secret_access_key='minioadmin';")
con.execute("SET s3_use_ssl=false;")
con.execute("SET s3_url_style='path';")

df = con.execute("""
    SELECT *
    FROM delta_scan('s3://lakehouse/bronze/persistent/reccobeats/audio_features_delta')
    LIMIT 20
""").df()

print(df)
print(f"Rows returned: {len(df)}")