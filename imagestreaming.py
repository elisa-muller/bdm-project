import os
import time
import json
import base64
from datetime import datetime

# Simulate streaming - for now just prints, later sends to Kafka
IMAGE_FOLDER = IMAGE_FOLDER = r"C:\Users\Elisa\Desktop\MDS\BDM\Project\archive\val_256\valid_samples"

def simulate_stream(interval_seconds=2):
    images = os.listdir(IMAGE_FOLDER)
    images = [f for f in images if f.endswith(('.jpg', '.jpeg', '.png'))]
    
    print(f"Found {len(images)} images. Starting stream...")
    
    for image_file in images:
        image_path = os.path.join(IMAGE_FOLDER, image_file)
        
        # Create a message simulating what Kafka would carry
        message = {
            "timestamp": datetime.utcnow().isoformat(),
            "filename": image_file,
            "path": image_path,
            "size_bytes": os.path.getsize(image_path)
        }
        
        print(json.dumps(message))
        time.sleep(interval_seconds)

simulate_stream(interval_seconds=2)