import os
import requests
from dotenv import load_dotenv
from pathlib import Path

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "").strip()

if not DATABRICKS_HOST.startswith("http://") and not DATABRICKS_HOST.startswith("https://"):
    DATABRICKS_HOST = "https://" + DATABRICKS_HOST

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/octet-stream"
}

# Testar PUT em um arquivo
url = f"{DATABRICKS_HOST}/api/2.0/fs/files/Volumes/lakehouse/wc_platform/files/raw/statsbomb/wc_2022/three-sixty/test_file.json?overwrite=true"
print(f"Testando PUT em: {url}")
res = requests.put(url, data='{"test": true}', headers=headers)
print("Status Code:", res.status_code)
print("Response Headers:", res.headers)
print("Response Text:", res.text)
