import os
import pandas as pd
import re
from yt_dlp import YoutubeDL
import gdown
from tqdm import tqdm

# === Konfigurasi ===
CSV_PATH = "datatrainfix.csv"
BASE_DIR = "data"

# Baca CSV
df = pd.read_csv(CSV_PATH)

# Validasi kolom
required_cols = {"id", "video", "emotion"}
if not required_cols.issubset(df.columns):
    raise ValueError(f"CSV harus mengandung kolom {required_cols}")

# Buat folder untuk setiap label
for label in sorted(df['emotion'].unique()):
    os.makedirs(os.path.join(BASE_DIR, str(label)), exist_ok=True)

def download_drive(url, save_path):
    """
    Unduh file dari Google Drive.
    """
    # Ekstrak file_id dari URL
    match = re.search(r'/d/([^/]+)', url)
    if match:
        file_id = match.group(1)
        direct_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    else:
        direct_url = url
    gdown.download(direct_url, save_path, quiet=False)

def download_instagram(url, save_path):
    """
    Unduh video dari Instagram (Reels) menggunakan yt_dlp.
    """
    ydl_opts = {
        'outtmpl': save_path,
        'format': 'mp4',
        'quiet': True
    }
    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

# Loop unduh berdasarkan sumber
for _, row in tqdm(df.iterrows(), total=len(df)):
    video_id = row['id']
    url = str(row['video'])
    emotion_label = str(row['emotion'])
    save_path = os.path.join(BASE_DIR, emotion_label, f"{video_id}.mp4")

    try:
        if "instagram.com" in url:
            download_instagram(url, save_path)
        elif "drive.google.com" in url:
            download_drive(url, save_path)
        else:
            print(f"[SKIP] {video_id} - URL tidak dikenali: {url}")
    except Exception as e:
        print(f"[GAGAL] {video_id}: {e}")

print("Selesai mengunduh semua video.")