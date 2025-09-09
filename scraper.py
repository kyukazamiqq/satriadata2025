import os
import pandas as pd
import re
import time
from yt_dlp import YoutubeDL
import gdown
from tqdm import tqdm

# === Konfigurasi ===
CSV_PATH = "datatrainfix.csv"    # CSV sumber data
BASE_DIR = "data"                # Folder output
COOKIE_FILE = "cookie.txt"       # File cookies Instagram (format Netscape)
BATCH_SIZE = 15                  # jumlah video per batch
SLEEP_MIN = 2                    # jeda minimal antar download
SLEEP_MAX = 5                    # jeda maksimal antar download
FAILED_FILE = "failed.txt"

# Baca CSV
df = pd.read_csv(CSV_PATH)
required_cols = {"id", "video", "emotion"}
if not required_cols.issubset(df.columns):
    raise ValueError(f"CSV harus mengandung kolom {required_cols}")

# Buat folder per label emosi
for label in sorted(df['emotion'].unique()):
    os.makedirs(os.path.join(BASE_DIR, str(label)), exist_ok=True)

failed_ids = []

def download_drive(url, save_path):
    match = re.search(r'/d/([^/]+)', url)
    if match:
        file_id = match.group(1)
        direct_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    else:
        direct_url = url
    gdown.download(direct_url, save_path, quiet=False)

def download_instagram(url, save_path):
    ydl_opts = {
        'outtmpl': save_path,
        'format': 'mp4',
        'cookiefile': COOKIE_FILE,
        'quiet': True,
        'continuedl': True,
        'sleep_interval': SLEEP_MIN,
        'max_sleep_interval': SLEEP_MAX
    }
    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

def process_batch(batch_df):
    for _, row in tqdm(batch_df.iterrows(), total=len(batch_df)):
        video_id = str(row['id'])
        url = str(row['video'])
        emotion_label = str(row['emotion'])
        save_path = os.path.join(BASE_DIR, emotion_label, f"{video_id}.mp4")
        try:
            if "instagram.com" in url:
                download_instagram(url, save_path)
            elif "drive.google.com" in url:
                download_drive(url, save_path)
            else:
                print(f"[SKIP] {video_id} - URL tidak dikenali")
            time.sleep(SLEEP_MIN)
        except Exception as e:
            print(f"[GAGAL] {video_id}: {e}")
            failed_ids.append(video_id)

# === Download utama ===
for start in range(0, len(df), BATCH_SIZE):
    end = min(start + BATCH_SIZE, len(df))
    batch_df = df.iloc[start:end]
    print(f"\n🚀 Memproses batch {start+1}–{end} dari {len(df)} total\n")
    process_batch(batch_df)
    print("⏳ Istirahat antar batch ±60 detik...")
    time.sleep(60)

# Simpan daftar gagal awal
if failed_ids:
    with open(FAILED_FILE, "w") as f:
        f.write("\n".join(map(str, failed_ids)))
    print(f"❌ {len(failed_ids)} video gagal. Lihat daftar di {FAILED_FILE}")
else:
    print("✅ Semua video berhasil diunduh")

# === Retry otomatis dari failed.txt (jika ada) ===
if os.path.exists(FAILED_FILE):
    with open(FAILED_FILE, 'r') as f:
        retry_ids = [line.strip() for line in f if line.strip()]
    if retry_ids:
        print(f"\n🔄 Retry download untuk {len(retry_ids)} video gagal...\n")
        retry_df = df[df['id'].astype(str).isin(retry_ids)]
        failed_ids_retry = []
        for _, row in tqdm(retry_df.iterrows(), total=len(retry_df)):
            video_id = str(row['id'])
            url = str(row['video'])
            emotion_label = str(row['emotion'])
            save_path = os.path.join(BASE_DIR, emotion_label, f"{video_id}.mp4")
            try:
                if "instagram.com" in url:
                    download_instagram(url, save_path)
                elif "drive.google.com" in url:
                    download_drive(url, save_path)
                else:
                    print(f"[SKIP] {video_id} - URL tidak dikenali")
                time.sleep(SLEEP_MIN)
            except Exception as e:
                print(f"[RETRY GAGAL] {video_id}: {e}")
                failed_ids_retry.append(video_id)
        if failed_ids_retry:
            with open(FAILED_FILE, "w") as f:
                f.write("\n".join(map(str, failed_ids_retry)))
            print(f"❌ Setelah retry, {len(failed_ids_retry)} masih gagal. Lihat {FAILED_FILE}")
        else:
            if os.path.exists(FAILED_FILE):
                os.remove(FAILED_FILE)
            print("✅ Semua video berhasil diunduh setelah retry")
