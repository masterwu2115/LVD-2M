import os
import argparse
import pandas as pd
import yt_dlp as ydl
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from tqdm import tqdm

# Argument parser for the script
parser = argparse.ArgumentParser(description="Download videos based on new CSV format")
parser.add_argument("--meta_csv", type=str, required=True, help="Path to the CSV file with metadata")
parser.add_argument("--output_dir", type=str, default="./videos", help="Directory to save the downloaded videos")
parser.add_argument("--num_workers", type=int, default=4, help="Number of parallel download workers")
args = parser.parse_args()

# Function to download a single video
def download_video(video_url, output_path):
    """Downloads a video from YouTube using yt-dlp."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        ydl_opts = {
            'format': '136/247',
            'outtmpl': output_path,
            'continue': True,
            'external-downloader': 'aria2c',
            'external-downloader-args': '-x 16 -k 1M',
        }
        with ydl.YoutubeDL(ydl_opts) as ydl_instance:
            ydl_instance.download([video_url])
        print(f"Downloaded: {output_path}")
        return True
    except Exception as e:
        print(f"Failed to download {video_url}: {e}")
        return False

# Function to process the new CSV format
def process_csv(meta_csv):
    """Processes the new CSV format and returns a list of video info."""
    df = pd.read_csv(meta_csv)
    video_data = []
    for _, row in df.iterrows():
        video_data.append({
            "key": row["key"],
            "url": row["url"],
            "orig_span": eval(row["orig_span"]),
        })
    return video_data

# Main script execution
def main():
    # Load and process the new CSV format
    video_data = process_csv(args.meta_csv)
    print(f"Found {len(video_data)} videos to download.")

    # Download videos in parallel
    with ThreadPoolExecutor(max_workers=args.num_workers) as executor:
        futures = []
        for video in video_data:
            output_path = os.path.join(args.output_dir, f"{video['key']}.mp4")
            futures.append(executor.submit(download_video, video["url"], output_path))
        
        # Track progress
        for _ in tqdm(futures, total=len(futures), desc="Downloading videos"):
            pass

    print("Download complete.")

if __name__ == "__main__":
    main()