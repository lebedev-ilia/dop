import json
import re
import os
import sys
import time
from typing import Optional, Tuple, Dict, Any, List

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils._huggingface_uploader import load_urls_data, get_flat_video_list
from core.yt_dlp_fetcher import fetch_from_ytdlp
from core.cookie_manager import CookieRotationManager


URLS_DATA_PATH = os.path.join(project_root, ".input", "urls.json")
COOKIE_MANAGER = CookieRotationManager()


def batchify(lst, batch_size):
    """Разбивает lst на батчи по batch_size"""
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]


def main():
    urls_data = load_urls_data(URLS_DATA_PATH)
    flat_video_list = get_flat_video_list(urls_data)


    results_dir = os.path.join(project_root, "_yt_dlp", ".results")
    os.makedirs(results_dir, exist_ok=True)
    progress_path = os.path.join(results_dir, "progress.json")

    def load_progress(path: str) -> set[str]:
        try:
            if os.path.isfile(path):
                with open(path, 'r', encoding='utf-8') as pf:
                    data = json.load(pf)
                    urls = data.get("processed_urls", [])
                    return set(urls) if isinstance(urls, list) else set()
        except Exception:
            pass
        return set()

    def save_progress(path: str, processed: set[str]) -> None:
        try:
            payload = {
                "processed_urls": sorted(list(processed)),
                "count": len(processed)
            }
            with open(path, 'w', encoding='utf-8') as pf:
                json.dump(payload, pf, ensure_ascii=False, indent=2)
        except Exception:
            pass

    processed_urls = load_progress(progress_path)
    # Filter out already processed URLs
    flat_video_list = [(cat, url) for (cat, url) in flat_video_list if url not in processed_urls]

    batch_size = 16
    batches = list(batchify(flat_video_list, batch_size))
    combined_results: Dict[str, Any] = {}
    batches_index: List[Dict[str, Any]] = []
    
    def get_existing_batches_numbers() -> List[int]:
        m = max([int(file.split("_")[1].split(".")[0]) for file in os.listdir(results_dir) if file.startswith("batch_") and file.endswith(".json")])
        return m if m else 0

    def extract_video_id(video_url: str) -> Optional[str]:
        patterns = [
            r'(?:v=|\/)([0-9A-Za-z_-]{11}).*',
            r'youtube\.com\/watch\?v=([0-9A-Za-z_-]{11})',
            r'youtu\.be\/([0-9A-Za-z_-]{11})'
        ]
        for pattern in patterns:
            match = re.search(pattern, video_url)
            if match:
                return match.group(1)
        return None
    
    last_batch_number = get_existing_batches_numbers()

    for i, batch in enumerate(batches):
        
        batch_num = last_batch_number + i + 1
        
        print(f"[yt-dlp] Batch {batch_num} (size={len(batch)}): starting processing")
        batch_start = time.perf_counter()

        def worker(args: Tuple[str, str]) -> Tuple[str, str, str, Dict[str, Any]]:
            category, video_url = args
            video_id = extract_video_id(video_url) or video_url.split("?v=")[-1]
            
            data = fetch_from_ytdlp(video_url, COOKIE_MANAGER)
            return category, video_url, video_id, data

        results: List[Tuple[str, str, str, Dict[str, Any]]] = []
        # Sequential processing within the batch (no parallelism)
        for item in batch:
            category, video_url, video_id, data = worker(item)
            
            status = "OK" if data else "EMPTY"
            if status == "OK":
                ext_time = data["timings_ytdlp"]["extract_info_seconds"]    
                captions_time = data["timings_ytdlp"]["captions_seconds_total"]
                total_time = data["timings_ytdlp"]["total_seconds"]
                
                if len(str(ext_time)) < 3:
                    ext_time = f"{ext_time}0"
                if len(str(captions_time)) < 3:
                    captions_time = f"{captions_time}0"
                if len(str(total_time)) < 3:
                    total_time = f"{total_time}0"
                
                print(
                    f"[yt-dlp] {status} | {video_url} | ext_time: {ext_time} | captions_time: {captions_time} | total_time: {total_time}"
                )
            else:
                print(f"[yt-dlp] {status} | {video_url}")
                
            if data and video_id:
                combined_results[video_id] = data
                
            results.append((category, video_url, video_id, data))

            processed_urls.add(video_url)
            save_progress(progress_path, processed_urls)

        success_count = sum(1 for _c,_u,_vid,d in results if d)
        elapsed = time.perf_counter() - batch_start
        
        print(f"[yt-dlp] Batch {i+1}: {success_count}/{len(results)} in {elapsed:.2f}s")

        batch_videos: Dict[str, Any] = {vid: data for (_c,_u,vid,data) in results if data and vid}
        
        per_batch_payload: Dict[str, Any] = {
            "batch": i + 1,
            "size": len(batch),
            "success": success_count,
            "durationSec": round(elapsed, 3),
            "videos": batch_videos,
        }
        
        per_batch_path = os.path.join(results_dir, f"batch_{batch_num}.json")
        
        with open(per_batch_path, "w", encoding="utf-8") as bf:
            json.dump(per_batch_payload, bf, ensure_ascii=False, indent=2)
            
        batches_index.append({
            "batch": batch_num,
            "size": len(batch),
            "success": success_count,
            "durationSec": round(elapsed, 3)
        })

    output_path = os.path.join(results_dir, "yt_dlp_aggregate.json")
    payload: Dict[str, Any] = {"_batches": batches_index}
    payload.update(combined_results)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        
    print(f"Saved yt-dlp combined results: {output_path} (videos: {len(combined_results)})")


if __name__ == "__main__":
    main()
