import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, Dict, Any, List
import os
import time
import threading
import time

import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils._huggingface_uploader import load_urls_data, get_flat_video_list
from _yt_api.core._get_base_info_yt_api import (
    fetch_from_youtube_api, 
    get_quota_used,
    force_switch_key,
    are_all_keys_exhausted,
    get_current_key_info
)

PROJECT_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HF_REPO_ID      = "Ilialebedev/snapshots_yt_api"
HF_TOKEN        = ""
CACHE_DIR       = os.path.join(PROJECT_ROOT, "_yt_api", ".cache")
RESULTS_DIR     = os.path.join(PROJECT_ROOT, "_yt_api", ".results")
URLS_DATA_PATH  = os.path.join(PROJECT_ROOT, ".input", "urls.json")
URLS_DATA       = load_urls_data(URLS_DATA_PATH)
FLAT_VIDEO_LIST = get_flat_video_list(URLS_DATA)
PROGRESS_PATH   = os.path.join(RESULTS_DIR, "progress.json")
FINAL_RESULTS_PATH = os.path.join(RESULTS_DIR, "yt_api_aggregate.json")
BATCH_SIZE      = 16


def batchify(lst, batch_size):
    """Разбивает lst на батчи по batch_size"""
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]

def get_exists_urls_and_last_batch_number(RESULTS_DIR: str):
    exist_urls = []

    with open(os.path.join(RESULTS_DIR, "progress.json"), "r") as f:
        data = json.load(f)
        exist_urls = [
            url for url in data.get("processed_urls", [])
        ]
        last_batch_number = max(
            int(
                file.split("_")[1].split(".")[0]
            ) for file in os.listdir(RESULTS_DIR) if file.startswith("batch_") and file.endswith(".json"))

    return exist_urls, last_batch_number

def check_and_mk_dirs(dirs: list[str]):
    for dir in dirs:
        os.makedirs(dir, exist_ok=True)

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

def worker(args: Tuple[str, str]) -> Tuple[str, str, str, Dict[str, Any]]:
    try:
        category, video_url = args
        video_id = extract_video_id(video_url) or video_url.split("?v=")[-1]
        if not video_id or len(video_id) != 11:
            return category, video_url, "", {}
        data = fetch_from_youtube_api(video_id)
    except Exception as e:
        print(f"worker error: {e}")
    return category, video_url, video_id, data

def process_results(results, batch_start, quota_before) -> Tuple[int, Dict[str, int], float, int, int, int, str]:
    success_count = sum(1 for _c,_u,_vid,d in results if d and not d.get('_error'))
    failures: Dict[str, int] = {}
    for _c,_u,_vid,d in results:
        if not d:
            failures['empty'] = failures.get('empty', 0) + 1
        elif d.get('_error'):
            reason = d['_error'].get('type', 'unknown')
            failures[reason] = failures.get(reason, 0) + 1
    elapsed = time.perf_counter() - batch_start
    quota_after = get_quota_used()
    quota_used_in_batch = quota_after - quota_before
    
    # Итоговая информация по батчу
    key_num, total_keys = get_current_key_info()
    if failures:
        fail_str = ", ".join([f"{k}:{v}" for k,v in failures.items()])
    else:
        fail_str = "-"
    return success_count, elapsed, quota_used_in_batch, key_num, total_keys, fail_str

def check_for_empty_batches(success_count: int, MAX_EMPTY_BATCHES, empty_batches_count: int, total_keys: int) -> bool:
    if success_count == 0:
        empty_batches_count += 1
        if empty_batches_count >= MAX_EMPTY_BATCHES:
            if are_all_keys_exhausted():
                print(f"⚠️  Все {total_keys} API ключа исчерпаны! Остановка обработки.")
                return True
            else:
                print(f"⚠️  {empty_batches_count} пустых батчей подряд. Принудительная ротация ключа...")
                if force_switch_key():
                    key_num, total_keys = get_current_key_info()
                    print(f"✅ Переключено на ключ {key_num}/{total_keys}")
                    empty_batches_count = 0  # Сбрасываем счетчик после ротации
                else:
                    print(f"❌ Не удалось переключить ключ. Все ключи исчерпаны.")
                    return True
    else:
        empty_batches_count = 0  # Сбрасываем счетчик при успешном батче
        return False

def save_batch_results(
    batch_num: int, 
    batch: List[Tuple[str, str, str, Dict[str, Any]]], 
    elapsed: float, 
    quota_used_in_batch: int, 
    success_count: int,
    batches_index: List[Dict[str, Any]],
    batch_videos: Dict[str, Any],
) -> None:
    per_batch_payload: Dict[str, Any] = {
        "batch": batch_num,
        "size": len(batch),
        "success": success_count,
        "durationSec": round(elapsed, 3),
        "quotaUsed": int(quota_used_in_batch),
        "videos": batch_videos,
    }
    with open(os.path.join(RESULTS_DIR, f"batch_{batch_num}.json"), "w", encoding="utf-8") as bf:
        json.dump(per_batch_payload, bf, ensure_ascii=False, indent=2)
    batches_index.append({
        "batch": batch_num,
        "size": len(batch),
        "success": success_count,
        "durationSec": round(elapsed, 3)
    })

def main():

    check_and_mk_dirs([CACHE_DIR, RESULTS_DIR])
    exist_urls, last_batch_number = get_exists_urls_and_last_batch_number(RESULTS_DIR)

    flat_video_list = [(cat, url) for (cat, url) in flat_video_list if url not in exist_urls]

    batches = list(batchify(flat_video_list, BATCH_SIZE))
    combined_results: Dict[str, Any] = {}
    batches_index: List[Dict[str, Any]] = []
    empty_batches_count = 0  # Счетчик пустых батчей подряд
    MAX_EMPTY_BATCHES = 3  # После 3 пустых батчей подряд - принудительная ротация ключа

    for i, batch in enumerate(batches):
        batch_num = last_batch_number + i + 1
        batch_start = time.perf_counter()
        quota_before = get_quota_used()

        results: List[Tuple[str, str, str, Dict[str, Any]]] = []

        max_workers = min(len(batch), 4)
        progress_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_item = {executor.submit(worker, item): item for item in batch}

            for future in as_completed(future_to_item):

                category, video_url, video_id, data = future.result()

                if data and video_id and not data.get('_error'):
                    combined_results[video_id] = data

                results.append((category, video_url, video_id, data))

                if data and video_id and not data.get('_error'):
                    with progress_lock:
                        exist_urls.add(video_url)
                        save_progress(PROGRESS_PATH, exist_urls)
        
        success_count, elapsed, quota_used_in_batch, key_num, total_keys, fail_str = process_results(results, batch_start, quota_before)
        print(f"Batch {batch_num}: всего: {len(batch)}, success: {success_count}, fails: {fail_str}, квота: {quota_used_in_batch}, время: {elapsed:.2f}s, ключ: {key_num}/{total_keys}")

        if check_for_empty_batches(success_count, MAX_EMPTY_BATCHES, empty_batches_count, total_keys):
            break

        if success_count > 0:
            batch_videos: Dict[str, Any] = {vid: data for (_c,_u,vid,data) in results if data and vid and not data.get('_error')}
            save_batch_results(batch_num, batch, elapsed, quota_used_in_batch, success_count, batches_index, batch_videos)

    payload: Dict[str, Any] = {"_batches": batches_index}
    payload.update(combined_results)
    with open(FINAL_RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
    
    
