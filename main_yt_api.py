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

def batchify(lst, batch_size):
    """Разбивает lst на батчи по batch_size"""
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]

def main():
    root_dir = os.path.dirname(os.path.dirname(__file__))
    urls_json_path = os.path.join(root_dir, ".input", "urls.json")
    urls_data = load_urls_data(urls_json_path)
    flat_video_list = get_flat_video_list(urls_data)

    # Progress directory and file
    progress_dir = os.path.join(root_dir, "_yt_api", ".results")
    os.makedirs(progress_dir, exist_ok=True)
    progress_path = os.path.join(progress_dir, "progress.json")

    # Determine next batch index based on existing files to avoid overwrite
    def _scan_batches_in_dir(path: str) -> int:
        max_idx = 0
        try:
            if os.path.isdir(path):
                for fname in os.listdir(path):
                    m = re.match(r"batch_(\\d+)\\.json$", fname)
                    if m:
                        try:
                            idx = int(m.group(1))
                            if idx > max_idx:
                                max_idx = idx
                        except Exception:
                            continue
        except Exception:
            pass
        return max_idx

    existing_batch_idx = 0
    # New location
    existing_batch_idx = max(existing_batch_idx, _scan_batches_in_dir(progress_dir))
    # Legacy location compatibility: results/yt_api
    legacy_dir = os.path.join(root_dir, "results", "yt_api")
    existing_batch_idx = max(existing_batch_idx, _scan_batches_in_dir(legacy_dir))

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
    empty_batches_count = 0  # Счетчик пустых батчей подряд
    MAX_EMPTY_BATCHES = 3  # После 3 пустых батчей подряд - принудительная ротация ключа
    
    for i, batch in enumerate(batches):
        batch_num = existing_batch_idx + i + 1
        batch_start = time.perf_counter()
        quota_before = get_quota_used()

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
            category, video_url = args
            video_id = extract_video_id(video_url) or video_url.split("?v=")[-1]
            if not video_id or len(video_id) != 11:
                return category, video_url, "", {}
            data = fetch_from_youtube_api(video_id)
            return category, video_url, video_id, data

        results: List[Tuple[str, str, str, Dict[str, Any]]] = []
        # Limit parallelism to reduce SSL/client pressure
        max_workers = min(len(batch), 4)
        progress_lock = threading.Lock()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_item = {executor.submit(worker, item): item for item in batch}
            for future in as_completed(future_to_item):
                category, video_url, video_id, data = future.result()
                if data and video_id:
                    combined_results[video_id] = data
                results.append((category, video_url, video_id, data))
                # Update progress only for successfully fetched URLs
                if data and video_id:
                    with progress_lock:
                        processed_urls.add(video_url)
                        save_progress(progress_path, processed_urls)
        
        # Optionally: do something with `results` here (save/upload)
        success_count = sum(1 for _c,_u,_vid,d in results if d)
        elapsed = time.perf_counter() - batch_start
        quota_after = get_quota_used()
        quota_used_in_batch = quota_after - quota_before
        
        # Итоговая информация по батчу
        key_num, total_keys = get_current_key_info()
        print(f"Batch {batch_num}: всего видео: {len(batch)}, успешно: {success_count}, квота: {quota_used_in_batch}, время: {elapsed:.2f}s, ключ: {key_num}/{total_keys}")

        # Проверка на пустые батчи (возможно, квота исчерпана)
        if success_count == 0:
            empty_batches_count += 1
            if empty_batches_count >= MAX_EMPTY_BATCHES:
                if are_all_keys_exhausted():
                    print(f"⚠️  Все {total_keys} API ключа исчерпаны! Остановка обработки.")
                    break
                else:
                    print(f"⚠️  {empty_batches_count} пустых батчей подряд. Принудительная ротация ключа...")
                    if force_switch_key():
                        key_num, total_keys = get_current_key_info()
                        print(f"✅ Переключено на ключ {key_num}/{total_keys}")
                        empty_batches_count = 0  # Сбрасываем счетчик после ротации
                    else:
                        print(f"❌ Не удалось переключить ключ. Все ключи исчерпаны.")
                        break
        else:
            empty_batches_count = 0  # Сбрасываем счетчик при успешном батче

        # Save per-batch file into resurrets/yt_api (skip if batch is empty)
        root_dir = os.path.dirname(os.path.dirname(__file__))
        per_batch_dir = os.path.join(root_dir, "_yt_api", ".results")
        os.makedirs(per_batch_dir, exist_ok=True)
        if success_count > 0:
            batch_videos: Dict[str, Any] = {vid: data for (_c,_u,vid,data) in results if data and vid}
            per_batch_payload: Dict[str, Any] = {
                "batch": batch_num,
                "size": len(batch),
                "success": success_count,
                "durationSec": round(elapsed, 3),
                "quotaUsed": int(quota_used_in_batch),
                "videos": batch_videos,
            }
            per_batch_path = os.path.join(per_batch_dir, f"batch_{batch_num}.json")
            # Safety: if a file with this batch number already exists (race or legacy mixes), bump the number
            while os.path.exists(per_batch_path):
                batch_num += 1
                per_batch_payload["batch"] = batch_num
                per_batch_path = os.path.join(per_batch_dir, f"batch_{batch_num}.json")
            with open(per_batch_path, "w", encoding="utf-8") as bf:
                json.dump(per_batch_payload, bf, ensure_ascii=False, indent=2)
            batches_index.append({
                "batch": batch_num,
                "size": len(batch),
                "success": success_count,
                "durationSec": round(elapsed, 3)
            })

    # Save combined JSON keyed by video_id into results directory
    output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".results")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "yt_api_aggregate.json")
    payload: Dict[str, Any] = {"_batches": batches_index}
    payload.update(combined_results)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()