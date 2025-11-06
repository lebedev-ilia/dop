import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from utils._huggingface_uploader import HuggingFaceUploader
import json
import os
from datetime import datetime
import time

def get_cat2ids():
    cat2ids_path = os.path.join(project_root, ".input", "cat2ids.json")
    with open(cat2ids_path, "r") as f:
        cat2ids = json.load(f)
    return cat2ids


def main():
    uploader = HuggingFaceUploader(
        repo_id="Ilialebedev/snapshots_yt_api",
    )
    
    YT_API_RESULTS_DIR = os.path.join(project_root, "_yt_api", ".results")
    # Порог на количество найденных batch-файлов для загрузки
    LOAD_BATCHES_TRESHOLD = 5
    # Минимум батчей для одного коммита (будут объединены в один commit)
    COMMIT_MIN_BATCHES = 10
    # Максимальное ожидание между коммитами (сек), чтобы не держать очередь бесконечно
    COMMIT_MAX_WAIT_SEC = 300
    # Ограничение на коммиты в час (лимит HF ~128/h, держим запас)
    MAX_COMMITS_PER_HOUR = 100
    
    cat2ids = get_cat2ids()
    date = datetime.now().strftime("%Y-%m-%d")
    meta_folder_name = f"meta_{date}_yt_api"
    
    success_batchs = []
    pending_batches = []  # имена batch_*.json к загрузке в одном коммите
    last_commit_ts = 0.0
    recent_commit_ts = []  # список Unix-меток последних коммитов (для лимита в час)
    
    while True:
        
        new_batchs = uploader.check_for_new_batches(success_batchs + pending_batches, YT_API_RESULTS_DIR)
        
        uloaded = 0
        
        now = time.time()
        # добавляем свежие батчи в очередь (если достигли порога обнаружения)
        if len(new_batchs) >= LOAD_BATCHES_TRESHOLD:
            pending_batches.extend(new_batchs)

        # чистим журнал коммитов старше часа
        recent_commit_ts = [t for t in recent_commit_ts if now - t < 3600]
        commits_left = MAX_COMMITS_PER_HOUR - len(recent_commit_ts)

        # Условия для одного объединённого коммита:
        #  - накопили минимум COMMIT_MIN_BATCHES
        #  - или ждем дольше COMMIT_MAX_WAIT_SEC с прошлого коммита
        #  - и у нас есть лимит на коммиты
        should_commit = (
            len(pending_batches) >= COMMIT_MIN_BATCHES or
            (pending_batches and (now - last_commit_ts >= COMMIT_MAX_WAIT_SEC))
        ) and commits_left > 0

        if should_commit:
            files_to_upload = []
            # Собираем все файлы из накопленных батчей в один коммит
            for batch_name in list(pending_batches):
                try:
                    with open(os.path.join(YT_API_RESULTS_DIR, batch_name), "r") as f:
                        batch_data = json.load(f)
                    for video_id, video_data in batch_data.get("videos", {}).items():
                        for cat, ids in cat2ids.items():
                            if video_id in ids:
                                hf_path = f"{cat}/{video_id}/{meta_folder_name}"
                                if hf_path in uploader.current_repo_files:
                                    continue
                                files_to_upload.append((video_data, cat, video_id, hf_path))
                except Exception:
                    # пропускаем проблемный батч, не выбиваем процесс
                    continue

            if files_to_upload:
                try:
                    uploaded, total = uploader.upload_metadata_batch(files_to_upload, meta_folder_name)
                    uloaded += uploaded
                    last_commit_ts = time.time()
                    recent_commit_ts.append(last_commit_ts)
                except Exception:
                    # Fallback: по одному, чтобы не терять прогресс
                    for video_data, cat, video_id, hf_path in files_to_upload:
                        try:
                            if uploader.upload_metadata(video_data, cat, video_id, folder_name=meta_folder_name):
                                uloaded += 1
                            time.sleep(0.2)
                        except Exception:
                            continue

            # помечаем батчи как успешно обработанные и обновляем кэш файлов
            success_batchs.extend(pending_batches)
            pending_batches.clear()
            uploader.update_current_repo_files()
                
        time.sleep(3)
        

if __name__ == "__main__":
    main()