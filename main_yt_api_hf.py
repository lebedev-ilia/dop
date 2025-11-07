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

def save_progress_upload(success_batchs):
    progress_path = os.path.join(project_root, "_yt_api", ".cache", "progress_upload.json")
    with open(progress_path, "w") as f:
        json.dump(success_batchs, f)

def load_progress_upload():
    progress_path = os.path.join(project_root, "_yt_api", ".cache", "progress_upload.json")
    if os.path.exists(progress_path):
        with open(progress_path, "r") as f:
            success_batchs = json.load(f)
        return success_batchs
    else:
        return []

def main():
    uploader = HuggingFaceUploader(
        repo_id="Ilialebedev/snap_test",
        token="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",
        cache_dir=os.path.join(project_root, "_yt_api", ".cache")
    )
    
    YT_API_RESULTS_DIR = os.path.join(project_root, "_yt_api", ".results")
    # Порог на количество найденных batch-файлов для загрузки
    LOAD_BATCHES_TRESHOLD = 5
    # Минимум батчей для одного коммита (будут объединены в один commit)
    COMMIT_MIN_BATCHES = 50
    COMMIT_MAX_BATCHES = 60
    # Максимальное ожидание между коммитами (сек), чтобы не держать очередь бесконечно
    COMMIT_MAX_WAIT_SEC = 300
    # Ограничение на коммиты в час (лимит HF ~128/h, держим запас)
    MAX_COMMITS_PER_HOUR = 100
    
    cat2ids = get_cat2ids()
    date = datetime.now().strftime("%Y-%m-%d")
    meta_folder_name = f"meta_{date}_yt_api"
    
    success_batchs = load_progress_upload()
    pending_batches = []  # имена batch_*.json к загрузке в одном коммите
    last_commit_ts = 0.0
    recent_commit_ts = []  # список Unix-меток последних коммитов (для лимита в час)
    
    while True:
        
        new_batchs = uploader.check_for_new_batches(success_batchs + pending_batches, YT_API_RESULTS_DIR)
        
        print(f"new batchs len: {len(new_batchs)}")

        uploaded = 0
        
        now = time.time()
        # добавляем свежие батчи в очередь (если достигли порога обнаружения)
        if len(new_batchs) >= LOAD_BATCHES_TRESHOLD:
            pending_batches.extend(new_batchs)

        # чистим журнал коммитов старше часа
        recent_commit_ts = [t for t in recent_commit_ts if now - t < 3600]
        commits_left = MAX_COMMITS_PER_HOUR - len(recent_commit_ts)

        should_commit = (
            len(pending_batches) >= COMMIT_MIN_BATCHES or
            (pending_batches and (now - last_commit_ts >= COMMIT_MAX_WAIT_SEC))
        ) and commits_left > 0

        if should_commit:
            # Разбиваем батчи на чанки по COMMIT_MAX_BATCHES, если их больше лимита
            batches_to_process = list(pending_batches)
            processed_batches = []
            
            # Обрабатываем батчи порциями по COMMIT_MAX_BATCHES
            while batches_to_process and commits_left > 0:
                # Берем максимум COMMIT_MAX_BATCHES батчей для текущего коммита
                current_batch_chunk = batches_to_process[:COMMIT_MAX_BATCHES]
                batches_to_process = batches_to_process[COMMIT_MAX_BATCHES:]
                
                files_to_upload = []
                # Собираем все файлы из текущего чанка батчей
                for batch_name in current_batch_chunk:
                    try:
                        with open(os.path.join(YT_API_RESULTS_DIR, batch_name), "r") as f:
                            batch_data = json.load(f)
                        for video_id, video_data in batch_data.get("videos", {}).items():
                            for cat, ids in cat2ids.items():
                                if video_id in ids:
                                    hf_path = f"{cat}/{video_id}/{meta_folder_name}"
                                    files_to_upload.append((video_data, cat, video_id, hf_path))
                    except Exception:
                        # пропускаем проблемный батч, не выбиваем процесс
                        continue

                if files_to_upload:
                    try:
                        uploaded, total = uploader.upload_metadata_batch(files_to_upload, meta_folder_name)
                        last_commit_ts = time.time()
                        recent_commit_ts.append(last_commit_ts)
                        uploader.update_repo_files_cache(len(files_to_upload))
                        print(f"Current repo files: {uploader.current_repo_files}")
                        # Помечаем батчи из текущего чанка как успешно обработанные
                        processed_batches.extend(current_batch_chunk)
                        print(f"Successfully uploaded chunk of {len(current_batch_chunk)} batches")
                    except Exception as e:
                        print(f"Upload error: {e}")
                        # В случае ошибки не помечаем батчи как обработанные, оставляем их для повторной попытки
                        break
                
                # Обновляем количество доступных коммитов
                recent_commit_ts = [t for t in recent_commit_ts if time.time() - t < 3600]
                commits_left = MAX_COMMITS_PER_HOUR - len(recent_commit_ts)
                
                # Небольшая задержка между коммитами, если есть еще батчи для обработки
                if batches_to_process and commits_left > 0:
                    time.sleep(1)

            # Обновляем списки: добавляем обработанные батчи в success_batchs, оставляем необработанные в pending_batches
            if processed_batches:
                success_batchs.extend(processed_batches)
                save_progress_upload(success_batchs)
                # Удаляем обработанные батчи из pending_batches
                pending_batches = [b for b in pending_batches if b not in processed_batches]
                
        time.sleep(3)
        

if __name__ == "__main__":
    main()