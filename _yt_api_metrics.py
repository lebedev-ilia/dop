"""
Prometheus metrics collector for yt_api batch results.

This module collects detailed metrics from yt_api batch JSON files and exposes them
in Prometheus format. Can be used as a standalone HTTP server or integrated into
other Prometheus exporters.
"""

import json
import os
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily

try:
    from huggingface_hub import HfApi
    HF_HUB_AVAILABLE = True
except ImportError:
    HF_HUB_AVAILABLE = False
    print("Warning: huggingface_hub is not installed. Install it with: pip install huggingface_hub")

# Определяем корень проекта относительно этого файла
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Основная директория результатов для yt_api. По умолчанию используем `_yt_api/.results`,
# если ее нет — fallback на `results/yt_api`, который создается в `_yt_api/main_yt_api.py`.
YT_API_RESULTS_DIR_PRIMARY = os.path.join(_project_root, "_yt_api", ".results")
YT_API_RESULTS_DIR_FALLBACK = os.path.join(_project_root, "results", "yt_api")


def _resolve_results_dir(preferred_dir: Optional[str] = None) -> str:
    """Определяет директорию результатов для чтения batch_*.json файлов."""
    if preferred_dir and os.path.isdir(preferred_dir):
        return preferred_dir
    if os.path.isdir(YT_API_RESULTS_DIR_PRIMARY):
        return YT_API_RESULTS_DIR_PRIMARY
    return YT_API_RESULTS_DIR_FALLBACK


class YtApiMetricsCollector:
    """Collector for yt_api metrics that can be registered with Prometheus."""

    def __init__(self, results_dir: Optional[str] = None, token = None):
        self.results_dir = _resolve_results_dir(results_dir)
        self.token = token
        self.repo_id = "Ilialebedev/yt-metrics"
        self.repo_type = "dataset"
        self._upload_thread = None
        self._stop_upload_thread = False
        
        # Запускаем загрузку при инициализации и настраиваем периодический запуск
        if self.token and HF_HUB_AVAILABLE:
            self._start_periodic_upload()

    def _start_periodic_upload(self):
        """Запускает периодическую загрузку метрик в HF (каждый час и при инициализации)."""
        def upload_loop():
            # Первая загрузка при инициализации
            self.upload_list_metrics_to_hf()
            
            # Затем каждые 3600 секунд (1 час)
            while not self._stop_upload_thread:
                time.sleep(3600)
                if not self._stop_upload_thread:
                    self.upload_list_metrics_to_hf()
        
        self._upload_thread = threading.Thread(target=upload_loop, daemon=True)
        self._upload_thread.start()
    
    def stop_periodic_upload(self):
        """Останавливает периодическую загрузку метрик."""
        self._stop_upload_thread = True
        if self._upload_thread:
            self._upload_thread.join(timeout=5)

    def upload_list_metrics_to_hf(self):
        """
        Выгружает все списки со значениями метрик в HuggingFace.
        Собирает все метрики и загружает их как JSON файл в репозиторий.
        """
        if not HF_HUB_AVAILABLE:
            print("Warning: huggingface_hub is not available. Cannot upload metrics.")
            return False
        
        if not self.token:
            print("Warning: HuggingFace token is not provided. Cannot upload metrics.")
            return False
        
        try:
            # Собираем все метрики
            self._collect_metrics()
            
            # Подготавливаем данные для загрузки
            metrics_data = {
                "timestamp": datetime.now().isoformat(),
                "batch_metrics": {
                    "batch_duration_seconds": self.batch_duration_seconds,
                    "batches_declared_sizes": self.batches_declared_sizes,
                    "batches_success_counts": self.batches_success_counts,
                    "batches_quota_used": self.batches_quota_used,
                },
                "video_metrics": {
                    "videos_total_count": self.videos_total_count,
                    "has_thumbnails_count": self.has_thumbnails_count,
                    "missing_thumbnails_count": self.missing_thumbnails_count,
                    "has_language_count": self.has_language_count,
                },
                "video_statistics": {
                    "view_count_values": self.view_count_values,
                    "like_count_values": self.like_count_values,
                    "comment_count_values": self.comment_count_values,
                },
                "channel_statistics": {
                    "subscriber_count_values": self.subscriber_count_values,
                    "video_count_values": self.video_count_values,
                    "view_count_channel_values": self.view_count_channel_values,
                },
                "timing_metrics": {
                    "extract_info_seconds": self.extract_info_seconds,
                    "extract_comments_seconds": self.extract_comments_seconds,
                },
                "comments_metrics": {
                    "comments_total_count": self.comments_total_count,
                    "comments_with_text_count": self.comments_with_text_count,
                    "comments_empty_text_count": self.comments_empty_text_count,
                    "comment_text_lengths": self.comment_text_lengths,
                    "comment_like_counts": self.comment_like_counts,
                    "comment_reply_counts": self.comment_reply_counts,
                    "video_comment_entries_counts": self.video_comment_entries_counts,
                },
            }
            
            # Сериализуем в JSON
            json_content = json.dumps(metrics_data, ensure_ascii=False, indent=2, default=str)
            json_bytes = json_content.encode('utf-8')
            
            # Формируем имя файла с временной меткой
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename = f"yt_api_metrics_{timestamp}.json"
            path_in_repo = f"metrics/{filename}"
            
            # Загружаем в HuggingFace
            api = HfApi(token=self.token)
            api.upload_file(
                path_or_fileobj=json_bytes,
                path_in_repo=path_in_repo,
                repo_id=self.repo_id,
                repo_type=self.repo_type,
                commit_message=f"Upload yt_api metrics: {timestamp}"
            )
            
            print(f"✓ Successfully uploaded metrics to {self.repo_id}/{path_in_repo}")
            return True
            
        except Exception as e:
            print(f"✗ Error uploading metrics to HuggingFace: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _collect_metrics(self):
        """Collect all metrics from batch JSON files."""
        # Batch-level metrics
        self.batch_duration_seconds: List[float] = []
        self.batches_declared_sizes: List[int] = []
        self.batches_success_counts: List[int] = []
        self.batches_quota_used: List[float] = []

        # Video-level metrics
        self.videos_total_count: int = 0

        # Basic fields presence
        self.has_thumbnails_count = 0
        self.missing_thumbnails_count = 0
        self.has_language_count = 0

        # Statistics presence and values
        self.view_count_values: List[float] = []
        self.like_count_values: List[float] = []
        self.comment_count_values: List[float] = []

        # Channel stats presence and values
        self.subscriber_count_values: List[float] = []
        self.video_count_values: List[float] = []
        self.view_count_channel_values: List[float] = []

        # Timings (as exported by `_get_base_info_yt_api.fetch_from_youtube_api`)
        self.extract_info_seconds: List[float] = []
        self.extract_comments_seconds: List[float] = []

        # Comments-level metrics (from `topComments` list)
        self.comments_total_count: int = 0
        self.comments_with_text_count: int = 0
        self.comments_empty_text_count: int = 0
        self.comment_text_lengths: List[float] = []
        self.comment_like_counts: List[float] = []
        self.comment_reply_counts: List[float] = []
        self.video_comment_entries_counts: List[float] = []  # count of comment entries per video

        if not os.path.isdir(self.results_dir):
            return

        for file in os.listdir(self.results_dir):
            if file.startswith("batch_") and file.endswith(".json"):
                file_path = os.path.join(self.results_dir, file)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    # Batch-level metrics
                    if "durationSec" in data:
                        self.batch_duration_seconds.append(float(data["durationSec"]))
                    if "size" in data:
                        try:
                            self.batches_declared_sizes.append(int(data["size"]))
                        except Exception:
                            pass
                    if "success" in data:
                        try:
                            self.batches_success_counts.append(int(data["success"]))
                        except Exception:
                            pass
                    if "quotaUsed" in data:
                        try:
                            self.batches_quota_used.append(float(data["quotaUsed"]))
                        except Exception:
                            pass

                    # Video-level metrics
                    videos: Dict[str, Any] = data.get("videos", {})
                    for _vid, vd in videos.items():
                        if not isinstance(vd, dict):
                            continue
                        self.videos_total_count += 1

                        # Thumbnails presence: `_get_base_info_yt_api` puts thumbnails under key 'thumbnails'
                        thumbs = vd.get("thumbnails", {})
                        if isinstance(thumbs, dict) and thumbs:
                            self.has_thumbnails_count += 1
                        else:
                            self.missing_thumbnails_count += 1

                        # Language presence
                        if vd.get("language"):
                            self.has_language_count += 1

                        # Video statistics
                        if isinstance(vd.get("viewCount"), (int, float)):
                            self.view_count_values.append(float(vd["viewCount"]))
                        if isinstance(vd.get("likeCount"), (int, float)):
                            self.like_count_values.append(float(vd["likeCount"]))
                        if isinstance(vd.get("commentCount"), (int, float)):
                            self.comment_count_values.append(float(vd["commentCount"]))

                        # Channel stats
                        if isinstance(vd.get("subscriberCount"), (int, float)):
                            self.subscriber_count_values.append(float(vd["subscriberCount"]))
                        if isinstance(vd.get("videoCount"), (int, float)):
                            self.video_count_values.append(float(vd["videoCount"]))
                        if isinstance(vd.get("viewCount_channel"), (int, float)):
                            self.view_count_channel_values.append(float(vd["viewCount_channel"]))

                        # Timings
                        timings = vd.get("timings_youtube_api", {})
                        if isinstance(timings, dict):
                            if isinstance(timings.get("extract_info_seconds"), (int, float)):
                                self.extract_info_seconds.append(float(timings["extract_info_seconds"]))
                            if isinstance(timings.get("extract_comments_seconds"), (int, float)):
                                self.extract_comments_seconds.append(float(timings["extract_comments_seconds"]))

                        # Comments (topComments list)
                        top_comments = vd.get("topComments", [])
                        if isinstance(top_comments, list):
                            per_video_count = 0
                            for c in top_comments:
                                if not isinstance(c, dict):
                                    continue
                                per_video_count += 1
                                self.comments_total_count += 1
                                # text length and emptiness
                                text = c.get("text")
                                if text:
                                    self.comments_with_text_count += 1
                                    try:
                                        self.comment_text_lengths.append(float(len(text)))
                                    except Exception:
                                        pass
                                else:
                                    self.comments_empty_text_count += 1
                                # likeCount
                                lc = c.get("likeCount")
                                if isinstance(lc, (int, float)):
                                    self.comment_like_counts.append(float(lc))
                                # replyCount
                                rc = c.get("replyCount")
                                if isinstance(rc, (int, float)):
                                    self.comment_reply_counts.append(float(rc))
                            self.video_comment_entries_counts.append(float(per_video_count))

                except Exception as e:
                    # Log error but continue processing other files
                    print(f"Error processing {file_path}: {e}")
                    continue

    def collect(self):
        """Generate Prometheus metrics from collected data."""
        # Re-collect metrics on each scrape to get fresh data
        self._collect_metrics()

        # Helper to emit basic stats (min/max/mean/count) as gauges
        def emit_stats(metric_base: str, desc: str, values: List[float]):
            if not values:
                return
            vmin = min(values)
            vmax = max(values)
            vmean = sum(values) / len(values)
            stats = GaugeMetricFamily(
                f"{metric_base}",
                f"{desc} (min/max/mean)",
                labels=["stat"]
            )
            stats.add_metric(["min"], vmin)
            stats.add_metric(["max"], vmax)
            stats.add_metric(["mean"], vmean)
            yield stats
            yield GaugeMetricFamily(f"{metric_base}_count", f"Count of {desc}", len(values))

        # Batch duration stats
        yield from emit_stats("ytapi_batch_duration_seconds", "Batch processing duration (seconds)", self.batch_duration_seconds)
        # Batch quota usage stats
        yield from emit_stats("ytapi_batch_quota_used", "Quota units used per batch", self.batches_quota_used)
        if self.batches_quota_used:
            yield GaugeMetricFamily(
                "ytapi_batch_quota_used_total",
                "Total quota units used across batches",
                sum(self.batches_quota_used)
            )

        # Video counts (gauges)
        yield GaugeMetricFamily(
            "ytapi_videos_total",
            "Total number of processed video entries across all batches",
            self.videos_total_count
        )
        if self.batches_declared_sizes:
            yield GaugeMetricFamily(
                "ytapi_videos_declared_total",
                "Sum of declared videos across all batches",
                sum(self.batches_declared_sizes)
            )
        if self.batches_success_counts:
            yield GaugeMetricFamily(
                "ytapi_videos_success_total",
                "Sum of successfully processed videos across all batches",
                sum(self.batches_success_counts)
            )

        # Thumbnails presence
        thumbs = CounterMetricFamily(
            "ytapi_thumbnails_entries_total",
            "Counts of videos by thumbnails presence",
            labels=["presence"]
        )
        thumbs.add_metric(["present"], self.has_thumbnails_count)
        thumbs.add_metric(["missing"], self.missing_thumbnails_count)
        yield thumbs

        # Language presence
        yield GaugeMetricFamily(
            "ytapi_language_present_total",
            "Number of videos with language set",
            self.has_language_count
        )

        # Video statistics
        yield from emit_stats("ytapi_video_view_count", "Video viewCount values", self.view_count_values)
        yield from emit_stats("ytapi_video_like_count", "Video likeCount values", self.like_count_values)
        yield from emit_stats("ytapi_video_comment_count", "Video commentCount values", self.comment_count_values)

        # Channel statistics
        yield from emit_stats("ytapi_channel_subscriber_count", "Channel subscriberCount values", self.subscriber_count_values)
        yield from emit_stats("ytapi_channel_video_count", "Channel videoCount values", self.video_count_values)
        yield from emit_stats("ytapi_channel_view_count", "Channel viewCount values", self.view_count_channel_values)

        # Timings
        yield from emit_stats("ytapi_extract_info_seconds", "Time spent extracting video info (seconds)", self.extract_info_seconds)
        yield from emit_stats("ytapi_extract_comments_seconds", "Time spent fetching comments (seconds)", self.extract_comments_seconds)

        # Comments totals
        comments_total = CounterMetricFamily(
            "ytapi_comments_total",
            "Total number of comment entries across all videos"
        )
        comments_total.add_metric([], self.comments_total_count)
        yield comments_total

        comments_empty_total = CounterMetricFamily(
            "ytapi_comments_empty_text_total",
            "Number of comments with empty or missing text"
        )
        comments_empty_total.add_metric([], self.comments_empty_text_count)
        yield comments_empty_total

        # Comment text length stats
        yield from emit_stats(
            "ytapi_comment_text_length_characters",
            "Comment text length (characters)",
            self.comment_text_lengths
        )

        # Comment like/reply count stats
        yield from emit_stats(
            "ytapi_comment_like_count",
            "Per-comment likeCount values",
            self.comment_like_counts
        )
        yield from emit_stats(
            "ytapi_comment_reply_count",
            "Per-comment replyCount values",
            self.comment_reply_counts
        )

        # Per-video comment entries count distribution
        yield from emit_stats(
            "ytapi_video_comment_entries",
            "Number of comment entries per video",
            self.video_comment_entries_counts
        )


def get_metrics_registry(results_dir: Optional[str] = None, token = None) -> CollectorRegistry:
    """Create a Prometheus registry with yt_api metrics."""
    registry = CollectorRegistry()
    collector = YtApiMetricsCollector(results_dir, token)
    registry.register(collector)
    return registry


def generate_metrics_text(results_dir: Optional[str] = None) -> str:
    """Generate Prometheus metrics text format."""
    registry = get_metrics_registry(results_dir)
    return generate_latest(registry).decode('utf-8')


if __name__ == "__main__":
    # Can be used as standalone HTTP server
    import argparse

    default_dir = _resolve_results_dir()
    parser = argparse.ArgumentParser(description="Prometheus metrics exporter for yt_api results")
    parser.add_argument("--port", type=int, default=8002, help="Port to listen on (default: 8002)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--results-dir", type=str, default=default_dir,
                       help=f"Directory containing batch_*.json files (default: {default_dir})")
    parser.add_argument("--token", type=str, help="HuggingFace token for uploading metrics")
    args = parser.parse_args()

    registry = get_metrics_registry(args.results_dir, args.token)

    print(f"Starting Prometheus metrics server for yt_api results...")
    print(f"  Host: {args.host}")
    print(f"  Port: {args.port}")
    print(f"  Results directory: {args.results_dir}")
    print(f"  Metrics endpoint: http://{args.host}:{args.port}/metrics")
    print(f"\nServer running. Press Ctrl+C to stop.")

    from prometheus_client import make_wsgi_app
    from wsgiref.simple_server import make_server

    app = make_wsgi_app(registry=registry)
    httpd = make_server(args.host, args.port, app)
    httpd.serve_forever()


