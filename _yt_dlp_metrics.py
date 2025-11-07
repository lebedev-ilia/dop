"""
Prometheus metrics collector for yt_dlp batch results.

This module collects detailed metrics from yt_dlp batch JSON files and exposes them
in Prometheus format. Can be used as a standalone HTTP server or integrated into
other Prometheus exporters.
"""

import json
import os
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional
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
YT_DLP_RESULTS_DIR = os.path.join(_project_root, "_yt_dlp", ".results")


class YtDlpMetricsCollector:
    """Collector for yt_dlp metrics that can be registered with Prometheus."""
    
    def __init__(self, results_dir: str = YT_DLP_RESULTS_DIR, token = None):
        self.results_dir = results_dir
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
                },
                "video_metrics": {
                    "videos_total_count": self.videos_total_count,
                    "age_limit": self.age_limit,
                    "duration_seconds": self.duration_seconds,
                },
                "subtitles_metrics": {
                    "subtitles_ru_len": self.subtitles_ru_len,
                    "subtitles_en_len": self.subtitles_en_len,
                    "subtitles_ru_count": self.subtitles_ru_count,
                    "subtitles_en_count": self.subtitles_en_count,
                    "empty_subtitles_ru_count": self.empty_subtitles_ru_count,
                    "empty_subtitles_en_count": self.empty_subtitles_en_count,
                },
                "automatic_captions_metrics": {
                    "automatic_captions_ru_len": self.automatic_captions_ru_len,
                    "automatic_captions_en_len": self.automatic_captions_en_len,
                    "automatic_captions_ru_count": self.automatic_captions_ru_count,
                    "automatic_captions_en_count": self.automatic_captions_en_count,
                    "empty_automatic_captions_ru_count": self.empty_automatic_captions_ru_count,
                    "empty_automatic_captions_en_count": self.empty_automatic_captions_en_count,
                },
                "chapters_metrics": {
                    "chapters_count": self.chapters_count,
                    "videos_with_chapters": self.videos_with_chapters,
                    "videos_without_chapters": self.videos_without_chapters,
                },
                "formats_metrics": {
                    "formats_count": self.formats_count,
                    "videos_with_formats": self.videos_with_formats,
                    "videos_without_formats": self.videos_without_formats,
                    "resolution_counts": self.resolution_counts,
                },
                "thumbnails_metrics": {
                    "thumbnails_count": self.thumbnails_count,
                    "videos_with_thumbnails": self.videos_with_thumbnails,
                    "videos_without_thumbnails": self.videos_without_thumbnails,
                },
                "timing_metrics": {
                    "extract_info_seconds": self.extract_info_seconds,
                    "captions_seconds_total": self.captions_seconds_total,
                    "total_seconds": self.total_seconds,
                },
            }
            
            # Сериализуем в JSON
            json_content = json.dumps(metrics_data, ensure_ascii=False, indent=2, default=str)
            json_bytes = json_content.encode('utf-8')
            
            # Формируем имя файла с временной меткой
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename = f"yt_dlp_metrics_{timestamp}.json"
            path_in_repo = f"metrics/{filename}"
            
            # Загружаем в HuggingFace
            api = HfApi(token=self.token)
            api.upload_file(
                path_or_fileobj=json_bytes,
                path_in_repo=path_in_repo,
                repo_id=self.repo_id,
                repo_type=self.repo_type,
                commit_message=f"Upload yt_dlp metrics: {timestamp}"
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
        
        # Video-level metrics
        self.videos_total_count: int = 0
        self.age_limit: List[int] = []
        self.subtitles_ru_len: List[int] = []
        self.subtitles_en_len: List[int] = []
        self.subtitles_ru_count = 0
        self.subtitles_en_count = 0
        self.empty_subtitles_ru_count = 0
        self.empty_subtitles_en_count = 0
        
        self.automatic_captions_ru_len: List[int] = []
        self.automatic_captions_en_len: List[int] = []
        self.automatic_captions_ru_count = 0
        self.automatic_captions_en_count = 0
        self.empty_automatic_captions_ru_count = 0
        self.empty_automatic_captions_en_count = 0
        
        self.chapters_count = 0
        self.videos_with_chapters = 0
        self.videos_without_chapters = 0
        
        self.formats_count = 0
        self.videos_with_formats = 0
        self.videos_without_formats = 0
        self.resolution_counts: Dict[str, int] = {}
        
        self.thumbnails_count = 0
        self.videos_with_thumbnails = 0
        self.videos_without_thumbnails = 0
        
        self.duration_seconds: List[float] = []
        self.extract_info_seconds: List[float] = []
        self.captions_seconds_total: List[float] = []
        self.total_seconds: List[float] = []
        
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
                    
                    # Video-level metrics
                    videos = data.get("videos", {})
                    for vid, vd in videos.items():
                        if not isinstance(vd, dict):
                            continue
                        self.videos_total_count += 1
                        
                        # Age limit
                        if "age_limit" in vd:
                            age_limit = vd["age_limit"]
                            if isinstance(age_limit, (int, float)):
                                self.age_limit.append(int(age_limit))
                        
                        # Subtitles
                        subtitles = vd.get("subtitles", {})
                        if isinstance(subtitles, dict):
                            for lang, subtitle_text in subtitles.items():
                                if lang == "ru":
                                    self.subtitles_ru_count += 1
                                    if subtitle_text:
                                        self.subtitles_ru_len.append(len(subtitle_text))
                                    else:
                                        self.empty_subtitles_ru_count += 1
                                elif lang == "en":
                                    self.subtitles_en_count += 1
                                    if subtitle_text:
                                        self.subtitles_en_len.append(len(subtitle_text))
                                    else:
                                        self.empty_subtitles_en_count += 1
                        
                        # Automatic captions
                        automatic_captions = vd.get("automatic_captions", {})
                        if isinstance(automatic_captions, dict):
                            for lang, caption_text in automatic_captions.items():
                                if lang == "ru":
                                    self.automatic_captions_ru_count += 1
                                    if caption_text:
                                        self.automatic_captions_ru_len.append(len(caption_text))
                                    else:
                                        self.empty_automatic_captions_ru_count += 1
                                elif lang == "en":
                                    self.automatic_captions_en_count += 1
                                    if caption_text:
                                        self.automatic_captions_en_len.append(len(caption_text))
                                    else:
                                        self.empty_automatic_captions_en_count += 1
                        
                        # Chapters
                        chapters = vd.get("chapters")
                        if chapters:
                            if isinstance(chapters, list):
                                self.chapters_count += len(chapters)
                                self.videos_with_chapters += 1
                            else:
                                self.videos_without_chapters += 1
                        else:
                            self.videos_without_chapters += 1
                        
                        # Formats
                        formats = vd.get("formats", [])
                        if formats and isinstance(formats, list):
                            self.formats_count += len(formats)
                            self.videos_with_formats += 1
                            for fmt in formats:
                                if isinstance(fmt, dict) and "resolution" in fmt:
                                    resolution = str(fmt["resolution"])
                                    self.resolution_counts[resolution] = self.resolution_counts.get(resolution, 0) + 1
                        else:
                            self.videos_without_formats += 1
                        
                        # Thumbnails
                        thumbnails = vd.get("thumbnails_ytdlp", vd.get("thumbnails", []))
                        if thumbnails and isinstance(thumbnails, list):
                            self.thumbnails_count += len(thumbnails)
                            self.videos_with_thumbnails += 1
                        else:
                            self.videos_without_thumbnails += 1
                        
                        # Duration
                        if "duration_seconds" in vd:
                            dur_sec = vd["duration_seconds"]
                            if isinstance(dur_sec, (int, float)):
                                self.duration_seconds.append(float(dur_sec))
                        
                        # Timings
                        timings = vd.get("timings_ytdlp", {})
                        if isinstance(timings, dict):
                            if "extract_info_seconds" in timings:
                                val = timings["extract_info_seconds"]
                                if isinstance(val, (int, float)):
                                    self.extract_info_seconds.append(float(val))
                            
                            if "captions_seconds_total" in timings:
                                val = timings["captions_seconds_total"]
                                if isinstance(val, (int, float)):
                                    self.captions_seconds_total.append(float(val))
                            
                            if "total_seconds" in timings:
                                val = timings["total_seconds"]
                                if isinstance(val, (int, float)):
                                    self.total_seconds.append(float(val))
                
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
        yield from emit_stats("ytdlp_batch_duration_seconds", "Batch processing duration (seconds)", self.batch_duration_seconds)

        # Video counts (gauges)
        yield GaugeMetricFamily(
            "ytdlp_videos_total",
            "Total number of processed video entries across all batches",
            self.videos_total_count
        )
        if self.batches_declared_sizes:
            yield GaugeMetricFamily(
                "ytdlp_videos_declared_total",
                "Sum of declared videos across all batches",
                sum(self.batches_declared_sizes)
            )
        if self.batches_success_counts:
            yield GaugeMetricFamily(
                "ytdlp_videos_success_total",
                "Sum of successfully processed videos across all batches",
                sum(self.batches_success_counts)
            )
        
        # Age limit stats
        yield from emit_stats("ytdlp_video_age_limit", "Video age_limit values", [float(v) for v in self.age_limit])
        
        # Subtitles metrics
        subtitles_total = CounterMetricFamily(
            "ytdlp_subtitles_total",
            "Total number of subtitle entries",
            labels=["language"]
        )
        subtitles_total.add_metric(["ru"], self.subtitles_ru_count)
        subtitles_total.add_metric(["en"], self.subtitles_en_count)
        yield subtitles_total

        subtitles_empty_total = CounterMetricFamily(
            "ytdlp_subtitles_empty_total",
            "Number of empty subtitle entries",
            labels=["language"]
        )
        subtitles_empty_total.add_metric(["ru"], self.empty_subtitles_ru_count)
        subtitles_empty_total.add_metric(["en"], self.empty_subtitles_en_count)
        yield subtitles_empty_total
        
        # Subtitles length stats and counts per language
        subtitles_stats = None
        subtitles_count = None
        if self.subtitles_ru_len or self.subtitles_en_len:
            subtitles_stats = GaugeMetricFamily(
                "ytdlp_subtitles_length_characters",
                "Length of subtitle text in characters (min/max/mean)",
                labels=["language", "stat"]
            )
            subtitles_count = GaugeMetricFamily(
                "ytdlp_subtitles_length_characters_count",
                "Count of subtitles entries with text",
                labels=["language"]
            )
        if self.subtitles_ru_len:
            v = self.subtitles_ru_len
            subtitles_stats.add_metric(["ru", "min"], min(v))
            subtitles_stats.add_metric(["ru", "max"], max(v))
            subtitles_stats.add_metric(["ru", "mean"], sum(v)/len(v))
            subtitles_count.add_metric(["ru"], len(v))
        if self.subtitles_en_len:
            v = self.subtitles_en_len
            subtitles_stats.add_metric(["en", "min"], min(v))
            subtitles_stats.add_metric(["en", "max"], max(v))
            subtitles_stats.add_metric(["en", "mean"], sum(v)/len(v))
            subtitles_count.add_metric(["en"], len(v))
        if subtitles_stats is not None:
            yield subtitles_stats
            yield subtitles_count
        
        # Automatic captions metrics
        auto_caps_total = CounterMetricFamily(
            "ytdlp_automatic_captions_total",
            "Total number of automatic caption entries",
            labels=["language"]
        )
        auto_caps_total.add_metric(["ru"], self.automatic_captions_ru_count)
        auto_caps_total.add_metric(["en"], self.automatic_captions_en_count)
        yield auto_caps_total

        auto_caps_empty_total = CounterMetricFamily(
            "ytdlp_automatic_captions_empty_total",
            "Number of empty automatic caption entries",
            labels=["language"]
        )
        auto_caps_empty_total.add_metric(["ru"], self.empty_automatic_captions_ru_count)
        auto_caps_empty_total.add_metric(["en"], self.empty_automatic_captions_en_count)
        yield auto_caps_empty_total
        
        # Automatic captions length stats and counts per language
        auto_stats = None
        auto_count = None
        if self.automatic_captions_ru_len or self.automatic_captions_en_len:
            auto_stats = GaugeMetricFamily(
                "ytdlp_automatic_captions_length_characters",
                "Length of automatic caption text in characters (min/max/mean)",
                labels=["language", "stat"]
            )
            auto_count = GaugeMetricFamily(
                "ytdlp_automatic_captions_length_characters_count",
                "Count of automatic captions entries with text",
                labels=["language"]
            )
        if self.automatic_captions_ru_len:
            v = self.automatic_captions_ru_len
            auto_stats.add_metric(["ru", "min"], min(v))
            auto_stats.add_metric(["ru", "max"], max(v))
            auto_stats.add_metric(["ru", "mean"], sum(v)/len(v))
            auto_count.add_metric(["ru"], len(v))
        if self.automatic_captions_en_len:
            v = self.automatic_captions_en_len
            auto_stats.add_metric(["en", "min"], min(v))
            auto_stats.add_metric(["en", "max"], max(v))
            auto_stats.add_metric(["en", "mean"], sum(v)/len(v))
            auto_count.add_metric(["en"], len(v))
        if auto_stats is not None:
            yield auto_stats
            yield auto_count
        
        # Chapters metrics
        chapters_total = CounterMetricFamily(
            "ytdlp_chapters_total",
            "Total number of chapters across all videos"
        )
        chapters_total.add_metric([], self.chapters_count)
        yield chapters_total

        videos_with_chapters = CounterMetricFamily(
            "ytdlp_videos_with_chapters_total",
            "Number of videos with chapters"
        )
        videos_with_chapters.add_metric([], self.videos_with_chapters)
        yield videos_with_chapters

        videos_without_chapters = CounterMetricFamily(
            "ytdlp_videos_without_chapters_total",
            "Number of videos without chapters"
        )
        videos_without_chapters.add_metric([], self.videos_without_chapters)
        yield videos_without_chapters
        
        # Formats metrics
        formats_total = CounterMetricFamily(
            "ytdlp_formats_total",
            "Total number of format entries across all videos"
        )
        formats_total.add_metric([], self.formats_count)
        yield formats_total

        videos_with_formats = CounterMetricFamily(
            "ytdlp_videos_with_formats_total",
            "Number of videos with formats"
        )
        videos_with_formats.add_metric([], self.videos_with_formats)
        yield videos_with_formats

        videos_without_formats = CounterMetricFamily(
            "ytdlp_videos_without_formats_total",
            "Number of videos without formats"
        )
        videos_without_formats.add_metric([], self.videos_without_formats)
        yield videos_without_formats
        
        # Resolution distribution
        if self.resolution_counts:
            resolution_gauge = GaugeMetricFamily(
                "ytdlp_resolution_count",
                "Number of formats with specific resolution",
                labels=["resolution"]
            )
            for resolution, count in self.resolution_counts.items():
                resolution_gauge.add_metric([resolution], count)
            yield resolution_gauge
        
        # Thumbnails metrics
        thumbnails_total = CounterMetricFamily(
            "ytdlp_thumbnails_total",
            "Total number of thumbnail entries"
        )
        thumbnails_total.add_metric([], self.thumbnails_count)
        yield thumbnails_total

        videos_with_thumbnails = CounterMetricFamily(
            "ytdlp_videos_with_thumbnails_total",
            "Number of videos with thumbnails"
        )
        videos_with_thumbnails.add_metric([], self.videos_with_thumbnails)
        yield videos_with_thumbnails

        videos_without_thumbnails = CounterMetricFamily(
            "ytdlp_videos_without_thumbnails_total",
            "Number of videos without thumbnails"
        )
        videos_without_thumbnails.add_metric([], self.videos_without_thumbnails)
        yield videos_without_thumbnails
        
        # Video duration stats
        yield from emit_stats("ytdlp_video_duration_seconds", "Video duration (seconds)", self.duration_seconds)
        
        # Timing stats
        yield from emit_stats("ytdlp_extract_info_seconds", "Time spent extracting video info (seconds)", self.extract_info_seconds)
        yield from emit_stats("ytdlp_captions_seconds_total", "Total time spent fetching captions (seconds)", self.captions_seconds_total)
        yield from emit_stats("ytdlp_total_processing_seconds", "Total processing time per video (seconds)", self.total_seconds)


def get_metrics_registry(results_dir: Optional[str] = None, token = None) -> CollectorRegistry:
    """Create a Prometheus registry with yt_dlp metrics."""
    registry = CollectorRegistry()
    collector = YtDlpMetricsCollector(results_dir or YT_DLP_RESULTS_DIR, token)
    registry.register(collector)
    return registry


def generate_metrics_text(results_dir: Optional[str] = None) -> str:
    """Generate Prometheus metrics text format."""
    registry = get_metrics_registry(results_dir)
    return generate_latest(registry).decode('utf-8')


if __name__ == "__main__":
    # Can be used as standalone HTTP server
    import argparse
    
    parser = argparse.ArgumentParser(description="Prometheus metrics exporter for yt_dlp results")
    parser.add_argument("--port", type=int, default=8001, help="Port to listen on (default: 8001)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--results-dir", type=str, default=YT_DLP_RESULTS_DIR,
                       help=f"Directory containing batch_*.json files (default: {YT_DLP_RESULTS_DIR})")
    parser.add_argument("--token", type=str)
    args = parser.parse_args()
    
    registry = get_metrics_registry(args.results_dir, args.token)
    
    print(f"Starting Prometheus metrics server for yt_dlp results...")
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