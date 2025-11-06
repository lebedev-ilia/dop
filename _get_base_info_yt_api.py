from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import json
import ssl
import threading
import os
from typing import Optional, List, Dict, Any


# List of YouTube API keys for rotation
# You can also set YOUTUBE_API_KEYS environment variable (comma-separated)
YOUTUBE_API_KEYS = []

# Thread-safe API key rotation state
_key_rotation_lock = threading.Lock()
_current_key_index = 0
_exhausted_keys = set()
_quota_used_by_key = {}  # key_index -> quota_used
quota_used = 0


def _load_api_keys() -> List[str]:
    """Load API keys from environment variable or use default list."""
    env_keys = os.getenv('YOUTUBE_API_KEYS')
    if env_keys:
        # Support comma-separated or space-separated keys
        if ',' in env_keys:
            keys = [k.strip() for k in env_keys.split(',') if k.strip()]
        else:
            keys = [k.strip() for k in env_keys.split() if k.strip()]
        if keys:
            return keys
    return YOUTUBE_API_KEYS.copy()


def _get_api_keys() -> List[str]:
    """Get the list of API keys (lazy initialization)."""
    if not hasattr(_get_api_keys, '_cached_keys'):
        _get_api_keys._cached_keys = _load_api_keys()
    return _get_api_keys._cached_keys


def _is_quota_error(error: Exception) -> bool:
    """
    Check if the error is a quota-related error.
    
    Args:
        error: Exception to check
        
    Returns:
        True if this is a quota error
    """
    if isinstance(error, HttpError):
        error_code = error.resp.status if hasattr(error.resp, 'status') else None
        error_message = str(error)
        
        # Quota errors: 429 (Too Many Requests), 403 (Forbidden), or messages about quota
        if error_code in [429, 403]:
            return True
        if 'quota' in error_message.lower() or 'exceeded' in error_message.lower():
            return True
    
    return False


def _switch_to_next_key() -> bool:
    """
    Switch to the next available API key (thread-safe).
    
    Returns:
        True if successfully switched, False if all keys are exhausted
    """
    global _current_key_index, _exhausted_keys
    
    with _key_rotation_lock:
        api_keys = _get_api_keys()
        
        # Mark current key as exhausted
        _exhausted_keys.add(_current_key_index)
        
        # Find next available key
        for i in range(len(api_keys)):
            if i not in _exhausted_keys:
                _current_key_index = i
                return True
        
        # All keys exhausted
        return False


def get_youtube_service():
    """
    Build a fresh service instance per thread using the current API key.
    Thread-safe: uses the current key index which is protected by locks.
    """
    with _key_rotation_lock:
        api_keys = _get_api_keys()
        if not api_keys:
            raise RuntimeError("No API keys available")
        
        if _current_key_index >= len(api_keys):
            raise RuntimeError("All API keys are exhausted")
        
        current_key = api_keys[_current_key_index]
    
    return build('youtube', 'v3', developerKey=current_key)


def _consume_quota(units: int, action: str = ""):
    """
    Увеличивает счетчик использованной квоты и логирует использование.
    Thread-safe: tracks quota per key.
    
    Args:
        units: Количество единиц квоты для добавления
        action: Название действия для логирования (опционально)
    """
    global quota_used
    
    with _key_rotation_lock:
        quota_used += units
        if _current_key_index not in _quota_used_by_key:
            _quota_used_by_key[_current_key_index] = 0
        _quota_used_by_key[_current_key_index] += units
        
        # Capture values for logging outside the lock (no print, just track)
        pass


def _fetch_top_comments(video_id: str, service: any, max_count: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch up to `max_count` most relevant top-level comments for a video.
    Captures likeCount, replyCount, publishedAt, and author display name (+optional channel id).
    """
    comments: List[Dict[str, Any]] = []
    if max_count <= 0:
        return comments

    page_token = None
    while len(comments) < max_count:
        # Include replies in part to get quick access to first few if present
        req = service.commentThreads().list(
            part='snippet,replies',
            videoId=video_id,
            order='relevance',
            maxResults=min(100, max_count - len(comments)),
            pageToken=page_token
        )
        resp = req.execute()
        _consume_quota(1, "commentThreads.list")
        items = resp.get('items', [])
        for it in items:
            sn = it.get('snippet', {})
            top = sn.get('topLevelComment', {}).get('snippet', {})
            comments.append({
                'text': top.get('textDisplay'),
                'likeCount': int(top.get('likeCount', 0)) if top.get('likeCount') is not None else 0,
                'replyCount': int(sn.get('totalReplyCount', 0)) if sn.get('totalReplyCount') is not None else 0,
                'publishedAt': top.get('publishedAt'),
                'author': top.get('authorDisplayName'),
                'authorChannelId': (top.get('authorChannelId') or {}).get('value') if isinstance(top.get('authorChannelId'), dict) else None,
            })
            if len(comments) >= max_count:
                break

            # If there are replies, fetch them too until we reach max_count
            total_replies = int(sn.get('totalReplyCount', 0) or 0)
            if total_replies > 0 and len(comments) < max_count:
                parent_id = (it.get('snippet', {})
                               .get('topLevelComment', {})
                               .get('id'))
                page_token_repl = None
                while len(comments) < max_count and parent_id:
                    repl_req = service.comments().list(
                        part='snippet',
                        parentId=parent_id,
                        maxResults=min(100, max_count - len(comments)),
                        pageToken=page_token_repl
                    )
                    repl_resp = repl_req.execute()
                    _consume_quota(1, "comments.list")
                    repl_items = repl_resp.get('items', [])
                    for r in repl_items:
                        rs = r.get('snippet', {})
                        comments.append({
                            'text': rs.get('textDisplay'),
                            'likeCount': int(rs.get('likeCount', 0)) if rs.get('likeCount') is not None else 0,
                            'replyCount': 0,
                            'publishedAt': rs.get('publishedAt'),
                            'author': rs.get('authorDisplayName'),
                            'authorChannelId': (rs.get('authorChannelId') or {}).get('value') if isinstance(rs.get('authorChannelId'), dict) else None,
                        })
                        if len(comments) >= max_count:
                            break
                    page_token_repl = repl_resp.get('nextPageToken')
                    if not page_token_repl or not repl_items:
                        break
        page_token = resp.get('nextPageToken')
        if not page_token or not items:
            break
    return comments


def fetch_from_youtube_api(video_id: str, service: Optional[any] = None, retries: int = 2, backoff_sec: float = 0.5):
    """
    Fetch video data from YouTube API with automatic API key rotation on quota errors.
    
    Args:
        video_id: YouTube video ID
        service: Optional pre-built YouTube service (if None, will create one)
        retries: Number of retries for non-quota errors
        backoff_sec: Initial backoff time in seconds
        
    Returns:
        Dictionary with video data or empty dict on error
    """
    result = {}
    timings = {
        "extract_info_seconds": None,
        "extract_comments_seconds": None,
    }
    total_start = time.perf_counter()
    
    try:
        if service is None:
            service = get_youtube_service()
        
        # Получаем основную информацию о видео
        video_response = service.videos().list(
            part='snippet,contentDetails,status,statistics,topicDetails,'
                    'recordingDetails,liveStreamingDetails',
            id=video_id
        ).execute()
        _consume_quota(1, "videos.list")
        
        if not video_response.get('items'):
            return {}
        
        video_item = video_response['items'][0]
        snippet = video_item.get('snippet', {})
        content_details = video_item.get('contentDetails', {})
        status = video_item.get('status', {})
        statistics = video_item.get('statistics', {})
        # Базовые поля
        result['videoId'] = video_id
        result['title'] = snippet.get('title')
        result['description'] = snippet.get('description')
        result['tags'] = snippet.get('tags', [])
        result['channelId'] = snippet.get('channelId')
        result['channelTitle'] = snippet.get('channelTitle')
        result['publishedAt'] = snippet.get('publishedAt')
        result['language'] = snippet.get('defaultLanguage')
        
        # Content Details
        duration_iso = content_details.get('duration')
        result['duration'] = duration_iso
        
        # Statistics
        result['viewCount'] = int(statistics.get('viewCount', 0)) if statistics.get('viewCount') else None
        result['likeCount'] = int(statistics.get('likeCount', 0)) if statistics.get('likeCount') else None
        result['commentCount'] = int(statistics.get('commentCount', 0)) if statistics.get('commentCount') else None
        
        result['madeForKids'] = status.get('madeForKids')
        
        # Thumbnails - только standard
        thumbnails = snippet.get('thumbnails', {})
        if thumbnails and 'standard' in thumbnails:
            result['thumbnails'] = {'standard': thumbnails['standard']}
        else:
            result['thumbnails'] = {}
        
        # Информация о канале
        if result.get('channelId'):
            try:
                channel_response = service.channels().list(
                    part='snippet,statistics,brandingSettings',
                    id=result['channelId']
                ).execute()
                _consume_quota(1, "channels.list")
                
                if channel_response.get('items'):
                    channel_item = channel_response['items'][0]
                    channel_stats = channel_item.get('statistics', {})
                    channel_snippet = channel_item.get('snippet', {})
                    branding = channel_item.get('brandingSettings', {})
                    image = branding.get('image', {})
                    
                    result['subscriberCount'] = int(channel_stats.get('subscriberCount', 0)) if channel_stats.get('subscriberCount') else None
                    result['videoCount'] = int(channel_stats.get('videoCount', 0)) if channel_stats.get('videoCount') else None
                    result['viewCount_channel'] = int(channel_stats.get('viewCount', 0)) if channel_stats.get('viewCount') else None
                    result['country'] = channel_snippet.get('country')
                
            except HttpError as e:
                # If quota error on channel fetch, try to switch key and retry once
                if _is_quota_error(e):
                    if _switch_to_next_key():
                        try:
                            service = get_youtube_service()
                            channel_response = service.channels().list(
                                part='snippet,statistics,brandingSettings',
                                id=result['channelId']
                            ).execute()
                            _consume_quota(1, "channels.list")
                            
                            if channel_response.get('items'):
                                channel_item = channel_response['items'][0]
                                channel_stats = channel_item.get('statistics', {})
                                channel_snippet = channel_item.get('snippet', {})
                                branding = channel_item.get('brandingSettings', {})
                                
                                result['subscriberCount'] = int(channel_stats.get('subscriberCount', 0)) if channel_stats.get('subscriberCount') else None
                                result['videoCount'] = int(channel_stats.get('videoCount', 0)) if channel_stats.get('videoCount') else None
                                result['viewCount_channel'] = int(channel_stats.get('viewCount', 0)) if channel_stats.get('viewCount') else None
                                result['country'] = channel_snippet.get('country')
                        except Exception:
                            pass  # Channel data is optional, continue without it
            except Exception:
                pass  # Channel data is optional, continue without it

        timings['extract_info_seconds'] = round(time.perf_counter() - total_start, 3)
        total_start = time.perf_counter()
        # Комментарии (топ релевантных)
        try:
            result['topComments'] = _fetch_top_comments(video_id, service, max_count=100)
        except HttpError as e:
            # If quota error on comments, try to switch key and retry once
            if _is_quota_error(e):
                if _switch_to_next_key():
                    try:
                        service = get_youtube_service()
                        result['topComments'] = _fetch_top_comments(video_id, service, max_count=100)
                    except Exception:
                        pass  # Comments are optional, continue without them
        except Exception:
            pass  # Comments are optional, continue without them
        
        timings['extract_comments_seconds'] = round(time.perf_counter() - total_start, 3)
        
    except HttpError as e:
        # Check if this is a quota error
        if _is_quota_error(e):
            if _switch_to_next_key():
                # Retry with new key (don't count as a regular retry)
                return fetch_from_youtube_api(video_id, service=None, retries=retries, backoff_sec=backoff_sec)
            else:
                # All keys exhausted - silent fail, will be counted as empty result
                return {}
        else:
            # Non-quota HTTP error, retry if retries available
            if retries > 0:
                time.sleep(backoff_sec)
                return fetch_from_youtube_api(video_id, service=None, retries=retries-1, backoff_sec=min(2.0, backoff_sec*2))
            return {}
            
    except (ssl.SSLError, Exception) as e:
        # Retry for transient SSL or other errors
        if retries > 0:
            time.sleep(backoff_sec)
            return fetch_from_youtube_api(video_id, service=None, retries=retries-1, backoff_sec=min(2.0, backoff_sec*2))
        return {}
    
    result['timings_youtube_api'] = timings
    return result

def save_to_json(result: dict, filepath: str):  
    with open(filepath, 'w') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


def get_quota_used() -> int:
    """Get the total quota used across all API keys."""
    global quota_used
    return quota_used


def force_switch_key() -> bool:
    """
    Force switch to next available API key (useful when detecting quota exhaustion).
    Returns True if switched, False if all keys exhausted.
    """
    return _switch_to_next_key()


def are_all_keys_exhausted() -> bool:
    """Check if all API keys are exhausted."""
    with _key_rotation_lock:
        api_keys = _get_api_keys()
        return len(_exhausted_keys) >= len(api_keys) if api_keys else True


def get_current_key_info() -> tuple[int, int]:
    """Get current key index and total keys count. Returns (current_index+1, total_count)."""
    with _key_rotation_lock:
        api_keys = _get_api_keys()
        return (_current_key_index + 1, len(api_keys))