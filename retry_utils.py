import time
from functools import wraps

def retry_with_backoff(max_retries=3, base_delay=1, max_delay=60):
    """Retry decorator with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        raise
                    
                    # Exponential backoff: 1s, 2s, 4s, 8s, etc.
                    delay = min(base_delay * (2 ** (retries - 1)), max_delay)
                    time.sleep(delay)
            return None
        return wrapper
    return decorator