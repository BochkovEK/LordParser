import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)

def retry_on_failure(max_retries: int = 2, delay: int = 5):
    """Декоратор для повторных попыток при ошибках"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(f"❌ Failed after {max_retries} attempts: {e}")
                        raise
                    logger.warning(f"⚠️ Attempt {attempt + 1} failed, retrying in {delay}s...")
                    time.sleep(delay)
        return wrapper
    return decorator