import logging
import time


def retry(retries=3, backoff=20):
    """ Same as open source run.py """
    def wrapper(func):
        def wrapped(*args, **kwargs):
            attempt = 0
            while attempt <= retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    logging.exception(e)
                    sleep_time = attempt * backoff
                    logging.info(
                        "[{}] Retry: {} out of {}/ Sleeping for {}"
                        .format(func.__name__, attempt, retries, sleep_time))
                    time.sleep(sleep_time)
            return func(*args, **kwargs)
        return wrapped
    return wrapper
