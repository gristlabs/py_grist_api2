import functools
from functools import reduce
from time import sleep


def retry(num_attempts, exception_class, log):
    """
    Decorator which makes the given function retry
    up to num_attempts times each time it encounters
    an exception of type exception_class. Uses `log`
    to log warnings of failed attempts.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(num_attempts):
                try:
                    return func(*args, **kwargs)
                except exception_class as e:
                    if i == num_attempts - 1:
                        raise
                    else:
                        log.warn('Failed with error %r, trying again', e, extra={'stack': True})
                        sleep(1)

        return wrapper

    return decorator


def join_urls(*urls):
    """
    Join the given strings together, inserting / in between where necessary
    """
    return reduce(lambda url1, url2: url1.rstrip('/') + '/' + str(url2).lstrip('/'),
                  urls).rstrip('/')


def strip_prefix(string, prefix):
    if string.startswith(prefix):
        return string[len(prefix):]
    return string


UNSET = object()


def passed_kwargs(**kwargs):
    return {k: v for k, v in kwargs.items() if v is not UNSET}
