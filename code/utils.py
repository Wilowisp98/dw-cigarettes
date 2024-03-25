import datetime as dt

def log_wrapper(func):
    def wrapper(*args, **kwargs):
        start = dt.datetime.now()
        print(f'<{start}> [{func.__name__}] Running with args {args} and kwargs {kwargs}')
        output = func(*args, **kwargs)
        end = dt.datetime.now()
        print(f'<{end}> [{func.__name__}] Finished executing. Duration: {end - start}')
        return output
    return wrapper