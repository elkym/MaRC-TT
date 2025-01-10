import time
from memory_profiler import memory_usage
import psutil
import os
from functools import wraps
import tracemalloc

# Timing Decorator
def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function '{func.__name__}' executed in {end_time - start_time:.4f} seconds")
        return result
    return wrapper

# Memory Usage Decorator
def memory_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        mem_before = memory_usage()[0]
        result = func(*args, **kwargs)
        mem_after = memory_usage()[0]
        print(f"Function '{func.__name__}' used {mem_after - mem_before:.2f} MiB")
        return result
    return wrapper

# CPU Usage Decorator
def cpu_usage_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        cpu_before = process.cpu_percent(interval=None)
        result = func(*args, **kwargs)
        cpu_after = process.cpu_percent(interval=None)
        print(f"Function '{func.__name__}' CPU usage: {cpu_after - cpu_before:.2f}%")
        return result
    return wrapper

# I/O Operations Decorator
def io_operations_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        io_before = process.io_counters()
        result = func(*args, **kwargs)
        io_after = process.io_counters()
        print(f"Function '{func.__name__}' I/O operations: read_count={io_after.read_count - io_before.read_count}, write_count={io_after.write_count - io_before.write_count}")
        return result
    return wrapper

# Function Call Count Decorator
def call_count_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        wrapper.call_count += 1
        result = func(*args, **kwargs)
        print(f"Function '{func.__name__}' call count: {wrapper.call_count}")
        return result
    wrapper.call_count = 0
    return wrapper

# Exception Tracking Decorator
def exception_tracking_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Function '{func.__name__}' raised an exception: {e}")
            raise
    return wrapper

# Memory Leak Detection Decorator
def memory_leak_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        tracemalloc.start()
        result = func(*args, **kwargs)
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(f"Function '{func.__name__}' memory usage: current={current / 10**6:.2f}MB, peak={peak / 10**6:.2f}MB")
        return result
    return wrapper