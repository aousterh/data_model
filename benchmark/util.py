#!/usr/bin/env python3
import time
import numpy as np
import os
import re
from resource import getrusage as resource_usage, RUSAGE_SELF
import subprocess
from time import time as timestamp

def unix_time(function, *args, **kwargs):
    '''Return `real`, `sys` and `user` elapsed time, like UNIX's command `time`
    You can calculate the amount of used CPU-time used by your
    function/callable by summing `user` and `sys`. `real` is just like the wall
    clock.
    Note that `sys` and `user`'s resolutions are limited by the resolution of
    the operating system's software clock (check `man 7 time` for more
    details).
    '''
    start_time, start_resources = timestamp(), resource_usage(RUSAGE_SELF)
    r = function(*args, **kwargs)
    end_resources, end_time = resource_usage(RUSAGE_SELF), timestamp()

    return {'return': r,
            'real': end_time - start_time,
            'sys': end_resources.ru_stime - start_resources.ru_stime,
            'user': end_resources.ru_utime - start_resources.ru_utime}

def unix_time_bash(cmd, stdout=subprocess.DEVNULL):
    '''Executes and times the command in bash.'''

    escaped_cmd = cmd.replace("'", "'\\''")
    completed = subprocess.run("bash -c 'time -p {}'".format(escaped_cmd),
                               shell=True, text=True, stdout=stdout,
                               stderr=subprocess.PIPE)

    m = re.match(r"real (.*)\nuser (.*)\nsys (.*)\n", completed.stderr)
    if m == None:
        print("Command threw error: " + completed.stderr)
        return {}

    return {'return': completed.stdout,
            'real': float(m.group(1)),
            'user': float(m.group(2)),
            'sys': float(m.group(3))
    }

def benchmark(fn, init_fn, *init_args, num_iter=10, **init_kwargs):
    '''Benchmarks fn a specified number of times (num_iter). Calls init_fn
    before each time it calls fn, to allow for custom per-iteration
    initialization.
    '''

    _real, _sys, _user = list(), list(), list()
    for _ in range(num_iter):
        if init_fn:
            (args, kwargs) = init_fn(*init_args, **init_kwargs)
        else:
            args = init_args
            kwargs = init_kwargs
        t = unix_time(fn, *args, **kwargs)
        _real.append(t["real"])
        _sys.append(t["sys"])
        _user.append(t["user"])

    print(f"{fn.__name__},"
          f"{round(np.mean(_real), 5)},"
          f"{round(np.mean(_user), 5)},"
          f"{round(np.mean(_sys), 5)}")

def benchmark_bash(cmd, num_iter=10):
    '''Benchmarks a bash command a specified number of times (num_iter).'''

    _real, _sys, _user = list(), list(), list()
    for _ in range(num_iter):
        flush_buffer_cache()
        t = unix_time_bash(cmd)
        _real.append(t["real"])
        _sys.append(t["sys"])
        _user.append(t["user"])

    return {'real': round(np.mean(_real), 5),
            'user': round(np.mean(_user), 5),
            'sys': round(np.mean(_sys), 5)
    }

def flush_buffer_cache():
    '''Flush the file buffer cache.'''
    os.system("sudo ./flush_cache.sh")
