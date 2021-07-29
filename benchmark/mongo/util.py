#!/usr/bin/env python3

import os
import time
import yaml

import numpy as np
from resource import getrusage as resource_usage, RUSAGE_SELF
from time import time as timestamp

path_join = os.path.join

_dir = os.path.dirname(os.path.realpath(__file__))
workload_dir = path_join(_dir, "..", "workload")
dataset_info = path_join(workload_dir, "dataset.yaml")


def workload_config(name):
    with open(path_join(workload_dir, name + ".yaml")) as f:
        return yaml.load(f, Loader=yaml.Loader)


# Note: copied from ../util.py

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


def benchmark(fn, init_fn=None, *init_args, num_iter=10, **init_kwargs):
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

    return {
        "real": round(np.mean(_real), 5),
        "user": round(np.mean(_user), 5),
        "sys": round(np.mean(_sys), 5),
    }


def timed(fn):
    def timeit(*args, **kw):
        ts = time.time()
        result = fn(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', fn.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f s' % (fn.__name__, (te - ts) * 1))
        return result

    return timeit
