# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 20:00:41 2015

Locking is inspired by 'Redis in Action' from Dr. Josiah L. Carlson

--see https://github.com/josiahcarlson/redis-in-action

@author: Eike
"""
import math
import time
import uuid

from contextlib import contextmanager

from tsdb.helper import script_load

# KEYS[1] - lock name
# ARGV[1] - timeout in seconds
# ARGV[2] - identifier
# return 
_LUA_ACQUIRE_LOCK = script_load('''
if redis.call('exists', KEYS[1]) == 0 then
    return redis.call('setex', KEYS[1], unpack(ARGV))
end
''')
_LUA_RELEASE_LOCK = script_load('''
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1]) or true
end
''')

def acquire_lock_with_timeout(
    conn, lockname, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))
    acquired = False
    end = time.time() + acquire_timeout
    while time.time() < end and not acquired:
        acquired =  b'OK' == _LUA_ACQUIRE_LOCK(
            conn,
            [lockname],
            [lock_timeout, identifier]
        )
        time.sleep(.001 * (not acquired))
    # returns false,      if not acquired
    #         identifier, otherwise
    return acquired and identifier

def release_lock(conn, lockname, identifier):
    lockname = 'lock:' + lockname
    return _LUA_RELEASE_LOCK(conn, [lockname], [identifier])

@contextmanager
def locked(conn, lockname, acquire_timeout=3, lock_timeout=3):
    identifier = acquire_lock_with_timeout(
        conn,
        lockname,
        acquire_timeout,
        lock_timeout,
    )
    try:
        yield
    finally:
        release_lock(conn, lockname, identifier)
    
    