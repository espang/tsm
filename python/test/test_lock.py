# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 20:23:09 2015

@author: Eike
"""
from threading import Thread
import time
import tsdb

import redis

TESTDB=9
POOL = redis.ConnectionPool(host='localhost', port=6379, db=TESTDB)

def test_lock_twice_acquire_lt_lock_timeout():
    lockname = 'a_key'    
    conn = redis.Redis(connection_pool=POOL)
    identifier = tsdb.acquire_lock_with_timeout(
        conn=conn,
        lockname=lockname,
        acquire_timeout=10,
        lock_timeout=3,
    )
    assert identifier is not False
    result = tsdb.acquire_lock_with_timeout(
        conn=conn,
        lockname=lockname,
        acquire_timeout=2,
        lock_timeout=2,
    )
    assert result == False
    tsdb.release_lock(conn, lockname, identifier)

def test_lock_twice_acquire_gt_lock_timeout():
    lockname = 'a_key'    
    conn = redis.Redis(connection_pool=POOL)
    identifier = tsdb.acquire_lock_with_timeout(
        conn=conn,
        lockname=lockname,
        acquire_timeout=10,
        lock_timeout=2,
    )
    assert identifier is not False
    identifier2 = tsdb.acquire_lock_with_timeout(
        conn=conn,
        lockname=lockname,
        acquire_timeout=3,
        lock_timeout=2,
    )
    assert identifier2 is not False
    assert identifier != identifier2
    tsdb.release_lock(conn, lockname, identifier2)

def test_lock_twice_and_release():
    def acquire(acquire_timeout):
        identifer2 = tsdb.acquire_lock_with_timeout(
            conn=conn,
            lockname=lockname,
            acquire_timeout=acquire_timeout,
            lock_timeout=5,
        )
        assert identifer2 is not False
        
    lockname = 'a_key'    
    conn = redis.Redis(connection_pool=POOL)
    identifier = tsdb.acquire_lock_with_timeout(
        conn=conn,
        lockname=lockname,
        acquire_timeout=10,
        lock_timeout=20,
    )
    assert identifier is not False
    
    t = Thread(target=acquire, args=(20, ))
    t.start()
    time.sleep(1)
    tsdb.release_lock(conn, lockname, identifier)
    