# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 21:32:26 2015

@author: Eike
"""
import tsdb



import redis

TESTDB=9
POOL = redis.ConnectionPool(host='localhost', port=6379, db=TESTDB)

 
    