# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 20:01:48 2015

The redis script_load method is inspired by 'Redis in Action' from Dr. Josiah L. Carlson

--see https://github.com/josiahcarlson/redis-in-action

@author: Eike
"""
import redis


def script_load(script):
    sha = [None]
    def call(conn, keys=[], args=[], force_eval=False):
        if force_eval:
            return conn.execute_command(
                "EVAL", script, len(keys), *(keys+args))
            
        if not sha[0]:
            sha[0] = conn.execute_command(
                "SCRIPT", "LOAD", script, parse="LOAD")
        try:
            return conn.execute_command(
                "EVALSHA", sha[0], len(keys), *(keys+args))
        except redis.exceptions.ResponseError as msg:
            if not msg.args[0].startswith("NOSCRIPT"):
                raise
        
    return call