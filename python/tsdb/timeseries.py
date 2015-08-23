# -*- coding: utf-8 -*-
"""
Created on Fri Mar 13 21:31:23 2015

Timeseries defined by
* Name
* Frequency
* Timezone

will be saved as bitmaps in redis. One Bitmap for every year containing data
saved as a simple redis-Strings with the key 'ts:{pk}:bmap:{year}' with pk being
the id of the timeseries and year is self-explaining.

The values of the timeseries will be written as doubles (8byte) in chronological
order. Every year will get a block for the hole year once used.

@author: Eike
"""
import struct
from collections import namedtuple

import pandas as pd
import numpy as np

from tsdb.lock import locked

FREQS = [ '15T', '1H', '1D' ]
_STEPS_PER_YEAR = {'15T': 35040, '1H': 8760, '1D': 365, }
_STEPS_PER_LEAP_YEAR = {'15T': 35136, '1H': 8784, '1D': 366, }
_SEC_PER_STEP = {'15T': 900, '1H': 3600,}

TZS = [ 'CET', 'UTC' ]

ts_struct = namedtuple('ts_struct', 'pk name freq tz')

_KEY_TS_PK = 'ts:pk:'
#different hash fields for timeseries
_HF_PK = b'pk'
_HF_FREQ = b'freq'
_HF_TZ = b'tz'
_HF_NAME = b'name'
_ATTS = (_HF_PK, _HF_NAME, _HF_FREQ, _HF_TZ)

def _timeseries_hash_key(pk):
    return 'ts:{pk}'.format(pk=pk)

def _timeseries_block_starts(pk):
    return 'ts:{pk}:starts'.format(pk=pk)

def _timeseries_block(pk, year):
    return 'ts:{pk}:bmap:{year}'.format(pk=pk, year=year)

def create_new_ts(conn, ts):
    pk = conn.incr(_KEY_TS_PK)
    hash_key = _timeseries_hash_key(pk)
    ts = ts._replace(pk=pk)
    conn.hmset(
        hash_key,
        {att_name: att for att_name, att in zip(_ATTS, ts)},
    )
    return pk

def read_ts(conn, pk):
    hash_key = _timeseries_hash_key(pk)
    with locked(conn, hash_key):
        exists = conn.exists(hash_key)
        if not exists:
            raise RuntimeWarning('no timeseries with pk {}'.format(pk))
        result = conn.hgetall(hash_key)
    pk = int(result.get(_HF_PK))
    name = result.get(_HF_NAME).decode()
    freq = result.get(_HF_FREQ).decode()
    tz = result.get(_HF_TZ).decode()
    ts = ts_struct(pk, name, freq, tz)
    return ts

def delete_ts(conn, pk):
    hash_key = _timeseries_hash_key(pk=pk)
    set_key = _timeseries_block_starts(pk=pk)
    with locked(conn, hash_key):
        years = conn.smembers(set_key)
        block_keys = [
            _timeseries_block(pk=pk, year=y) for y in map(int, years)
        ]
        conn.delete(hash_key, set_key, *block_keys)

def _initial_year_block(freq, leap_year=False):
    # get steps in given year defined by the freq
    steps = _STEPS_PER_LEAP_YEAR[freq] if leap_year else _STEPS_PER_YEAR[freq]
    # initialize an array of length steps with NaNs and datatype double
    arr = np.nan * np.empty(steps, dtype='<f8')
    return arr

def _get_start_index(dt, freq, tz):
    start = '1/1/{year}'.format(year=dt.year)
    first_dt, = pd.date_range(start=start,periods=1,tz=tz,)
    delta = dt - first_dt
    secs_per_step = _SEC_PER_STEP[freq]
    return int(delta.total_seconds() // secs_per_step)

def _is_leap(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

def write_data(conn, pk, series):
    
    hash_key = _timeseries_hash_key(pk=pk)    # key to the hash of timeseries
    set_key = _timeseries_block_starts(pk=pk) # key to set of years with values
    ts = read_ts(conn, pk)                    # read ts attributes

    # generator for all years in the given period    
    years = range(series.index[0].year, series.index[-1].year+1)
    
    with locked(conn, hash_key):              # lock actions on timeseries
        # get index of first value in the first year        
        start_index = _get_start_index(series.index[0], ts.freq, ts.tz)
        
        for year in years:                    # iterate over the years
            vkey = _timeseries_block(pk, year)# key to the values of the year
            leap = _is_leap(year)             # is leapyear?
            values = series[str(year)].values # view on the values of the year
            if not conn.sismember(set_key, year):
                # no data for this year, write a whole year
                arr = _initial_year_block(ts.freq, leap)
                # write from first index with a value the values to this block
                arr[start_index:start_index+len(values)] = values
                # start is now zero
                start_index=0
            else:
                arr = values
            # format of values: len(arr) doubles
            fmt = 'd' * len(arr)
            # write to redis a bitmap of the array, each value needs 8 byte
            conn.setrange(vkey, start_index*8, struct.pack(fmt, *arr))
            # add year to set
            conn.sadd(set_key, year)
            # for followin years: start will always be zero
            start_index=0

def _trans(a_datetime):
    pattern = '%m/%d/%Y %H:%M'
    return a_datetime.strftime(pattern)    

def _steps_in_year(year, freq):
    leap_year = _is_leap(year)
    steps = _STEPS_PER_LEAP_YEAR[freq] if leap_year else _STEPS_PER_YEAR[freq]
    return steps

def read_data(conn, pk, start, end):
    hash_key = _timeseries_hash_key(pk=pk)     
    ts = read_ts(conn, pk)
    index = pd.date_range(                      # index with all dates of the
        start=_trans(start),                    #  given period from start to
        end=_trans(end),                        #  end
        freq=ts.freq,
        tz='UTC',
    ) 
    index = index.tz_convert(ts.tz)             # to tz of timeseries

    years = range(index[0].year, index[-1].year+1)
    # amount of values to be read. Needed for calculation of end indizes of the
    # years
    rest_vals = len(index)
    # initialize an array with NaNs and datatype double
    data = np.nan * np.empty(rest_vals, dtype='<f8')

    with locked(conn, hash_key):
        start_index = _get_start_index(index[0], ts.freq, ts.tz)
        # index for inserting in data array        
        data_index = 0

        for year in years:
            vkey = _timeseries_block(pk, year)
            steps = _steps_in_year(year, ts.freq)
            # do not write a whole block if steps > rest_values
            end_index = steps if rest_vals > steps else rest_vals
            raw_data = conn.getrange(vkey, start_index*8, end_index*8-1)
            #convert bitmap to array of double
            small_arr = np.fromstring(raw_data, dtype='<f8')

            size = len(small_arr)
            # sets the len(small_arr) values beginning at data_index to the 
            # values of small_arr
            data[data_index: data_index+size] = small_arr
            data_index += size
            rest_vals -= size
            start_index = 0
    return pd.Series(data=data, index=index)
