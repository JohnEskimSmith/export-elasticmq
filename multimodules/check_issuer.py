#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "SAI"
__license__ = "GPLv3"
__email__ = "andrew.foma@gmail.com"
__status__ = "Dev"

def main(**kwargs) -> dict:
    from ujson import dumps
    try:
        record = kwargs.get('record')
        if record:
            line = dumps(record)
            if "bank".lower() in line.lower():
                return record
    except:
        pass