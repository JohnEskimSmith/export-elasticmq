#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "SAI"
__license__ = "GPLv3"
__email__ = "andrew.foma@gmail.com"
__status__ = "Dev"

__all__ = ["access_dot_path",
           "return_dict_for_packed_record",
           "return_value_from_dict_extended",
           "make_path",
           "check_iter",
           "grouper_generation"]

from typing import Any, Optional, Dict, Iterable, Iterator
from ujson import load as ujson_load
from pathlib import Path
from itertools import zip_longest


def access_dot_path(dictionary: dict, path: str, value=None) -> Optional[Any]:
    """
    Access dot-separated path in dictionary or return None
    """
    dot_index = path.find('.')
    if dot_index == -1:  # no more dots in path
        try:
            _value = dictionary.get(path)
            return _value if _value else value
        except:
            return value
    previous = path[:dot_index]  # key before first dot
    if previous not in dictionary:
        return None
    element = dictionary[previous]
    if isinstance(element, dict):
        return access_dot_path(element, path[dot_index + 1:])


def return_value_from_dict_extended(some_dict: dict,
                           path_string: str) -> Any:
    """
    Возвращает значение ключа в словаре по пути ключа "key.subkey.subsubkey"
    :param some_dict:
    :param path_string:
    :return:
    """
    check = access_dot_path(some_dict, path_string)
    if check:
        return check
    else:
        paths = []
        result = []
        values = []
        for i in path_string.split('.'):
            paths.append(i)
            o = '.'.join(paths)
            result.append(o)
        for path_to_value in result:
            value = access_dot_path(some_dict, path_to_value)
            if isinstance(value, list):
                for row in value:
                    sub_path = path_string[len(path_to_value)+1:]
                    data = return_value_from_dict_extended(row, sub_path)
                    if data:
                        if isinstance(data, list):
                            values.extend(data)
                        else:
                            values.append(data)
    if values:
        if len(values) == 1:
            return values[0]
        else:
            return values


def make_path(d: dict,
              value,
              *paths: str) -> None:
    for i, key in enumerate(paths):
        if i != len(paths)-1:
            d = d.setdefault(key, {})
        else:
            d = d.setdefault(key, value)


def return_dict_for_packed_record(filename_packing_dict: str,
                                  logger) -> Optional[Dict]:
    dict_for_packed_record = {}
    if filename_packing_dict:
        if Path(filename_packing_dict).exists():
            try:
                with open(filename_packing_dict, 'rt') as file_dict:
                    dict_for_packed_record = ujson_load(file_dict)
            except Exception as e:
                logger.error(e)
    if not dict_for_packed_record:
        logger.info("No data packing dictionary found")
    else:
        return dict_for_packed_record


def check_iter(value: Any) -> bool:
    try:
        iter(value)
    except TypeError:
        return False
    else:
        if not isinstance(value, str):
            return True
        else:
            return False

def grouper_generation(count: int,
                       iterable: Iterable,
                       fillvalue: Any = None) -> Iterator[list]:
    """
    :param count: length of subblock
    :param iterable: array of data
    :param fillvalue: is fill value in last chain
    :return:
    grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    генератор блоков по count элементов из списка iterable
    """
    args = [iter(iterable)] * count
    for element in zip_longest(fillvalue=fillvalue, *args):
        tmp = filter(lambda y: y is not None, element)
        yield list(tmp)