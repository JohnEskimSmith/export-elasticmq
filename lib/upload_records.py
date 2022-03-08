#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "SAI"
__license__ = "GPLv3"
__email__ = "andrew.foma@gmail.com"
__status__ = "Dev"

__all__ = ["zlib_prepare_records_bytes",
           "gzip_prepare_records_bytes",
           "parse_single_record_sync",
           "parse_single_record_async"]

from typing import List, Dict, Optional, Callable, Any
from ujson import dumps as ujson_dumps
from gzip import compress as gzip_compress
from zlib import compress as zlib_compress
from ipaddress import ip_address
from asyncio import wait_for as asyncio_wait_for
from .upload_utils import (return_value_from_dict_extended, make_path, )
from asyncio import TimeoutError as asyncio_TimeoutError


def pack_record(record: dict,
                keys: dict) -> dict:
    if keys:
        result = {}
        for k, path_to_key in keys.items():
            value = return_value_from_dict_extended(record, path_to_key)
            if value:
                need_path = k.split('.')
                make_path(result, value, *need_path)
        return result
    else:
        return record


def gzip_prepare_records_bytes(records: List[Dict]) -> Optional[bytes]:
    try:
        data_json = bytes(ujson_dumps(records), encoding='utf-8')
        data_packed = gzip_compress(data_json)
        return data_packed
    except:
        pass


def zlib_prepare_records_bytes(records: List[Dict]) -> Optional[bytes]:
    try:
        data_json = bytes(ujson_dumps(records), encoding='utf-8')
        data_packed = zlib_compress(data_json, 9)
        return data_packed
    except:
        pass

def method_return_maxmind(ip_str: str,
                          maxmind_reader_city,
                          maxmind_reader_asn) -> Optional[Dict]:
    result = {}
    try:
        # region asn
        response_asn = maxmind_reader_asn.asn(ip_str)
        asn = str(response_asn.autonomous_system_number)
        aso = str(response_asn.autonomous_system_organization)
        maxmind_asn_build_epoch = maxmind_reader_asn.metadata().build_epoch
        update_dict = {"asn_info": {"asn": asn,
                                    "aso": aso,
                                    "build_epoch": maxmind_asn_build_epoch
                                    }
                       }
        for k in list(update_dict.keys()):
            if not update_dict[k]:
                update_dict.pop(k)
        result.update(update_dict)
        # endregion
    except:
        pass
    try:
        # region city
        response_city = maxmind_reader_city.city(ip_str)
        city = response_city.city.name
        country = response_city.country.name
        maxmind_city_build_epoch = maxmind_reader_city.metadata().build_epoch
        longitude, latitude = response_city.location.longitude, response_city.location.latitude
        update_dict = {"city": city,
                       "country": country,
                       "location": {"type": "Point", "coordinates": [longitude, latitude]},
                       "build_epoch": maxmind_city_build_epoch}
        for k in list(update_dict.keys()):
            if not update_dict[k]:
                update_dict.pop(k)
        result.update({"city_info": update_dict})
        # endregion
    except Exception as e:
        pass
    if result:
        return {"geoip": result}


def return_geoip(record: Dict, config: "AppConfig") -> Dict:
    _ip = ''
    field_geoip = {}
    # TODO: rethink
    if 'ipv4' in record:
        _ip = record['ipv4']
    elif 'ip_v4_int' in record:
        try:
            _ip = ip_address(record['ip_v4_int'])
        except:
            pass
    elif 'ip' in record:
        try:
            _ip = ip_address(record['ip_v4_int'])
        except:
            pass
    if _ip:
        field_geoip: Optional[Dict] = method_return_maxmind(_ip,
                                                            config.geoip['reader_city'],
                                                            config.geoip['reader_asn'])
    if field_geoip:
        record.update(field_geoip)
    return record


def parse_single_record_sync(record: Dict,
                        config: "AppConfig",
                        save_function: Callable,
                        current_filter_record: Callable,
                        source_worker: Dict,
                        logger) -> Optional[Dict]:

    ready_record = None
    if config.operations.unfiltred:
        ready_record = record
    else:  # в противном случае срабатывает стандартный фильтр
        try:
            check_result_after_filter: Any = current_filter_record(record=record)
        except Exception as exp:
            ready_record = record  # as mode config.operations.unfiltred == True
            logger.error(exp)
        else:
            if check_result_after_filter:
                ready_record = record
    if not ready_record:
        return None
    packing_dict: Dict = config.packing_dict
    # TODO: other default function
    default_record: Optional[Dict] = save_function(record=ready_record, port=config.port)
    if default_record:
        if config.geoip:
            default_record: Dict = return_geoip(default_record, config)  # returned self default records or with geo
        default_record.update(source_worker)
        need_record: Dict = pack_record(default_record, packing_dict)
        if need_record:
            return need_record


async def parse_single_record_async(record: Dict,
                        config: "AppConfig",
                        save_function: Callable,
                        current_filter_record: Callable,
                        source_worker: Dict,
                        logger) -> Optional[Dict]:

    ready_record = None
    if config.operations.unfiltred:
        ready_record = record
    else:  # в противном случае срабатывает стандартный фильтр
        try:
            _c: Callable = current_filter_record(record=record)
            check_result_after_filter: Any = await asyncio_wait_for(_c, timeout=config.timeout_filter)
        except asyncio_TimeoutError:
            # ready_record = record  # as mode config.operations.unfiltred == True
            logger.error('timeout: filter function')
        except Exception as exp:
            logger.error(exp)
        else:
            if check_result_after_filter:
                ready_record = record
    if not ready_record:
        return None
    packing_dict: Dict = config.packing_dict
    # TODO: other default function
    default_record: Optional[Dict] = save_function(record=ready_record, port=config.port)
    if default_record:
        if config.geoip:
            default_record: Dict = return_geoip(default_record, config)  # returned self default records or with geo
        default_record.update(source_worker)
        need_record: Dict = pack_record(default_record, packing_dict)
        if need_record:
            return need_record
