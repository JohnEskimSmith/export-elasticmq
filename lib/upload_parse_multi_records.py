from typing import Callable, Optional, List, Dict, Any
from asyncio import gather, wait_for
from .upload_utils import return_geoip, pack_record
from .upload_settings import AppConfig


async def parse_single_record_async(record: Dict,
                                    config: AppConfig,
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
            check_result_after_filter: Any = await wait_for(_c, timeout=config.timeout_filter)
        except TimeoutError:
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


def parse_single_record_sync(record: Dict,
                             config: AppConfig,
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


def parse_multi_records_sync(records: List[Dict],
                             config: AppConfig,
                             logger) -> List[Optional[Dict]]:
    source_worker = {}
    if config.source:
        try:
            source_worker = {'source': config.source}
        except:
            pass
    current_filter_record = None
    save_function: Callable = config.operations.create_records_function
    if config.operations.use_standart_filter:
        current_filter_record: Callable = config.operations.standart_filter_function
    elif config.operations.use_custom_filter:
        current_filter_record: Callable = config.operations.custom_filter_function
    _data = [parse_single_record_sync(record, config, save_function, current_filter_record, source_worker, logger)
             for record in records]
    result = [record for record in _data if record]
    return result


async def parse_multi_records_async(records: List[Dict],
                                    config: "AppConfig",
                                    logger) -> List[Optional[Dict]]:
    source_worker = {}
    if config.source:
        try:
            source_worker = {'source': config.source}
        except:
            pass
    current_filter_record = None
    save_function: Callable = config.operations.create_records_function
    if config.operations.use_standart_filter:
        current_filter_record: Callable = config.operations.standart_filter_function
    elif config.operations.use_custom_filter:
        current_filter_record: Callable = config.operations.custom_filter_function
    _coroutines = []
    for record in records:
        c = parse_single_record_async(record, config, save_function, current_filter_record, source_worker, logger)
        _coroutines.append(c)
    _data = await gather(*_coroutines)
    result = [record for record in _data if record]
    return result
