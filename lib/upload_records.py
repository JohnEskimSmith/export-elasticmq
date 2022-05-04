#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "SAI"
__license__ = "GPLv3"
__email__ = "andrew.foma@gmail.com"
__status__ = "Dev"

__all__ = ["parse_records"]

from typing import List, Dict, Optional, Tuple
from ujson import dumps as ujson_dumps
from sys import getsizeof as sys_getsizeof
from .upload_utils import (STOP_SIGNAL, gzip_prepare_records_bytes, )
from .upload_settings import AppConfig
from asyncio import Queue, Semaphore, create_task, sleep
from time import monotonic as time_monotonic
from .upload_sqs import split_records, send_to_sqs
from .upload_parse_multi_records import parse_multi_records_sync, parse_multi_records_async
from .upload_settings import create_default_tags_for_routes_messages


async def send_to_logcollector(sem: Semaphore,
                               block_records: List[dict],
                               config: AppConfig,
                               collector: dict,
                               queue_statistics: Queue,
                               duration_parserecords: float,
                               logger) -> None:
    trace_request_ctx = {'request': True}
    url: str = collector['url']
    collector_host_name: str = collector['name']
    client_session = collector['session']  # ClientSession from aiohttp
    use_gzip: bool = collector.get('use_gzip', False)
    http_status = 1000  # for example, http_status, payload - need rethink?
    payload = b''
    if use_gzip:
        payload: Optional[bytes] = gzip_prepare_records_bytes(block_records)
    async with sem:
        if (payload and use_gzip) or (block_records and not use_gzip):
            http_status = 0
            try:
                attempts: int = config.try_retry_upload
                sleep_time = 1
                while True and attempts > 0:
                    if use_gzip and payload:
                        async with client_session.post(url,
                                                       data=payload,
                                                       trace_request_ctx=trace_request_ctx) as response:
                            http_status = response.status
                    elif block_records and not use_gzip:
                        async with client_session.post(url,
                                                       json=block_records,
                                                       trace_request_ctx=trace_request_ctx) as response:
                            http_status = response.status
                    if http_status == 200:
                        break
                    else:
                        logger.error(f"Log-collector: http status: {http_status}")
                        attempts -= 1
                        await sleep(sleep_time)
            except Exception as exc:
                logger.error(exc)
    try:
        duration_sendlogcollector = trace_request_ctx['duration']
    except Exception as exp:
        logger.error(exp)
        duration_sendlogcollector = 0
    size_payload = len(payload)
    if not payload and not use_gzip:
        size_payload = sys_getsizeof(ujson_dumps(block_records))

    message = (1,
               http_status,
               len(block_records),
               size_payload,
               duration_parserecords,
               duration_sendlogcollector,
               collector_host_name
               )
    await queue_statistics.put(message)


async def parse_records(config: AppConfig,
                        queue_input: Queue,
                        queue_tasks: Queue,
                        queue_statistics: Queue,
                        sem: Semaphore,
                        logger) -> None:

    while True:
        records = await queue_input.get()
        if records == STOP_SIGNAL:
            break
        else:
            # не лучшее решение, надо обдумать, но для проверки и как рабочая версия - норм
            # проверяем асинхронная ли функция фильтра или нет
            if not config.operations.async_filter:
                start_time_parserecords = time_monotonic()
                ready_records: List[Dict] = parse_multi_records_sync(records, config, logger)
                duration_time_parserecords: float = round(time_monotonic() - start_time_parserecords, 4)
            else:
                start_time_parserecords = time_monotonic()
                ready_records: List[Dict] = await parse_multi_records_async(records, config, logger)
                duration_time_parserecords: float = round(time_monotonic() - start_time_parserecords, 4)
            if ready_records:
                config.statistics['valid_records'] += len(ready_records)
                if config.collectors:
                    collector = next(config.collectors['cycle'])  # ClientSession from aiohttp
                    coroutine_logcollector = send_to_logcollector(sem,
                                                                  ready_records,
                                                                  config,
                                                                  collector,
                                                                  queue_statistics,
                                                                  duration_time_parserecords,
                                                                  logger)
                    task = create_task(coroutine_logcollector)
                    await queue_tasks.put(task)
                elif config.sqs:
                    # из-за Яндекса: отсутствия в Яндексе тэгов для очередей,
                    # приходится писать информацию о сохранении внутрь блока с сообщением
                    tags = None
                    if 'yandex' in config.sqs['init_keys']['endpoint_url']:
                        _, tags = create_default_tags_for_routes_messages(config.sqs)
                    blocks: List[Tuple[int, str]] = split_records(ready_records, tags)
                    for message in blocks:
                        coroutine_logcollector = send_to_sqs(sem,
                                                             message,
                                                             config.sqs,
                                                             queue_statistics,
                                                             duration_time_parserecords,
                                                             logger)
                        task = create_task(coroutine_logcollector)
                        await queue_tasks.put(task)

                else:
                    pass

    await queue_tasks.put(STOP_SIGNAL)
