#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "SAI"
__license__ = "GPLv3"
__email__ = "andrew.foma@gmail.com"
__status__ = "Dev"

import asyncio
import uvloop
import logging
from aiomisc.log import basic_config
from ujson import dump as ujson_dump, dumps as ujson_dumps
from aiofiles import open as aiofiles_open
from pathlib import Path
from sys import getsizeof as sys_getsizeof
from typing import Callable, Optional, List, Dict, Tuple, Iterator
from lib import (parse_args,
                 parse_settings,
                 AppConfig,
                 gzip_prepare_records_bytes,
                 parse_single_record_sync,
                 parse_single_record_async,
                 send_one_message,
                 split_records,
                 grouper_generation,
                 create_default_tags_for_routes_messages,
                 )
from time import monotonic as time_monotonic

STOP_SIGNAL = 'STOP'  # signal for stop to Queues chain
# Configure logging globally
basic_config(level=logging.INFO, buffered=False, log_format='json')


def chunkify(file_name_raw: str,
             file_end: int,
             size: int) -> Iterator[Tuple]:
    """ Return a new chunk """
    with open(file_name_raw, 'rb') as file:
        chunk_end = file.tell()
        while True:
            chunk_start = chunk_end
            file.seek(size, 1)
            file.readline()
            chunk_end = file.tell()
            if chunk_end > file_end:
                chunk_end = file_end
                yield chunk_start, chunk_end - chunk_start
                break
            else:
                yield chunk_start, chunk_end - chunk_start


async def send_to_logcollector(sem: asyncio.Semaphore,
                               block_records: List[dict],
                               config: AppConfig,
                               collector: dict,
                               queue_statistics: asyncio.Queue,
                               duration_parserecords: float,
                               logger):
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
                        await asyncio.sleep(sleep_time)
            except Exception as exc:
                logger.error(exc)
    try:
        duration_sendlogcollector = trace_request_ctx['duration']
    except:
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


async def send_to_sqs(sem: asyncio.Semaphore,
                      message_and_size: Tuple,
                      config_sqs: Dict,
                      queue_statistics: asyncio.Queue,
                      duration_parserecords: float,
                      logger):
    start_time_of_send = time_monotonic()

    sqs_queue_url: str = config_sqs['url']
    client = config_sqs['client']

    size_payload = message_and_size[0]
    message = message_and_size[1]
    count_message = 1
    async with sem:
        http_status = await send_one_message(client, message, sqs_queue_url, logger)

    collector_host_name = 'SQS'
    duration_sendlogcollector = round(time_monotonic() - start_time_of_send, 4)

    message = (count_message,
               http_status,
               count_message,
               size_payload,
               duration_parserecords,
               duration_sendlogcollector,
               collector_host_name
               )
    await queue_statistics.put(message)


async def reader_file(config: AppConfig,
                      queue_input: asyncio.Queue,
                      positions: List[Tuple],
                      logger):
    errors = 0
    filename = config.input_file
    # how to convert list of strings in utf-8 from file
    convert_function: Callable = config.operations.converter_raw_function
    if Path(filename).exists():
        async with aiofiles_open(str(filename), mode='rb') as file:
            for chunk_start, chunk_size in positions:
                await file.seek(chunk_start)
                _lines = await file.read(chunk_size)
                try:
                    lines = _lines.decode('utf-8').splitlines()
                except Exception as exp:
                    errors += 1
                    logger.error(f'error lines, start:{chunk_start}, size:{chunk_size}')
                    logger.error(exp)
                    async with aiofiles_open('errors.data', 'ab') as error_file:
                        await error_file.write(_lines)
                        await error_file.write('b\n')
                else:
                    config.statistics['all_records'] += len(lines)
                    records: List[Dict] = convert_function(lines=lines)
                    await queue_input.put(records)
        logger.info(f'errors:{errors}')
        await queue_input.put(STOP_SIGNAL)
    else:
        logger.error(f'{filename} - not found')
        await queue_input.put(STOP_SIGNAL)


async def reader_file_from_memory(config: AppConfig,
                      queue_input: asyncio.Queue,
                      blocks: List[List[str]],
                      logger):
    errors = 0
    convert_function: Callable = config.operations.converter_raw_function
    for lines in blocks:
        config.statistics['all_records'] += len(lines)
        try:
            records: List[Dict] = convert_function(lines=lines)
            await queue_input.put(records)
        except Exception as exp:
            errors += 1
            logger.error(f'error lines:...')
            logger.error(exp)
            async with aiofiles_open('errors.data', 'at') as error_file:
                await error_file.write('\n'.join(lines))
                await error_file.write('b\n')

    await queue_input.put(STOP_SIGNAL)


def parse_multi_records_sync(records: List[Dict],
                             config: AppConfig,
                             logger) -> List[Dict]:
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
    result = list(filter(lambda z: z, _data))
    return result


async def parse_multi_records_async(records: List[Dict],
                                    config: AppConfig,
                                    logger) -> List[Dict]:
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
    _data = await asyncio.gather(*_coroutines)
    result = list(filter(lambda z: z, _data))
    return result


async def parse_records(config: AppConfig,
                        queue_input: asyncio.Queue,
                        queue_tasks: asyncio.Queue,
                        queue_statistics: asyncio.Queue,
                        sem: asyncio.Semaphore,
                        logger):

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
                coroutine_logcollector = None
                if config.collectors:
                    collector = next(config.collectors['cycle'])  # ClientSession from aiohttp
                    coroutine_logcollector = send_to_logcollector(sem,
                                                                  ready_records,
                                                                  config,
                                                                  collector,
                                                                  queue_statistics,
                                                                  duration_time_parserecords,
                                                                  logger)
                elif config.sqs:
                    # из-за Яндекса: отсутствия в Яндексе тэгов для очередей,
                    # приходится писать информацию о сохранении внутрь блока с сообщением
                    tags = None
                    if 'yandex' in config.sqs['init_keys']['endpoint_url']:
                        _, tags = create_default_tags_for_routes_messages(config.sqs)
                    blocks = split_records(ready_records, tags)
                    for message in blocks:
                        coroutine_logcollector = send_to_sqs(sem,
                                                             message,
                                                             config.sqs,
                                                             queue_statistics,
                                                             duration_time_parserecords,
                                                             logger)
                else:
                    pass
                if coroutine_logcollector:
                    task = asyncio.create_task(coroutine_logcollector)
                    await queue_tasks.put(task)
    await queue_tasks.put(STOP_SIGNAL)


async def promise_to_close_clients(config: AppConfig, logger) -> None:
    if config.collectors:
        try:
            for collector in config.collectors['list']:
                await collector['session'].close()  # ClientSession from aiohttp
        except Exception as e:
            logger.error(e)
            logger.error('errors when closing aiohttp ClientSessions connections')
    elif config.sqs:
        try:
            await config.sqs['client'].close()
        except Exception as e:
            logger.error(e)
            logger.error('errors when closing SQS Client connection')


async def reader_statistics(config: AppConfig,
                            queue_statistics: asyncio.Queue,
                            logger,
                            count_files=1):
    size_sum = 0
    count_sum = 0
    all_messages = 0
    while True:
        message = await queue_statistics.get()
        if message == STOP_SIGNAL:
            break
        if message:
            number_of_messages, status, count_records, size, \
            duration_parserecords, duration_sendlogcollector, collector_host = message
            if status == 200:
                size_sum += size
                count_sum += count_records
                all_messages += number_of_messages
                message_info = f"sent to log collectors: sum. size:{size_sum}, " \
                               f"sum. records:{count_sum}, current:{count_records}, " \
                               f"number of messages: {number_of_messages}, " \
                               f"parsing(duration): {duration_parserecords}, " \
                               f"sending to {collector_host}(duration): {duration_sendlogcollector}"
                logger.info(message_info)
            else:
                logger.error(f'not save to SQS')  # TODO: rethink
    logger.info(f'sent to Collectors(records): {config.statistics["valid_records"]}')
    _kilobytes = size_sum / 1024
    if _kilobytes < 1:
        _size_sum = size_sum
        desc_size_sum = 'bytes'
    else:
        _size_sum = _kilobytes
        desc_size_sum = 'Kbytes'
    logger.info(f'sent to Collectors({desc_size_sum}): {"%.2f %s" % (_size_sum, desc_size_sum)}')
    logger.info(f'sent to Collectors(messages): {all_messages}')
    config.statistics['size'] = size_sum
    config.statistics['message'] = all_messages
    if count_files == 1:
        await promise_to_close_clients(config, logger)



async def executor_tasks(queue_tasks: asyncio.Queue,
                         queue_statistics: asyncio.Queue):
    while True:
        task = await queue_tasks.get()
        if task == STOP_SIGNAL:
            break
        if task:
            await task
    await queue_statistics.put(STOP_SIGNAL)


async def main_upload_function(arguments_in=None, config_in=None):
    start_time = time_monotonic()
    # region simple Configure
    # TODO: rethink
    if not arguments_in:
        arguments = parse_args()
    else:
        arguments = arguments_in
    if not arguments:
        print('not found arguments for upload...')
        exit(1)

    basic_config(
        level=logging.INFO,
        buffered=True,
        log_format=arguments.log,
        flush_interval=2
    )
    logger = logging
    # endregion

    if not config_in:
        config: AppConfig = await parse_settings(arguments, logger)
    else:
        config = config_in
    if not config:
        print('not found file: yaml with settings for upload...')
        exit(1)

    queue_input = asyncio.Queue()
    queue_task = asyncio.Queue()
    queue_statistics = asyncio.Queue()
    task_semaphore = asyncio.Semaphore(config.senders)

    start_time_about_file = time_monotonic()

    input_files = []
    _input_file = config.input_file
    status_set_one_input_file = True
    if isinstance(_input_file, str):
        input_files = [_input_file]
    elif isinstance(_input_file, list):
        status_set_one_input_file= False
        input_files = _input_file
    count_files = len(input_files)
    for index_file, input_file in enumerate(input_files):
        input_file_end = Path(input_file).stat().st_size
        input_file_size_kb = round(input_file_end/1024, 2)
        logger.info(f"{index_file+1}. {input_file} size: {input_file_size_kb} Kbytes")
        if config.mode_read_input == 'memory':
            # region read file to memory
            try:
                with open(input_file, 'rt', encoding='utf-8') as f:
                    raw_rows = [row[:-1] for row in f.readlines()]
                blocks = grouper_generation(config.number_lines, raw_rows)
            except:
                exit(1)
            duration_time_about_file = round(time_monotonic() - start_time_about_file, 4)
            logger.info(f'File size: {round(input_file_end / 1024, 2)} '
                        f'Kbytes. Time read file to memory: {duration_time_about_file}')
            task_reader = reader_file_from_memory(config, queue_input, blocks, logger)
            # endregion
        elif config.mode_read_input == 'asyncio':
            # region positions of chunk in file
            default_size = config.size_bulk_mb * (1024 * 1024)
            chunkify_positions = [(chunk_start, chunk_size)
                                  for chunk_start, chunk_size in
                                  chunkify(input_file, file_end=input_file_end, size=default_size)
                                  ]
            duration_time_about_file = round(time_monotonic() - start_time_about_file, 4)
            logger.info(f'File size: {input_file_size_kb} '
                        f'Kbytes. Time chunkify positions file: {duration_time_about_file}')
            task_reader = reader_file(config, queue_input, chunkify_positions, logger)
            # endregion
        else:
            logger.error('unknow type(mode) method read input file, exit.')
            exit(1)

        task_parse = parse_records(config, queue_input, queue_task, queue_statistics, task_semaphore, logger)
        task_execute = executor_tasks(queue_task, queue_statistics)
        task_reader_statistics = reader_statistics(config, queue_statistics, logger, count_files=count_files)

        running_tasks = [asyncio.create_task(worker)
                         for worker in [task_reader, task_parse, task_execute, task_reader_statistics]]
        await asyncio.wait(running_tasks)

        config.statistics['duration'] = round(time_monotonic() - start_time, 4)
        logger.info(f'summary duration: {config.statistics["duration"]}')
        if config.statistics_file:
            if count_files == 1:
                try:
                    with open(config.statistics_file, 'wt') as statistics_file:
                        ujson_dump(config.statistics, statistics_file)
                except Exception as exp:
                    logger.error(f'save statistics: {str(exp)}')
            else:
                try:
                    with open(config.statistics_file, 'at') as statistics_file:
                        statistics_file.write(ujson_dumps(config.statistics)+'\n')
                except Exception as e:
                    logger.error(e)
        start_time_about_file = time_monotonic()
        start_time = time_monotonic()
    if count_files > 1:
        await promise_to_close_clients(config, logger)

if __name__ == "__main__":
    # TODO: add read from stdin?
    uvloop.install()
    asyncio.run(main_upload_function())
