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
from pathlib import Path

from lib import (parse_args,
                 parse_settings,
                 AppConfig,
                 grouper_generation,
                 STOP_SIGNAL,
                 reader_file_from_memory,
                 chunkify,
                 reader_file,
                 parse_records
                 )
from time import monotonic as time_monotonic

# Configure logging globally
basic_config(level=logging.INFO, buffered=False, log_format='json')


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
        status_set_one_input_file = False
        input_files = _input_file
    count_files = len(input_files)
    for index_file, input_file in enumerate(input_files):
        input_file_end = Path(input_file).stat().st_size
        input_file_size_kb = round(input_file_end / 1024, 2)
        logger.info(f"{index_file + 1}. {input_file} size: {input_file_size_kb} Kbytes")
        if config.mode_read_input == 'memory':
            # region read file to memory
            try:
                with open(input_file, 'rt', encoding='utf-8') as f:
                    raw_rows = [row[:-1] for row in f.readlines()]
                blocks = grouper_generation(config.number_lines, raw_rows)
            except:
                exit(1)
            else:
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
                        statistics_file.write(ujson_dumps(config.statistics) + '\n')
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
