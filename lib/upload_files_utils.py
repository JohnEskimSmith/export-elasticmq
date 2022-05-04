from typing import Iterator, Tuple, List, Callable, Dict, Union
from pathlib import Path
from asyncio import Queue
from aiofiles import open as aiofiles_open
from .upload_utils import STOP_SIGNAL
from .upload_settings import AppConfig


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


async def reader_file(config: AppConfig,
                      queue_input: Queue,
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
                                  queue_input: Queue,
                                  blocks: Union[List[List[str]], Iterator],
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
