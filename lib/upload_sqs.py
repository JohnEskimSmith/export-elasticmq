import asyncio
from typing import List, Optional, Dict, Iterator, Tuple
from .upload_utils import grouper_generation
from .upload_records import gzip_prepare_records_bytes
from base64 import standard_b64encode
from math import ceil as math_ceil
from ujson import dumps as ujson_dumps
CONST_SQS_SIZE = 250*1024


def split_records(block_records: List[Dict], tags=None) -> List[Tuple[int, str]]:
    """
    кодируется List[Dict] в байты и BASE64, если размер больше лимита для SQS,
    разбивается на количество частей, на сколько больше (мин. на 2 части)
    """
    payload: Optional[bytes] = gzip_prepare_records_bytes(block_records)
    _payload_base64 = standard_b64encode(payload)
    size_payload_base64_bytes = len(_payload_base64)
    count = size_payload_base64_bytes / CONST_SQS_SIZE
    result = []
    if count <= 1:  # меньше и равно лимиту
        if not tags:
            return [(size_payload_base64_bytes, _payload_base64.decode('utf-8'))]
        else:
            updated_payload = {}
            updated_payload.update(tags)
            updated_payload['data'] = _payload_base64.decode('utf-8')
            updated_payload = ujson_dumps(updated_payload)
            return [(size_payload_base64_bytes, updated_payload)]

    else: #  больше лимита сообщения SQS
        # grouping_factor - по сколько записей в "новых" блоках
        grouping_factor = len(block_records) // math_ceil(count)
        blocks = (split_records(block, tags) for block in grouper_generation(grouping_factor, block_records))
        for block in blocks:
            result.extend(block)
        return result


async def send_one_message(client_q, message:str, queue_url: str) -> int:
    try:
        response = await client_q.send_message(QueueUrl=queue_url, MessageBody=message)
        status_sqs_aws = response['ResponseMetadata']['HTTPStatusCode']
    except:
        status_sqs_aws = 0
    return status_sqs_aws


async def send_batch_to_queue(client_q, messages: List[str], queue_url: str) -> bool:
    tasks = [asyncio.create_task(send_one_message(client_q, message, queue_url))
             for message in messages]
    responses = await asyncio.gather(*tasks)
    #  все значения должны быть 200, http status из API SQS
    return all((value == 200 for value in responses))


async def return_queue_url(client_q, queue_name: str, logger):
    queue_url = None
    try:
        current_queue = await client_q.get_queue_url(QueueName=queue_name)
        queue_url = current_queue.get('QueueUrl')
    except Exception as e:
        logger.error(str(e))
    if queue_url:
        return queue_url


async def return_queue_url_realtime(client_q, queue_name, logger, tags={}, auto_create=False):
    queue_url = await return_queue_url(client_q, queue_name, logger)
    if queue_url:
        return queue_url
    elif auto_create:
        logger.info(f'not found Queue: {queue_name}')
        logger.info(f'try create Queue: {queue_name}, and get its url')
        try:
            if queue_name.endswith('.fifo'):
                _queue_url = await client_q.create_queue(QueueName=queue_name,
                                                         Attributes={'FifoQueue': 'true'},
                                                         tags=tags)
            else:
                _queue_url = await client_q.create_queue(QueueName=queue_name,
                                                         tags=tags)
            queue_url = _queue_url.get('QueueUrl')
            logger.info("created Queue {}".format(queue_name))
            return queue_url
        except Exception as e:
            logger.error(e)