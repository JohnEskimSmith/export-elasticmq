# проверка на чтение данных из SQS и их распаковка в структуру
import asyncio
import aiobotocore
from typing import List, Dict
from base64 import standard_b64decode
from gzip import decompress as gzip_decompress
from ujson import loads as ujson_loads
from contextlib import AsyncExitStack
from aiobotocore.session import AioSession


def decompaction(data: str) -> List:
    '''распакова данных из сообщения из sqs'''
    message_bytes: bytes = data.encode('utf-8')
    _unpack_data = gzip_decompress(standard_b64decode(message_bytes))
    unpack_data = _unpack_data.decode('utf-8')
    dict_struct = ujson_loads(unpack_data)
    return dict_struct


# How to use with an external exit_stack
async def create_sqs_client(session: AioSession, exit_stack: AsyncExitStack, auth_struct: Dict):
    # Create client and add cleanup
    client = await exit_stack.enter_async_context(session.create_client(**auth_struct))
    return client


async def return_queue_url(client_q, queue_name: str):
    queue_url = None
    try:
        current_queue = await client_q.get_queue_url(QueueName=queue_name)
        queue_url = current_queue.get('QueueUrl')
    except Exception as e:
        print(e)
        # logger.error(str(e))
    if queue_url:
        return queue_url


async def main(size=1, need_delete_messages=False):
    auth_struct = {'service_name': 'sqs',
                   'endpoint_url': 'http://192.168.1.51:9324',
                   'region_name': 'elasticmq',
                   'use_ssl' : False,
                   'aws_secret_access_key': 'x',
                   'aws_access_key_id': 'x'}

    session = AioSession()
    exit_stack = AsyncExitStack()
    client = await create_sqs_client(session, exit_stack, auth_struct)
    sqsname_queue = 'Results_examples_json_cuuid_811b5320-30bb-4639-aa97-922b67ed7eff'
    queue_url = await return_queue_url(client, sqsname_queue)
    data_with_sqs_messages = []
    try:
        for i in range(size):
            _messages = await client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                VisibilityTimeout=3,
                WaitTimeSeconds=2,
            )
            messages = _messages.get('Messages')
            if messages:
                msg = messages[0]
                data = msg.get('Body')
                _values = {'data': data,
                           'sqs': msg}
                data_with_sqs_messages.append(_values)
    except Exception as e:
        print(e)
    i = 0
    for message in data_with_sqs_messages:
        unpack_struct = decompaction(message['data'])
        for record in unpack_struct:
            print(record)
            i += 1
            print(i)
    if need_delete_messages:
        for message in data_with_sqs_messages:
            msg = message['sqs']
            aws_sqs_log = await client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg.get('ReceiptHandle')
            )
            if aws_sqs_log['ResponseMetadata']['HTTPStatusCode'] == 200:  # удалили нормально
                print(f'message deleted from SQS')
    await client.close()

if __name__ == '__main__':
    delete_messages = True
    asyncio.run(main(need_delete_messages=delete_messages))