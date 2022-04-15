#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "SAI"
__license__ = "GPLv3"
__email__ = "andrew.foma@gmail.com"
__status__ = "Dev"

__all__ = ["parse_args", "parse_settings", "AppConfig", "RecordOperation", "create_default_tags_for_routes_messages"]
from base64 import standard_b64encode
from dataclasses import dataclass
from random import shuffle
from aiohttp import ClientSession, TraceConfig
from .upload_utils import access_dot_path, return_dict_for_packed_record, return_value_from_dict_extended, check_iter
from ujson import load as ujson_load, dumps as ujson_dumps, loads as ujson_loads
import argparse
from typing import Callable, Optional, Dict, List, Any, Tuple
import geoip2.database
from ipaddress import ip_address
from datetime import datetime
from yaml import (FullLoader as yaml_FullLoader,
                  load as yaml_load,
                  )
from itertools import cycle
from aiohttp import BasicAuth
from pathlib import Path
import sys
from os import path as os_path, access as os_access, R_OK, sep, environ
import importlib
from asyncio import get_event_loop
from inspect import iscoroutinefunction as inspect_iscoroutinefunction
from .upload_sqs import return_queue_url_realtime
from contextlib import AsyncExitStack
from aiobotocore.session import AioSession
from aiobotocore.config import AioConfig

CONST_PATH_TO_MODULES = '/multimodules'
CONST_PATH_TO_CONVERTERS = 'converters'

class RecordOperation:

    def __init__(self,
                 settings_converter: Dict,
                 settings_create_record: Dict,
                 settings_filter_record: Dict,
                 settings_custom_filter_record: Dict,
                 use_standart_filter: bool,
                 use_custom_filter: bool,
                 root_dir_to_modules: str,
                 root_dir_to_converters: str,
                 logger):

        self.logger = logger
        self.unfiltred = False
        self.async_filter = False
        if 'python' in [settings_converter.get('type'),
                        settings_create_record.get('type')] or use_custom_filter:
            sys.path.append(root_dir_to_modules)
            sys.path.append(root_dir_to_converters)

        # self.converter_raw_function - function need for convert str line to Python Dict
        self.converter_raw_function = None
        self.special_converter_add = ','
        if settings_converter['type'] == 'json':
            self.raw_record_format = 'json'
            self.converter_raw_function: Callable = self.simple_json
        elif settings_converter['type'] == 'csv':
            self.raw_record_format = 'csv'
            self.converter_raw_function: Callable = self.simple_csv
            self.special_converter_add = settings_converter.get('special', ',')
            if not self.special_converter_add:
                self.special_converter_add = ','
        elif settings_converter['type'] == 'python':
            self.raw_record_format = 'python'
            _list_args = [settings_converter.get('path'), settings_converter.get('module')]
            if not any(_list_args):
                self.logger.error('converter: where settings for module - converter?')
                exit(1)
            if settings_converter.get('module'):
                path_to_module = f'{root_dir_to_converters}{sep}{settings_converter["module"]}'
            elif settings_converter.get('path'):
                path_to_module: str = settings_converter['path']
            try:
                name_function: str = settings_converter.get('function', 'main')
                if not name_function:
                    name_function = 'main'
                self.converter_raw_function: Callable = load_python_module(path_to_module, name_function)
                if self.converter_raw_function:
                    self.logger.info(f'loaded module: {path_to_module}, function:{name_function}')
                else:
                    self.logger.error(f'not found module: {path_to_module}')
                    exit(1)
            except Exception as e:
                logger.error(e)
                exit(1)

        self.create_records_function = None
        # self.create_records_function - function need for convert Dict from app to ready record Dict
        if settings_create_record['type'] == 'default':
            self.create_records_function: Callable = self.save_default  # input single record
        elif settings_create_record['type'] == 'json':
            self.create_records_function: Callable = self.save_json  # input single record
        elif settings_create_record['type'] == 'python':
            _list_args = [settings_create_record.get('path'), settings_create_record.get('module')]
            if not any(_list_args):
                self.logger.error('converter: where settings for module - create records?')
                exit(1)
            if settings_create_record.get('module'):
                path_to_module = f'{root_dir_to_modules}{sep}{settings_create_record.get("module")}'
            elif settings_create_record.get('path'):
                path_to_module: str = settings_create_record.get('path')
            try:
                name_function: str = settings_create_record.get('function', 'main')
                if not name_function:
                    name_function = 'main'
                self.create_records_function: Callable = load_python_module(path_to_module, name_function)
                if self.create_records_function:
                    self.logger.info(f'loaded module: {path_to_module}, function:{name_function}')
                else:
                    self.logger.error(f'not found module: {path_to_module}')
                    exit(1)
            except Exception as e:
                logger.error(e)
                exit(1)

        self.default_create_fields = []
        if settings_create_record.get('default_fields'):
            if isinstance(settings_create_record['default_fields'], list):
                self.default_create_fields = settings_create_record['default_fields']
            elif isinstance(settings_create_record['default_fields'], str):
                self.default_create_fields = list(
                    set([c.strip() for c in settings_create_record['default_fields'].split(',')]))

        if not all([self.converter_raw_function, self.create_records_function]):
            logger.error('settings: raw converter or function of create records  - not found')
            exit(1)

        self.use_standart_filter = use_standart_filter
        if self.use_standart_filter:
            self.standart_filter_value: str = settings_filter_record.get('value_success')
            self.standart_filter_path: str = settings_filter_record.get('path')
            self.standart_filter_function = self.filter_default

        self.use_custom_filter = use_custom_filter
        if self.use_custom_filter:
            _list_args = [settings_custom_filter_record.get('path'), settings_custom_filter_record.get('module')]
            if not any(_list_args):
                self.logger.error('custom filters: where settings for module - custom filter records?')
                exit(1)
            if settings_custom_filter_record.get('module'):
                path_to_module = f'{root_dir_to_modules}{sep}{settings_custom_filter_record.get("module")}'
            elif settings_custom_filter_record.get('path'):
                path_to_module: str = settings_custom_filter_record.get('path')
            try:
                name_function: str = settings_custom_filter_record.get('function', 'main')
                if not name_function:
                    name_function = 'main'
                self.custom_filter_function: Callable = load_python_module(path_to_module, name_function)
                if self.custom_filter_function:
                    self.logger.info(f'loaded module: {path_to_module}, function:{name_function}')
                    self.async_filter = inspect_iscoroutinefunction(self.custom_filter_function)  # async def or def
                else:
                    self.logger.error(f'not found module: {path_to_module}')
                    exit(1)
            except Exception as e:
                logger.error(e)
                exit(1)
        if not self.use_standart_filter and not self.use_custom_filter:
            self.unfiltred = True

    def simple_csv(self, lines: List[str]) -> List[Dict]:
        results = []
        for line in lines:
            try:
                dict_line = {f'field_{i}': v for i, v in enumerate(line.split(self.special_converter_add))}
                results.append(dict_line)
            except:
                pass
        return results

    def simple_json(self, lines: List[str]) -> List[Dict]:
        results = []
        for line in lines:
            try:
                results.append(ujson_loads(line))
            except:
                pass
        return results

    def save_default(self, record: Dict, port: int) -> Optional[Dict]:  # TODO rewrite with kwargs
        """

        :param json_input:
        :param port:
        :param logger:
        :return:
        """

        # TODO: rethink
        # region not good idea
        # if self.default_create_fields:
        #     set_of_record_fields = set(record.keys())
        #     set_default_fields = set(self.default_create_fields)
        #     _check = set_of_record_fields & set_default_fields
        #     if _check != set(self.default_create_fields):
        #         return None
        # endregion

        record_fields = ["ipv4", "ip_v4_int", "port", "datetime", "data"]
        ip_str = record.get('ip')
        ipv4_int = 0

        if ip_str:
            try:
                ipv4 = ip_address(ip_str)
                ipv4_int = int(ipv4)
            except Exception as exc:
                self.logger.error(exc)
                ipv4 = ip_str
        else:
            ipv4 = ip_str

        port_for_record = int(port)
        record_datetime = datetime.utcnow()

        _data = [str(ipv4), ipv4_int, port_for_record, record_datetime]
        result = dict(zip(record_fields, _data))
        if self.raw_record_format == 'json':
            result['data'] = record.get('data')
        elif self.raw_record_format == 'csv':
            result['data'] = record
        elif self.raw_record_format == 'python':
            pass
        for k in list(result.keys()):
            if not result[k]:
                result.pop(k)
        return result


    def save_json(self, record: Dict, port: int) -> Optional[Dict]:  # TODO rewrite with kwargs
        """

        :param json_input:
        :param port:
        :param logger:
        :return:
        """

        record_fields = ["port", "datetime"]
        try:
            port_for_record = int(port)
        except:
            port_for_record = None
        record_datetime = datetime.utcnow()

        _data = [port_for_record, record_datetime]
        result = dict(zip(record_fields, _data))
        result['data'] = record
        for k in list(result.keys()):
            if not result[k]:
                result.pop(k)
        return result

    def filter_default(self,
                       record: Dict) -> bool:
        """
        проверка на 'success'==value_filter из файла настроек в записе (default)
        :param record: record from standart parse
        :param config: application config
        :return:
        """
        result = False
        try:
            value_record: Any = return_value_from_dict_extended(record, self.standart_filter_path)
            value_filter: Any = self.standart_filter_value
            if (value_filter and value_record) and (value_record == value_filter):
                result = True
            elif check_iter(value_record) and value_filter:
                # result = any([value_filter == value for value in value_record])
                result = value_filter in value_record
        except Exception as exp:
            pass
        return result


@dataclass(frozen=True)
class AppConfig:
    senders: int
    number_lines: int
    mode_read_input: str
    port: int
    queue_sleep: int
    operations: RecordOperation
    input_file: str
    input_stdin: str
    collectors: dict
    sqs: dict
    size_bulk_mb: int
    try_retry_upload: int
    app_module: str
    packing_dict: dict
    geoip: dict
    source: dict
    statistics_file: str
    statistics: dict
    timeout_filter: int



def load_config(path):
    with Path(path).open(encoding='utf-8') as fp:
        return yaml_load(fp.read(), Loader=yaml_FullLoader)


def parse_args():
    parser = argparse.ArgumentParser(description='Upload data to ElasticMQ(SQS)')

    parser.add_argument('-settings',
                        type=str,
                        help='path to file with settings (yaml)',
                        required=True)

    parser.add_argument('-log',
                        type=str,
                        default='color',
                        help='log_format: color,json')

    parser.add_argument('-mode',
                        type=str,
                        dest='mode',
                        default='memory',
                        help='mode, how to read input file: memory, asyncio, default: memory')

    parser.add_argument('-statistics',
                        type=str,
                        dest='statistics',
                        help='save statistics to file')

    parser.add_argument('-multimodules',
                        type=str,
                        dest='multimodules',
                        default=CONST_PATH_TO_MODULES,
                        help='directory path for custom Python modules')

    parser.add_argument('-converters',
                        type=str,
                        dest='converters',
                        default=CONST_PATH_TO_CONVERTERS,
                        help='directory path for custom Python modules')
    # надо проверять наличие файла

    return parser.parse_args()


async def parse_settings(args: argparse.Namespace, logger) -> AppConfig:
    if args.settings:
        settings = await parse_settings_file(args, logger)
        return settings


# noinspection PyUnresolvedReferences
def load_function_from_pythonfile(py_module_path, function_name: str) -> Callable:
    """
    Imports generator function from single .py file
    """
    module_name_like_filename_py = str(py_module_path).rstrip('.py')
    spec = importlib.util.spec_from_file_location(module_name_like_filename_py, py_module_path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return getattr(m, function_name)


def load_function_from_pythonmodule(module_name: str, function_name: str) -> Callable:
    """
    Imports generator function from required module
    """
    _mod = importlib.import_module(module_name)
    return getattr(_mod, function_name)


# noinspection PyBroadException
def load_python_module(py_module_path: str, _name_function: str) -> Optional[Callable]:
    """
    Imports generator from python file OR module
    """
    if not py_module_path.endswith('.py'):
        py_module_path += '.py'
    _path_to_file = Path(py_module_path)
    if _path_to_file.exists() and _path_to_file.is_file():
        return load_function_from_pythonfile(_path_to_file, _name_function)


async def check_collector(collector: Dict,
                          logger) -> bool:
    """
    simple check of  connection to http Logcollector
    :param collector:
    :param logger:
    :return:
    """

    trace_request_ctx = {'logger': logger,
                         'name': collector['name'],
                         'check': True}
    result = False
    session = collector['session']
    url = collector['url']
    try:
        async with session.head(url, trace_request_ctx=trace_request_ctx) as response:
            http_status = response.status
            if http_status == 200:
                result = True
    except:
        pass
    return result


async def create_sqs_client(session: AioSession, exit_stack: AsyncExitStack, auth_struct: Dict):
    # Create client and add cleanup
    client = await exit_stack.enter_async_context(session.create_client(**auth_struct))
    return client


def create_default_tags_for_routes_messages(_sqs: Dict) -> Tuple[Dict, Dict]:

    endpoint = _sqs['endpoint'].strip('/')
    dest, database, space = endpoint.split('/')
    _tag = {dest: {'database': database,
                   'space': space}
            }

    tags = ujson_dumps(_tag).encode('utf-8')
    tags = standard_b64encode(tags).decode('utf-8')

    return {'routes': tags}, {'routes': _tag}


async def parse_settings_file(args: argparse.Namespace, logger) -> AppConfig:

    async def on_request_start(session, trace_config_ctx, params):
        trace_config_ctx.start = get_event_loop().time()

    async def on_request_end(session, trace_config_ctx, params):
        elapsed = get_event_loop().time() - trace_config_ctx.start
        if trace_config_ctx.trace_request_ctx:
            if 'logger' in trace_config_ctx.trace_request_ctx and 'check' in trace_config_ctx.trace_request_ctx:
                _name = trace_config_ctx.trace_request_ctx.get('name', '')
                _logger = trace_config_ctx.trace_request_ctx['logger']
                _logger.info("collector {} took request {}".format(_name, round(elapsed, 4)))
            elif 'request' in trace_config_ctx.trace_request_ctx:
                trace_config_ctx.trace_request_ctx['duration'] = round(elapsed, 4)

    filename_settings: str = args.settings
    try:
        config: Dict = load_config(filename_settings)
    except FileNotFoundError:
        logger.error(f'not found: {filename_settings}')
        exit(1)

    senders = config.get('senders', 1024)
    number_lines = config.get('number_lines', 1000)
    queue_sleep = config.get('queue_sleep', 1)
    try_retry_upload = config.get('try_retry_upload', 3)
    size_bulk_mb = config.get('size_bulk_mb', 4)
    if not isinstance(size_bulk_mb, int):
        if isinstance(size_bulk_mb, str):
            if size_bulk_mb.isdigit():
                size_bulk_mb = int(size_bulk_mb)
    if not isinstance(size_bulk_mb, int):
        size_bulk_mb = 4
    _collectors: Optional[Dict] = config.get('collectors')
    _sqs: Optional[Dict] = config.get('sqs')

    collectors = {}
    sqs = {}
    if not _collectors and not _sqs:
        logger.error('collectors settings not found, sqs settings not found, exit.')
        exit(1)
    if _collectors and _sqs:
        logger.error('incorrect settings found. There are settings for queues and collectors.\n'
                     'You only need to specify one method for sending data.')
        exit(1)
    elif _collectors:
        collectors_list = []
        collectors = {}
        _collectors: dict = config.get('collectors')
        trace_config = TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        for collector_name, value in _collectors.items():
            use_gzip: bool = value.get('gzip', False)
            headers = {'content-encoding': 'gzip'} if use_gzip else {}
            _collector = {'name': collector_name,
                          'url': value['endpoint'],
                          'use_gzip': use_gzip,
                          'session': ClientSession(auth=BasicAuth(value['user'], value['password']),
                                                   headers=headers,
                                                   json_serialize=ujson_dumps,
                                                   trace_configs=[trace_config]
                                                   )
                          }
            # TODO timeout
            check_collector_status = await check_collector(_collector, logger)
            if check_collector_status:
                collectors_list.append(_collector)
            else:
                await _collector['session'].close()
        if collectors_list:
            shuffle(collectors_list) # random shuffle list of collectors
            collectors = {'cycle': cycle(collectors_list),  # like simple round-robin
                          'list': collectors_list}
            logger.info(f'Count collectors: {len(collectors["list"])}')
        else:
            logger.error('All collectors hosts are down or no routes to hosts')
            exit(112)

        if not collectors:
            logger.error('errors with Collectors connections, exit.')
            exit(1)
    elif _sqs:
        using_elasticmq = False
        # 1.
        # default tags:
        tags = {}
        if 'yandex' not in _sqs['endpoint_url']:
            # endpoint = _sqs['endpoint'].strip('/')
            # dest, database, space = endpoint.split('/')
            # _tag = {dest: {'database': database,
            #                'space': space}
            #         }
            # _tag = ujson_dumps(_tag).encode('utf-8')
            # _tag = standard_b64encode(_tag).decode('utf-8')
            #
            # tags = {'routes': _tag}
            tags, _tags = create_default_tags_for_routes_messages(_sqs)
            try:
                _name_task = _sqs['name']
            except:
                # _name_task = space
                _name_task = _tags['dest']['space']
            currentuuid = _sqs['currentuuid']
            sqsname_queue = f'Results_{_name_task}_cuuid_{currentuuid}'

            if _sqs.get('region_name', '').lower() == 'elasticmq':
                if 'https' in _sqs['endpoint_url'] and _sqs['use_ssl'] \
                    and not (_sqs.get('aws_ca_bundle', False) and _sqs.get('client_crt', False) and _sqs.get('client_key', False)):
                    logger.error(f'using elasticmq and SSL, but not seted some of crt, ca, key, exit')
                    exit(1)
                else:
                    using_elasticmq = True
        else:
            if 'name_queue' not in _sqs:
                sqsname_queue = 'TargetsDev'
            else:
                sqsname_queue = _sqs['name_queue']

        keys = ['service_name', 'endpoint_url', 'region_name', 'aws_secret_access_key', 'aws_access_key_id', 'use_ssl']
        init_keys = dict().fromkeys(keys)
        for k in init_keys:
            if k in _sqs:
                init_keys[k] = _sqs[k]
        if using_elasticmq:
            client_crt = _sqs['client_crt']
            client_key = _sqs['client_key']
            config_elasticmq = AioConfig(client_cert=(client_crt, client_key))
            init_keys['config'] = config_elasticmq
            if not environ.get('AWS_CA_BUNDLE'):
                environ['AWS_CA_BUNDLE'] = _sqs['aws_ca_bundle']

        _session = AioSession()
        exit_stack = AsyncExitStack()
        client = await create_sqs_client(_session, exit_stack, auth_struct=init_keys)

        logger.info(f'Trying to create a client for a queue: {sqsname_queue}')
        queue_url = await return_queue_url_realtime(client, sqsname_queue, logger, tags=tags, auto_create=True)
        if not queue_url:
            logger.error(f"Uploader client can't create or access to queue: {sqsname_queue}")
            exit(1)
        else:
            logger.info(f'Queue client created: {queue_url}')
        sqs['url'] = queue_url
        sqs['init_keys'] = init_keys
        sqs['client'] = client
        sqs['endpoint'] = _sqs['endpoint']

    input_file = access_dot_path(config, 'input.file')
    input_stdin = access_dot_path(config, 'input.stdin')  # not implemented
    if input_file:
        if not Path(input_file).exists():
            logger.error(f'errors: "input.file" - file not found: {input_file}')
            exit(2)
    try:
        filename_packing_dict = config['app_module_schema']
        packing_dict = return_dict_for_packed_record(filename_packing_dict, logger)
    except Exception as exp:
        logger.error(exp)
        packing_dict = {}
    # region creat Reader for Geoip
    status_geoip = config.get('geoip')
    geoip = {}
    if status_geoip:
        # default paths for maxmind databases
        asn_filename_path = 'geo/GeoLite2-ASN.mmdb'
        city_filename_path = 'geo/GeoLite2-City.mmdb'
        try:
            if Path(asn_filename_path).exists() and Path(city_filename_path).exists() and status_geoip:
                status_geoip = True
            else:
                status_geoip = False
        except:
            logger.error('geoip: MaxmindDB files not founded')
            status_geoip = False
        if status_geoip:
            reader_asn = geoip2.database.Reader(asn_filename_path, mode=1)
            reader_city = geoip2.database.Reader(city_filename_path, mode=1)
            geoip = {'reader_asn': reader_asn,
                     'reader_city': reader_city}

    app_module = config.get('app_module')  # ??? need to rethink

    source_file = config.get('source')
    source_worker = {}
    try:
        if Path(source_file).exists():
            with open(source_file, 'rt') as f:
                source_worker = ujson_load(f)
    except Exception as exp:
        logger.error(f'file Dict-source(about machine) not exists: {exp}')

    mode_read_input = args.mode

    root_dir_to_modules = ''
    if os_path.isdir(args.multimodules):
        if os_access(args.multimodules, R_OK):
            root_dir_to_modules = args.multimodules
    if not root_dir_to_modules:
        logger.error(f'directory for modules not found or error with read: {args.multimodules}')
        exit(2)
    root_dir_to_converters = args.converters
    if os_path.isdir(CONST_PATH_TO_CONVERTERS):
        if os_access(CONST_PATH_TO_CONVERTERS, R_OK):
            root_dir_to_converters = CONST_PATH_TO_CONVERTERS
    if not root_dir_to_converters:
        logger.error(f'directory for converters not found or error with read: {root_dir_to_converters}')
        exit(2)
    settings_converter = access_dot_path(config, 'converter')
    settings_create_record = access_dot_path(config, 'create_record')
    settings_filter_record = access_dot_path(config, 'standart_filter')
    settings_custom_filter_record = access_dot_path(config, 'custom_filter')
    use_standart_filter = access_dot_path(config, 'use_standart_filter', False)
    use_custom_filter = access_dot_path(config, 'use_custom_filter', False)

    settings_for_records = RecordOperation(settings_converter,
                                           settings_create_record,
                                           settings_filter_record,
                                           settings_custom_filter_record,
                                           use_standart_filter,
                                           use_custom_filter,
                                           root_dir_to_modules,
                                           root_dir_to_converters,
                                           logger
                                           )
    timeout_filter = config.get('timeout_filter', 7)
    app_settings = AppConfig(**{
        'senders': senders,
        'number_lines': number_lines,
        'mode_read_input': mode_read_input,
        'queue_sleep': queue_sleep,
        'operations': settings_for_records,
        'input_file': '' if not input_file else input_file,
        'input_stdin': '' if not input_stdin else input_stdin,
        'collectors': collectors if collectors else {},  # TODO: спорный момент, обдумать
        'sqs': sqs if sqs else {},  # TODO: спорный момент, обдумать
        'size_bulk_mb': size_bulk_mb,
        'port': int(config['port']),
        'geoip': geoip,
        'timeout_filter': timeout_filter,
        'app_module': app_module,
        'packing_dict': packing_dict,
        'try_retry_upload': try_retry_upload,
        'source': source_worker,
        'statistics_file': '' if not args.statistics else args.statistics,
        'statistics': {'all_records': 0,
                       'valid_records': 0,
                       'duration': 0,
                       'size': 0}
        }
                             )
    return app_settings

