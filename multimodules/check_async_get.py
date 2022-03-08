#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "SAI"
__license__ = "GPLv3"
__email__ = "andrew.foma@gmail.com"
__status__ = "Dev"


async def main(**kwargs) -> dict:
    from aiohttp import ClientSession
    from typing import Optional, Any

    def access_dot_path(dictionary: dict, path: str) -> Optional[Any]:
        """
        Access dot-separated path in dictionary or return None
        """
        dot_index = path.find('.')
        if dot_index == -1:  # no more dots in path
            return dictionary.get(path)
        previous = path[:dot_index]  # key before first dot
        if previous not in dictionary:
            return None
        element = dictionary[previous]
        if isinstance(element, dict):
            return access_dot_path(element, path[dot_index + 1:])

    try:
        record = kwargs.get('record')
        # example path to field: data.tls.result.handshake_log.server_certificates.certificate.parsed.subject_dn
        path_to_field = 'data.tls.result.handshake_log.server_certificates.certificate.parsed.subject_dn'
        if record:
            if isinstance(record, dict):
                _need_example_value = access_dot_path(record, path_to_field)
                if _need_example_value:
                    if isinstance(_need_example_value, str):
                        # filter that domain is .de
                        if _need_example_value.endswith('de'):
                            _ip = ''
                            if 'ip' in record:
                                _ip = record['ip']
                            elif 'ipv4' in record:
                                _ip = record['ipv4']
                            if _ip:
                                async with ClientSession() as session:
                                    async with session.get(url=f'https://{_ip}', ssl=False) as resp:
                                        status = resp.status
                                        # if domain response on GET - return record
                                        if status == 200:
                                            return record
    except:
        pass