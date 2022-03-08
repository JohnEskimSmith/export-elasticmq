

def main(**kwargs):
    """
    для разбора записей в виде massdns Type A только с ipv4
    """
    from ipaddress import IPv4Address
    from ujson import loads as ujson_loads
    from collections.abc import Iterable
    import datetime
    from functools import lru_cache
    from typing import Tuple

    @lru_cache(maxsize=100)
    def return_timestamp(need_date):
        day = need_date.toordinal()
        day = int(datetime.datetime.fromordinal(day).timestamp())
        return day

    @lru_cache(maxsize=64000)
    def convert_ip(value: str) -> int:
        try:
            ip = IPv4Address(value.strip())
            return int(ip)
        except:
            pass

    def convert_domain(value: str) -> Tuple[str, str, str]:
        try:
            domain_parts = value.split('.')
            tld = domain_parts[-1]
            domain = domain_parts[-2]
            subdomain = value[:-len(tld) - len(domain) - 1].strip('.')
            tld = tld if tld else ''
            domain = domain if domain else ''
            subdomain = subdomain if subdomain else ''
            return tld, domain, subdomain
        except:
            pass

    results = []
    try:
        lines = kwargs.get('lines')
    except:
        pass
    else:
        if lines:
            if isinstance(lines, Iterable):
                records = filter(lambda record: record['status'] == 'NOERROR', (ujson_loads(line) for line in lines))
                records = filter(lambda record: record['data'], records)
                for record in records:
                    if 'answers' in record['data']:
                        if isinstance(record['data']['answers'], list):
                            rows = filter(lambda z: z['type']=='A', record['data']['answers'])
                            for row in rows:
                                ip_v4_int = convert_ip(row.get('data'))
                                if ip_v4_int:
                                    domain = row['name'].lower().strip('.')
                                    name_parts = convert_domain(row['name'].lower().strip('.'))
                                    if name_parts:
                                        tld, name, subdomain = name_parts
                                        need_day = return_timestamp(datetime.datetime.now().date())
                                        result_struct = {'_id':
                                            {
                                                'domain': domain,
                                                'ip_v4_int': ip_v4_int
                                            },
                                            'tld': tld,
                                            'name': name,
                                            f'z_{need_day}': None}
                                        if subdomain:
                                            result_struct['sub'] = subdomain
                                        results.append(result_struct)

    return results

# f = '/home/user/Wave/Digitalwave-ServiceWorkerSingle/bin/results_raw_massdns.txt'
# lines = [line[:-1] for line in open(f, 'r')]
# values = main(lines=lines)
# for value in values:
#     print(value)