def main(**kwargs):
    """
    для разбора записей в виде ip-addresses
    """
    from ipaddress import ip_address
    from collections.abc import Iterable
    results = []
    try:
        lines = kwargs.get('lines')
    except:
        pass
    else:
        if lines:
            if isinstance(lines, Iterable):
                for line in lines:
                    try:
                        v = ip_address(line)
                    except:
                        pass
                    else:
                        _record = {'ip': str(v)}
                        results.append(_record)
    return results
