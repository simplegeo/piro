from collections import Sequence, Set
import sys

import piro.clustohttp as clusto

CLUSTO = clusto.ClustoProxy('http://clusto.simplegeo.com/api')

def hosts_by_az(hosts):
    result = {}
    for host in hosts:
        zones = [z for z in host.parents() if z.type == 'zone']
        if len(zones) < 1:
            raise Exception
        elif len(zones) > 1:
            raise Exception
        else:
            zone = zones[0]
        if zone not in result:
            result[zone] = []
        result[zone] += [host]
    return result

def hostname(host):
    return host.attr_value(key='ec2', subkey='public-dns')

def disable_az(az):
    pass

def set_cassandra_score(host):
    pass

def clear_cassandra_score(host):
    pass

def _print_status(status):
    (host, (status, message)) = status
    if type(host) is str:
        host = host.rjust(len(host) + 15)
    else:
        host = host.name
        host = host.ljust(len(host) + 5)
    status = status.ljust(15)
    if not isinstance(message, (str, unicode)) and isinstance(message,
                                                              (Sequence, Set)):
        message = ', '.join(message)
    print '%s%s%s' % (host, status, message)

def print_status(statuses):
    if type(statuses) is tuple:
        _print_status(statuses)
    else:
        for status in statuses:
            _print_status(status)

def disable_puppet(host):
    # sys.stderr.write('disabling puppet...')
    # sys.stderr.write('not implemented yet\n')
    # sys.stderr.flush()
    pass
