"""Utility functions for piro."""

from collections import Sequence, Set
from time import sleep, time
import urllib2 as url

import piro.clustohttp as clusto
from piro.util.amazinghorse import AmazingHorse

CLUSTO_API  = 'http://clusto.simplegeo.com/api'
UTILITY_API = 'https://utility-api.simplegeo.com:8443/0.1/puppet'


def get_contents(pool, username=None, password=''):
    """Given a clusto pool, return the set of entities that pool
    contains."""
    return set(clusto.ClustoProxy(CLUSTO_API,
                                  username=username,
                                  password=password).get_by_name(pool).contents())

def get_hosts(pools, username=None, password=''):
    """Given an iterable containing clusto pools, return the set of
    entities contained by all of those pools."""
    return set.intersection(*map(partial(get_contents,
                                         username=username,
                                         password=password),
                                 pools))

def hosts_by_az(hosts):
    """
    Given a list of clusto host entities, return a dictionary mapping
    AZs to the hosts from the list which are located in the AZ.
    """
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
    """
    Given a clusto host entity, return the ec2 public DNS name for that host.
    """
    return host.attr_value(key='ec2', subkey='public-dns')

def disable_az(az, args):
    """
    Given an AZ, disable that AZ in the ELB using amazinghorse.
    """
    client = AmazingHorse(username=args.username, password=args.password)
    client.disable_availability_zone(az, 'API')
    status = az in client.get_availability_zones('API')
    start_time = int(time())
    # ELB requests usually take 2 minutes or less, timeout after 3
    timeout_time = start_time + 180
    while status:
        now = int(time())
        if (now >= timeout_time):
            break
        sleep(.5)
        status = az in client.get_availability_zones('API')
    return not status

def enable_az(az, args):
    """
    Given an AZ, enable that AZ in the ELB using amazinghorse.
    """
    client = AmazingHorse(username=args.username, password=args.password)
    client.disable_availability_zone(az, 'API')
    status = az not in client.get_availability_zones('API')
    start_time = int(time())
    # ELB requests usually take 2 minutes or less, timeout after 3
    timeout_time = start_time + 180
    while status:
        now = int(time())
        if (now >= timeout_time):
            break
        sleep(.5)
        status = az not in client.get_availability_zones('API')
    return not status

def set_cassandra_score(host):
    """
    Given a host, override the DES score on that host.
    """
    pass

def clear_cassandra_score(host):
    """
    Given a host, clear any DES score over-rides on that host.
    """
    pass

def enable_puppet(host):
    """
    Given a host, enable puppet on that host.
    """
    req = url.Request(UTILITY_API + '/instance/%s.json?state=enabled' % host.name)
    res = url.urlopen(req, timeout=1)
    data = res.read()
    if not data:
        raise NoContentException('No content from server')
    if data == 1:
        return True
    else:
        return False

def disable_puppet(host):
    """
    Given a host, disable puppet on that host.
    """
    req = url.Request(UTILITY_API + '/instance/%s.json?state=disabled' % host.name)
    res = url.urlopen(req, timeout=1)
    data = res.read()
    if not data:
        raise NoContentException('No content from server')
    if data == "OK":
        return True
    else:
        return False

def _print_status(status):
    """
    Pretty-print status tuples.
    """
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
    """
    Pretty-print status tuples or lists of status tuples.
    """
    if type(statuses) is tuple:
        _print_status(statuses)
    else:
        for status in statuses:
            _print_status(status)
