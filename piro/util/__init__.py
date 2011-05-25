"""Utility functions for piro."""

from collections import Sequence, Set
import sys
from time import sleep, time

import piro.clustohttp as clusto
from piro.util.amazinghorse import AmazingHorse

CLUSTO = clusto.ClustoProxy('http://clusto.simplegeo.com/api')

class NoContentException(Exception):
    """
    Exception class for when a call to an HTTP endpoint returns an
    empty response.
    """
    pass

def get_contents(pool):
    """Given a clusto pool, return the set of entities that pool
    contains."""
    return set(CLUSTO.get_by_name(pool).contents())

def get_hosts(pools):
    """Given an iterable containing clusto pools, return the set of
    entities contained by all of those pools."""
    return set.intersection(*map(get_contents, pools))

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

def get_piro_password():
    """
    Fetch piro's password from clusto.
    """
    # Obviously this isn't fetched from clusto yet. It will be.
    return 'piro'

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

def disable_puppet(host):
    """
    Given a host, disable puppet on that host.
    """
    pass

def enable_puppet(host):
    """
    Given a host, enable puppet on that host.
    """
    pass

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
