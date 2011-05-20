import sys

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated

from piro.service import monit
import piro.util as util

from piro.thrift.cassandra import Cassandra
from piro.thrift.cassandra.ttypes import *

def start(service, hosts, **kwargs):
    try:
        service_fn = service.lower().replace('-', '_')
        return getattr(sys.modules['piro.service'],
                       'start_%s' % service_fn)(hosts, **kwargs)
    except AttributeError:
        return generic_start(service, hosts, **kwargs)

def generic_start(service, hosts, **kwargs):
    statuses = []
    for host in hosts:
        statuses.append((host, monit.start(service, util.hostname(host))))
    for (host, (status, message)) in statuses:
        print '%s\t%s\t\t%s' % (host, status, message)

def stop(service, hosts, **kwargs):
    try:
        service_fn = service.lower().replace('-', '_')
        return getattr(sys.modules['piro.service'],
                       'stop_%s' % service_fn)(hosts, **kwargs)
    except AttributeError:
        return generic_stop(service, hosts, **kwargs)

def generic_stop(service, hosts, **kwargs):
    statuses = []
    for host in hosts:
        statuses.append((host, monit.stop(service, util.hostname(host))))
    for (host, (status, message)) in statuses:
        print '%s\t%s\t\t%s' % (host, status, message)

def restart(service, hosts, **kwargs):
    try:
        service_fn = service.lower().replace('-', '_')
        return getattr(sys.modules['piro.service'],
                       'restart_%s' % service_fn)(hosts, **kwargs)
    except AttributeError:
        return generic_restart(service, hosts, **kwargs)

def generic_restart(service, hosts, **kwargs):
    statuses = []
    for host in hosts:
        statuses.append((host, monit.restart(service, util.hostname(host))))
    for (host, (status, message)) in statuses:
        print '%s\t%s\t\t%s' % (host, status, message)

def status(service, hosts, **kwargs):
    try:
        service_fn = service.lower().replace('-', '_')
        return getattr(sys.modules['piro.service'],
                       'status_%s' % service_fn)(hosts, **kwargs)
    except AttributeError:
        return generic_status(service, hosts, **kwargs)

def generic_status(service, hosts, **kwargs):
    statuses = [(host, monit.status(service, util.hostname(host)))
                for host in hosts]
    for (host, (status, message)) in statuses:
        print '%s\t%s\t\t%s' % (host, status, message)

### Put non-generic services here. ###

def start_cassandra(hosts, prod=False):
    print "Not implemented yet."

def stop_cassandra(hosts, prod=False):
    print "Not implemented yet."

# This is obviously incomplete. Do not use.
def restart_cassandra(hosts, prod=False):
    hosts = util.hosts_by_az(hosts)
    for az in hosts.keys():
        if prod:
            util.disable_az(az)
            util.raise_cassandra_scores(hosts[az])
        for host in hosts[az]:
            monit.stop('cassandra', host, wait=True)

def status_cassandra(hosts, **kwargs):
    statuses = [(host, monit.status('simplegeo-cassandra', util.hostname(host)))
                for host in hosts]
    for (host, (status, message)) in statuses:
        print '%s\t%s\t%s' % (host, status, message)
