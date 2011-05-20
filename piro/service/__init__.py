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
    print_status(statuses)

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
    print_status(statuses)

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
    print_status(statuses)

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
    print_status(statuses)

### Put non-generic services here. ###
def start_cassandra(hosts, timeout=120, **kwargs):
    for host in hosts:
        util.clear_cassandra_score(host)
        print_status((host, monit.start('cassandra'
                                        util.hostname(host))))
        print 'waiting for thrift response from %s' % host
        response = wait_for_cassandra_response(host, timeout)
        print_status((host, response))

def stop_cassandra(hosts, prod=False, **kwargs):
    azs = util.hosts_by_az(hosts).keys()
    if prod and len(azs) > 1:
        print 'cannot stop production cassandra in multiple AZs!'
        return
    elif prod:
        util.disable_az(azs[0])
    for host in hosts:
        util.set_cassandra_score(host)
        util.disable_puppet(util.hostname(host)):
        print_status((host, monit.stop('cassandra'
                                       util.hostname(host),
                                       wait=True)))

def wait_for_cassandra_response(host, timeout):
    start = int(time.time())
    cassandra_alive = (False, 'no response from cassandra')
    host = util.hostname(host)
    socket = TSocket.TSocket(host, 9160)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    while (not cassandra_alive):
        try:
            transport.open()
            cassandra_alive = client.get_ring_state()
            break
        except Thrift.TException:
            continue
        finally:
            transport.close()
        if (int(time.time()) > (start + timeout)):
            cassandra_alive = ('timeout',
                               'no response from cassandra in %ss' % timeout)
            break
    return cassandra_alive

def restart_cassandra(hosts, prod=False, timeout=120, **kwargs):
    hosts = util.hosts_by_az(hosts)
    for az in hosts.keys():
        if prod:
            util.disable_az(az)
        for host in hosts[az]:
            util.set_cassandra_score(host)
            util.disable_puppet(util.hostname(host)):
            print_status((host, monit.stop('cassandra',
                                           util.hostname(host),
                                           wait=True)))
            print_status((host, monit.start('cassandra',
                                            util.hostname(host))))
            print 'waiting for thrift response from %s' % host
            response = wait_for_cassandra_response(host, timeout)
            print_status((host, response))

def status_cassandra(hosts, timeout=120, **kwargs):
    statuses = [(host, monit.status('simplegeo-cassandra', util.hostname(host)))
                for host in hosts]
    statuses += [(host, ('ring_state', wait_for_cassandra_response(host, timeout)))
                 for host in hosts]
    print_status(sorted(statuses))
