import sys
import time

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated

from piro.service import monit
import piro.util as util

from piro.thrift.cassandra import Cassandra
from piro.thrift.cassandra.ttypes import *

# I need to make the generic call signature match the specific call
# signature. Otherwise I'm going to unintentionally catch
# AttributeErrors lower down the call stack.
def start(hosts, service=None, **kwargs):
    try:
        service_fn = getattr(sys.modules['piro.service'],
                             'start_%s' % service.lower().replace('-', '_'))
    except AttributeError:
        service_fn = generic_start
    return service_fn(hosts, service=service, **kwargs)

def generic_start(hosts, service=None, **kwargs):
    for host in hosts:
        util.print_status((host, monit.start(service, util.hostname(host))))

def stop(hosts, service=None, **kwargs):
    try:
        service_fn = getattr(sys.modules['piro.service'],
                             'stop_%s' % service.lower().replace('-', '_'))
    except AttributeError:
        service_fn = generic_stop
    return service_fn(hosts, service=service, **kwargs)

def generic_stop(hosts, service=None, **kwargs):
    for host in hosts:
        util.print_status((host, monit.stop(service, util.hostname(host))))

def restart(hosts, service=None, **kwargs):
    try:
        service_fn = getattr(sys.modules['piro.service'],
                             'restart_%s' % service.lower().replace('-', '_'))
    except AttributeError:
        service_fn = generic_restart
    return service_fn(hosts, service=service, **kwargs)

def generic_restart(hosts, service=None, **kwargs):
    for host in hosts:
        util.print_status((host, monit.restart(service, util.hostname(host))))

def status(hosts, service=None, **kwargs):
    try:
        service_fn = getattr(sys.modules['piro.service'],
                             'status_%s' % service.lower().replace('-', '_'))
    except AttributeError:
        service_fn = generic_status
    return service_fn(hosts, service=service, **kwargs)

def generic_status(hosts, service=None, **kwargs):
    for host in hosts:
        util.print_status((host, monit.status(service, util.hostname(host))))

### Put non-generic services here. ###
def start_cassandra(hosts, timeout=120, **kwargs):
    for host in hosts:
        util.clear_cassandra_score(host)
        util.print_status((host, monit.start('cassandra',
                                        util.hostname(host))))
        print 'waiting for thrift response from %s' % host
        response = wait_for_cassandra_response(host, timeout)
        util.print_status((host, response))

start_simplegeo_cassandra = start_cassandra

def stop_cassandra(hosts, prod=False, **kwargs):
    azs = util.hosts_by_az(hosts).keys()
    if prod and len(azs) > 1:
        print 'cannot stop production cassandra in multiple AZs!'
        return
    elif prod:
        util.disable_az(azs[0])
    for host in hosts:
        util.set_cassandra_score(host)
        util.disable_puppet(util.hostname(host))
        util.print_status((host, monit.stop('cassandra',
                                       util.hostname(host),
                                       wait=True)))

stop_simplegeo_cassandra = stop_cassandra

def wait_for_cassandra_response(host, timeout):
    start = int(time.time())
    cassandra_alive = (False, 'no response from cassandra')
    host = util.hostname(host)
    socket = TSocket.TSocket(host, 9160)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    while (not cassandra_alive[0]):
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
    print cassandra_alive
    return cassandra_alive

def restart_cassandra(hosts, prod=False, timeout=120, **kwargs):
    hosts = util.hosts_by_az(hosts)
    for az in hosts.keys():
        if prod:
            util.disable_az(az)
        for host in hosts[az]:
            util.set_cassandra_score(host)
            util.disable_puppet(util.hostname(host))
            util.print_status((host, monit.stop('cassandra',
                                           util.hostname(host),
                                           wait=True)))
            util.print_status((host, monit.start('cassandra',
                                            util.hostname(host))))
            print 'waiting for thrift response from %s' % host
            response = wait_for_cassandra_response(host, timeout)
            util.print_status((host, response))

restart_simplegeo_cassandra = restart_cassandra

def status_cassandra(hosts, timeout=120, **kwargs):
    statuses = [(host, monit.status('simplegeo-cassandra', util.hostname(host)))
                for host in hosts]
    statuses += [(host, ('ring_state', wait_for_cassandra_response(host, timeout)))
                 for host in hosts]
    util.print_status(sorted(statuses))

status_simplegeo_cassandra = status_cassandra
