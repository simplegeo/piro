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
def start_simplegeo_cassandra(hosts, args=None, **kwargs):
    for host in hosts:
        util.clear_cassandra_score(host)
        util.print_status((host, monit.start('simplegeo-cassandra',
                                             util.hostname(host))))
        response = wait_for_cassandra_response(host, args.keyspace, args.timeout)
        util.print_status(('', ('keyspaces', response)))

def stop_simplegeo_cassandra(hosts, args=None, **kwargs):
    azs = util.hosts_by_az(hosts).keys()
    if args.prod and len(azs) > 1:
        print 'cannot stop production cassandra in multiple AZs!'
        return
    elif args.prod:
        util.disable_az(azs[0])
    for host in hosts:
        util.set_cassandra_score(host)
        util.disable_puppet(util.hostname(host))
        util.print_status((host, monit.stop('simplegeo-cassandra',
                                       util.hostname(host),
                                       wait=True)))

def wait_for_cassandra_response(host, keyspace, timeout):
    start = int(time.time())
    cassandra_alive = False
    host = util.hostname(host)
    while (not cassandra_alive):
        if (int(time.time()) > (start + timeout)):
            cassandra_alive = 'timeout'
            transport.close()
            continue
        try:
            socket = TSocket.TSocket(host, 9160)
            transport = TTransport.TFramedTransport(socket)
            protocol = TBinaryProtocolAccelerated(transport)
            client = Cassandra.Client(protocol)
            transport.open()
        except Thrift.TException:
            transport.close()
            continue
        try:
            cassandra_alive = client.describe_keyspaces()
        except Thrift.TException, e:
            cassandra_alive = e
            transport.close()
            continue
    return cassandra_alive

def restart_simplegeo_cassandra(hosts, args=None, **kwargs):
    hosts = util.hosts_by_az(hosts)
    for az in hosts.keys():
        if args.prod:
            util.disable_az(az)
        for host in hosts[az]:
            util.set_cassandra_score(host)
            util.disable_puppet(util.hostname(host))
            util.print_status((host, monit.stop('simplegeo-cassandra',
                                           util.hostname(host),
                                           wait=True)))
            util.print_status((host, monit.start('simplegeo-cassandra',
                                            util.hostname(host))))
            response = wait_for_cassandra_response(host, args.keyspace,
                                                   args.timeout)
            util.print_status(('', ('keyspaces', response)))

def status_simplegeo_cassandra(hosts, args=None, **kwargs):
    for host in hosts:
        util.print_status((host, monit.status('simplegeo-cassandra',
                                              util.hostname(host))))
        util.print_status(('', ('keyspaces',
                                (wait_for_cassandra_response(host,
                                                             args.keyspace,
                                                             args.timeout)))))
