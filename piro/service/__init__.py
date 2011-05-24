"""Service control functions."""

import sys
import time

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated

from piro.service import monit
import piro.util as util


from cassandra import Cassandra
from cassandra.ttypes import *

def start(hosts, service=None, **kwargs):
    """
    Attempts to dispatch to a service-specific start function. If
    one cannot be found, dispatches to a generic start function
    instead.
    """
    try:
        service_fn = getattr(sys.modules['piro.service'],
                             'start_%s' % service.lower().replace('-', '_'))
    except AttributeError:
        service_fn = generic_start
    return service_fn(hosts, service=service, **kwargs)

def generic_start(hosts, service=None, **kwargs):
    """
    Generic start function. Attempts to use monit to start the
    specified service.
    """
    for host in hosts:
        util.print_status((host, monit.start(service, util.hostname(host))))

def stop(hosts, service=None, **kwargs):
    """
    Attempts to dispatch to a service-specific stop function. If
    one cannot be found, dispatches to a generic stop function
    instead.
    """
    try:
        service_fn = getattr(sys.modules['piro.service'],
                             'stop_%s' % service.lower().replace('-', '_'))
    except AttributeError:
        service_fn = generic_stop
    return service_fn(hosts, service=service, **kwargs)

def generic_stop(hosts, service=None, **kwargs):
    """
    Generic stop function. Attempts to use monit to stop the
    specified service.
    """
    for host in hosts:
        util.print_status((host, monit.stop(service, util.hostname(host))))

def restart(hosts, service=None, **kwargs):
    """
    Attempts to dispatch to a service-specific restart function. If
    one cannot be found, dispatches to a generic restart function
    instead.
    """
    try:
        service_fn = getattr(sys.modules['piro.service'],
                             'restart_%s' % service.lower().replace('-', '_'))
    except AttributeError:
        service_fn = generic_restart
    return service_fn(hosts, service=service, **kwargs)

def generic_restart(hosts, service=None, **kwargs):
    """
    Generic restart function. Attempts to use monit to restart the
    specified service.
    """
    for host in hosts:
        util.print_status((host, monit.restart(service, util.hostname(host))))

def status(hosts, service=None, **kwargs):
    """
    Attempts to dispatch to a service-specific status function. If
    one cannot be found, dispatches to a generic status function
    instead.
    """
    try:
        service_fn = getattr(sys.modules['piro.service'],
                             'status_%s' % service.lower().replace('-', '_'))
    except AttributeError:
        service_fn = generic_status
    return service_fn(hosts, service=service, **kwargs)

def generic_status(hosts, service=None, **kwargs):
    """
    Generic status function. Attempts to use monit to get the
    status of the specified service.
    """
    for host in hosts:
        util.print_status((host, monit.status(service, util.hostname(host))))

### Put non-generic services here. ###
def start_simplegeo_cassandra(hosts, args=None, **kwargs):
    """
    Service-specific start function for simplegeo-cassandra.

    First, clears any DES score over-rides, then starts the
    simplegeo-cassandra service using monit and blocks waiting on a
    response to a thrift method call until the call succeeds or a
    specified timeout elapses.
    """
    for host in hosts:
        util.clear_cassandra_score(host)
        util.print_status((host, monit.start('simplegeo-cassandra',
                                             util.hostname(host))))
        response = wait_for_cassandra_response(host,
                                               args.timeout)
        util.print_status(('', ('keyspaces', response)))

def stop_simplegeo_cassandra(hosts, args=None, **kwargs):
    """
    Service-specific stop function for simplegeo-cassandra.

       First, sets a DES score over-ride in order to black-list the
       hosts we are about to stop simplegeo-cassandra on. Then,
       disables puppet on those hosts so puppet does not restart the
       service. Finally, blocks waiting for verification that monit
       has sucessfully stopped the service.

       If one of the specified clusto pools is 'production', this
       function will disable the home AZ of the hosts in the specified
       pools. It will refuse to stop the simplegeo-cassandra service
       on hosts in more than one AZ.
       """
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

def wait_for_cassandra_response(host, timeout):
    """
    Given a cassandra host and a timeout, attempt to connect to the
    host via thrift and call describe_keyspaces(), blocking until the
    call succeeds or the timeout elapses.

    """
    start_time = int(time.time())
    timeout_time = start_time + timeout
    cassandra_alive = False
    host = util.hostname(host)
    while (not cassandra_alive):
        now = int(time.time())
        if (now >= timeout_time):
            cassandra_alive = 'timeout'
            continue
        try:
            socket = TSocket.TSocket(host, 9160)
            socket.setTimeout(timeout * 1000)
            transport = TTransport.TFramedTransport(socket)
            protocol = TBinaryProtocolAccelerated(transport)
            client = Cassandra.Client(protocol)
            transport.open()
        except Thrift.TException:
            transport.close()
            continue
        try:
            cassandra_alive = client.describe_keyspaces()
        except Thrift.TException, exc:
            cassandra_alive = exc
            transport.close()
            continue
    return cassandra_alive

def restart_simplegeo_cassandra(hosts, args=None, **kwargs):
    """
    Service-specific restart function for simplegeo-cassandra.

    First, sets a DES score over-ride in order to black-list the hosts
    we are about to restart simplegeo-cassandra on. Then, disables
    puppet on those hosts so puppet does not restart the service while
    we are trying to restart it. Finally, blocks waiting for
    verification that the service is back up and responding to thrift
    requests.

    If one of the specified clusto pools is 'production', this
    function will iterate through availability zones, disabling each
    AZ before restarting simplegeo-cassandra on hosts in that AZ, then
    re-enabling that AZ before moving on to the next. If any restart
    fails, or if disabling or re-enabling the AZ fails, this function
    will refuse to move on to the next host or AZ.
    """
    hosts = util.hosts_by_az(hosts)
    for az in hosts.keys():
        if args.prod and not util.disable_az(az):
            print 'Could not disable AZ %s' % az
            print 'Cowardly refusing to continue production restart'
            break
        for host in hosts[az]:
            util.set_cassandra_score(host)
            util.disable_puppet(util.hostname(host))
            util.print_status((host, monit.stop('simplegeo-cassandra',
                                           util.hostname(host),
                                           wait=True)))
            util.print_status((host, monit.start('simplegeo-cassandra',
                                            util.hostname(host))))
            response = wait_for_cassandra_response(host, args.timeout)
            util.print_status(('', ('keyspaces', response)))
            if 'system' not in response:
                util.print_status(('', ('ERROR', 'Could not verify '
                                        'simplegeo-cassandra startup '
                                        'on %s' % host.name)))
                if args.prod:
                    print
                    print 'Cowardly refusing to continue production restart'
                    break
        if args.prod and not util.enable_az(az):
            print 'Could not re-enable AZ %s' % az
            print 'Cowardly refusing to continue production restart'
            break

def status_simplegeo_cassandra(hosts, args=None, **kwargs):
    """
    Service-specific status function for simplegeo-cassandra.

    This function gets the monit status of the service as well as
    verifying that each host is responding to thrift requests.
    """
    for host in hosts:
        util.print_status((host, monit.status('simplegeo-cassandra',
                                              util.hostname(host))))
        util.print_status(('', ('keyspaces',
                                (wait_for_cassandra_response(host,
                                                             args.timeout)))))
