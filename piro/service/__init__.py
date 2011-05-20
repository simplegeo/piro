from piro.service import monit
import piro.util as util

SERVICES=['cassandra', 'nagios-nrpe-server']
ACTIONS=['start', 'stop', 'restart', 'status']

def start_cassandra(hosts, prod=False):
    pass

def stop_cassandra(hosts, prod=False):
    pass

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

def status_nagios_nrpe_server(hosts, **kwargs):
    statuses = [(host, monit.status('nagios-nrpe-server', util.hostname(host)))
                for host in hosts]
    for (host, (status, message)) in statuses:
        print '%s\t%s\t\t%s' % (host, status, message)

def start_nagios_nrpe_server(hosts, **kwargs):
    statuses = []
    for host in hosts:
        statuses.append((host, monit.start('nagios-nrpe-server',
                                           util.hostname(host))))
    for (host, (status, message)) in statuses:
        print '%s\t%s\t\t%s' % (host, status, message)

def stop_nagios_nrpe_server(hosts, **kwargs):
    statuses = []
    for host in hosts:
        statuses.append((host, monit.stop('nagios-nrpe-server',
                                          util.hostname(host))))
    for (host, (status, message)) in statuses:
        print '%s\t%s\t\t%s' % (host, status, message)

def status_cassandra(hosts, **kwargs):
    statuses = [(host, monit.status('nagios-nrpe-server', util.hostname(host)))
                for host in hosts]
    for (host, (status, message)) in statuses:
        print '%s\t%s\t%s' % (host, status, message)
