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

def print_status(statuses):
    if type(statuses) is tuple:
        print '%s\t%s\t\t%s' % (host, status, message)
    else:
        for (host, (status, message)) in statuses:
            print '%s\t%s\t\t%s' % (host, status, message)

def disable_puppet(host):
    print 'disabling puppet...'
    print '\tnot implemented yet'
