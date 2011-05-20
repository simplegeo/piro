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

def raise_cassandra_scores(hosts):
    pass
