from decimal import Decimal
import re
import sys
from time import sleep
from urllib import urlencode
import urllib2
from xml.etree import ElementTree

def stop(service, host, wait=False):
    data = urlencode({'action': 'stop'})
    req = urllib2.Request('http://%s:2812/%s' % (host, service), data=data,
                          headers={'Authorization': 'Basic YWRtaW46bW9uaXQ='})
    urllib2.urlopen(req, timeout=1)
    if not wait:
        return ('success', 'stop signal sent to service %s' % service)
    else:
        svc_status = None
        while svc_status != 'stopped':
            sleep(.1)
            try:
                svc_status = status(service, host)[0]
            except Exception, e:
                pass
        return ('success', 'service %s is stopped' % service)

def start(service, host, wait=False):
    data = urlencode({'action': 'start'})
    req = urllib2.Request('http://%s:2812/%s' % (host, service), data=data,
                          headers={'Authorization': 'Basic YWRtaW46bW9uaXQ='})
    urllib2.urlopen(req, timeout=1)
    if not wait:
        return ('success', 'start signal sent to service %s' % service)
    else:
        svc_status = None
        while svc_status != 'running':
            sleep(.1)
            try:
                svc_status = status(service, host)[0]
            except Exception, e:
                pass
        return ('success', 'service %s is running' % service)

def restart(service, host, wait=False):
    data = urlencode({'action': 'restart'})
    req = urllib2.Request('http://%s:2812/%s' % (host, service), data=data,
                          headers={'Authorization': 'Basic YWRtaW46bW9uaXQ='})
    urllib2.urlopen(req, timeout=1)
    if not wait:
        return ('success', 'restart signal sent to service %s' % service)
    else:
        while status(service, host)[0] != 'running':
            sleep(.1)
        return ('success', 'service %s is running' % service)

def status(service, host):
    req = urllib2.Request('http://%s:2812/_status?format=xml' % host,
                          headers={'Authorization': 'Basic YWRtaW46bW9uaXQ='})
    res = urllib2.urlopen(req, timeout=1)
    data = res.read()
    if not data:
        raise Exception('No content from server')
    tree = ElementTree.fromstring(data)
    services = [xmldict(x) for x in tree.getiterator('service')
                if x.attrib.get('type', None) == '3']
    services = filter(lambda s: s['name'] == service, services)
    if len(services) < 1:
        return ('error', 'service %s not found' % service)
    elif len(services) > 1:
        return ('error', 'multiple services %s found' % service)
    else:
        return parse_status(services[0])

def parse_status(service):
    monitor = service['monitor']
    status = service['status']
    if monitor == 0:
        return ('stopped', 'service %s is stopped or not monitored' % service['name'])
    if status == 0:
        return ('running', 'service %s is running' % service['name'])
    else:
        return ('error', service['status_hint'])

def xmldict(xml):
    result = {}
    for node in xml.getiterator():
        children = list(node)
        if children:
            result[node.tag] = reformat([xmldict(x) for x in children])
        else:
            result[node.tag] = typecast(node.text)
    return result

def reformat(d):
    r = {}
    for item in d:
        if isinstance(item, dict) and len(item) == 1:
            key, value = item.items()[0]
            r[key] = value
        else:
            r.update(dict(item))
    return r

def typecast(x):
    floatpattern = re.compile('^[0-9]+\.[0-9]+$')
    if x is None:
        return x
    if x.isdigit():
        return int(x)
    if floatpattern.match(x):
        return Decimal(x)
    return x
