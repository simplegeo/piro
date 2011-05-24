"""Functions which use the monit web interface to control services."""

from decimal import Decimal
import re
from time import sleep
from urllib import urlencode
import urllib2 as url
from xml.etree import ElementTree

class NoContentException(Exception):
    """
    Exception class for when a call to the monit HTTP status enpoint
    returns an empty response.
    """
    pass

def stop(service, host, wait=False):
    """
    Send a stop command for the given service to the monit web
    interface on the given host.

    If wait=True, block until monit shows that the service has
    sucessfully been stopped.
    """
    data = urlencode({'action': 'stop'})
    req = url.Request('http://%s:2812/%s' % (host, service), data=data,
                          headers={'Authorization': 'Basic YWRtaW46bW9uaXQ='})
    url.urlopen(req, timeout=1)
    if not wait:
        return ('stop', 'stop signal sent to service %s' % service)
    else:
        svc_status = None
        while svc_status != 'stopped':
            sleep(.1)
            try:
                svc_status = status(service, host)[0]
            except (url.URLError, NoContentException):
                pass
        return ('stop', 'service %s is stopped' % service)

def start(service, host, wait=False):
    """
    Send a start command for the given service to the monit web
    interface on the given host.

    If wait=True, block until monit shows that the service has
    sucessfully been started.
    """
    data = urlencode({'action': 'start'})
    req = url.Request('http://%s:2812/%s' % (host, service), data=data,
                          headers={'Authorization': 'Basic YWRtaW46bW9uaXQ='})
    url.urlopen(req, timeout=1)
    if not wait:
        return ('start', 'start signal sent to service %s' % service)
    else:
        svc_status = None
        while svc_status != 'running':
            sleep(.1)
            try:
                svc_status = status(service, host)[0]
            except (url.URLError, NoContentException):
                pass
        return ('start', 'service %s is running' % service)

def restart(service, host, wait=False):
    """
    Send a restart command for the given service to the monit web
    interface on the given host.

    If wait=True, block until monit shows that the service has
    sucessfully been restarted.
    """
    data = urlencode({'action': 'restart'})
    req = url.Request('http://%s:2812/%s' % (host, service), data=data,
                          headers={'Authorization': 'Basic YWRtaW46bW9uaXQ='})
    url.urlopen(req, timeout=1)
    if not wait:
        return ('restart', 'restart signal sent to service %s' % service)
    else:
        svc_status = None
        while svc_status != 'running':
            sleep(.1)
            try:
                svc_status = status(service, host)[0]
            except (url.URLError, NoContentException):
                pass
        return ('restart', 'service %s is running' % service)

def status(service, host):
    """
    Fetch the monit status of the given service using the monit web
    interface on the given host.
    """
    req = url.Request('http://%s:2812/_status?format=xml' % host,
                          headers={'Authorization': 'Basic YWRtaW46bW9uaXQ='})
    res = url.urlopen(req, timeout=1)
    data = res.read()
    if not data:
        raise NoContentException('No content from server')
    tree = ElementTree.fromstring(data)
    services = [xmldict(x) for x in tree.getiterator('service')
                if x.attrib.get('type', None) == '3']
    services = [s for s in services if s['name'] == service]
    if len(services) < 1:
        return ('error', 'service %s not found' % service)
    elif len(services) > 1:
        return ('error', 'multiple services %s found' % service)
    else:
        return parse_status(services[0])

def parse_status(service):
    """
    Given a status dictionary fetched from monit, determine the state
    of the service and return a tuple describing that state.
    """
    monitor = service['monitor']
    svc_status = service['status']
    if monitor == 0:
        return ('stopped', 'service %s is stopped or not monitored' %
                service['name'])
    if svc_status == 0:
        return ('running', 'service %s is running' % service['name'])
    else:
        return ('error', service['status_hint'])

def xmldict(xml):
    """
    Given an XML document fetched from monit, convert the XML into a
    dict.
    """
    result = {}
    for node in xml.getiterator():
        children = list(node)
        if children:
            result[node.tag] = reformat([xmldict(x) for x in children])
        else:
            result[node.tag] = typecast(node.text)
    return result

def reformat(xmld):
    """
    Format XML elements from a monit XML document. Helper function for
    xmldict.
    """
    res = {}
    for item in xmld:
        if isinstance(item, dict) and len(item) == 1:
            key, value = item.items()[0]
            res[key] = value
        else:
            res.update(dict(item))
    return res

def typecast(string):
    """
    Convert XML strings of numbers into python-native numeric
    types. Helper function for xmldict.
    """
    floatpattern = re.compile('^[0-9]+\.[0-9]+$')
    if string is None:
        return string
    if string.isdigit():
        return int(string)
    if floatpattern.match(string):
        return Decimal(string)
    return string
