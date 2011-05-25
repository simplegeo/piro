"""amazinghorse API client"""

try:
    import simplejson as json
except ImportError:
    import json
from urllib import urlencode
import urllib2 as url
from piro.util import NoContentException

class AmazingHorse(object):
    """
    Client object for interacting with the amazinghorse API.
    """
    def __init__(self, url='https://amazinghorse.simplegeo.com:4430/aws',
                 username=None, password=None):
        """
        Initialize the amazinghorse object.
        """
        self.url = url
        auth = username + ':' + password
        self.auth = auth.encode('base64')

    def get_elbs(self):
        req = url.Request(self.url + '/elb/')
        req.add_header('Authorization', 'Basic %s' % self.auth)
        res = url.urlopen(req)
        data = res.read()
        if not data:
            raise NoContentException('No content from server')
        return json.loads(data)

    def get_elb(self, name):
        req = url.Request(self.url + '/elb/' + name)
        req.add_header('Authorization', 'Basic %s' % self.auth)
        res = url.urlopen(req)
        data = res.read()
        if not data:
            raise NoContentException('No content from server')
        return json.loads(data)

    def get_availability_zones(self, elb_name):
        return self.get_elb(elb_name)['availability_zones']

    def enable_availability_zone(self, zone_name, elb_name):
        req = url.Request(self.url + '/elb/%s/%s?state=on' % (elb_name,
                                                              zone_name))
        req.add_header('Authorization', 'Basic %s' % self.auth)
        res = url.urlopen(req)
        data = res.read()
        if not data:
            raise NoContentException('No content from server')
        return json.loads(data)

    def disable_availability_zone(self, zone_name, elb_name):
        req = url.Request(self.url + '/elb/%s/%s?state=off' % (elb_name,
                                                               zone_name))
        req.add_header('Authorization', 'Basic %s' % self.auth)
        res = url.urlopen(req)
        data = res.read()
        if not data:
            raise NoContentException('No content from server')
        return json.loads(data)
