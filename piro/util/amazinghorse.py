"""amazinghorse API client"""

try:
    import simplejson as json
except ImportError:
    import json
import urllib2 as url
from piro import NoContentException

class AmazingHorse(object):
    """
    Client object for interacting with the amazinghorse API.
    """
    def __init__(self, base_url='https://amazinghorse.simplegeo.com:4430/aws',
                 username=None, password=None):
        self.url = base_url
        auth = username + ':' + password
        self.auth = auth.encode('base64')

    def get_elbs(self):
        """
        Fetch the list of ELBs from the amazinghorse API
        """
        req = url.Request(self.url + '/elb/')
        req.add_header('Authorization', 'Basic %s' % self.auth)
        res = url.urlopen(req)
        data = res.read()
        if not data:
            raise NoContentException('No content from server')
        return json.loads(data)

    def get_elb(self, name):
        """
        Given an ELB name, fetch the info about that ELB from the
        amazinghorse API.
        """
        req = url.Request(self.url + '/elb/' + name)
        req.add_header('Authorization', 'Basic %s' % self.auth)
        res = url.urlopen(req)
        data = res.read()
        if not data:
            raise NoContentException('No content from server')
        return json.loads(data)

    def get_availability_zones(self, elb_name):
        """
        Given an ELB name, fetch the active availability zones for
        that ELB from the amazinghorse API.
        """
        return self.get_elb(elb_name)['availability_zones']

    def enable_availability_zone(self, zone_name, elb_name):
        """
        Given the name of an availability zone and an ELB name,
        activate that availability zone in that ELB.
        """
        req = url.Request(self.url + '/elb/%s/%s?state=on' % (elb_name,
                                                              zone_name))
        req.add_header('Authorization', 'Basic %s' % self.auth)
        res = url.urlopen(req)
        data = res.read()
        if not data:
            raise NoContentException('No content from server')
        return json.loads(data)

    def disable_availability_zone(self, zone_name, elb_name):
        """
        Given the name of an availability zone and an ELB name,
        deactivate that availability zone in that ELB.
        """
        req = url.Request(self.url + '/elb/%s/%s?state=off' % (elb_name,
                                                               zone_name))
        req.add_header('Authorization', 'Basic %s' % self.auth)
        res = url.urlopen(req)
        data = res.read()
        if not data:
            raise NoContentException('No content from server')
        return json.loads(data)
