import sys

class Service(object):
    @classmethod
    def get_service(klass, service, args):
        service_class = "%s%s" % (service.capitalize(), 'Service')
        try:
            module = __import__('piro.services.%s' % service_class, fromlist=[None])
            instance = getattr(module, 'get_instance')(args)
            print "Got an instance of %s" % service_class
        except AttributeError:
            instance = Service()
            print "Could not find %s, using Service" % service_class
        return instance

    name = None
    controller_name = None

    def __init__(self, args):
        self.name = args.service

    def start(self):
        print "Starting service %s" % self.name
        print "noop: not implemented"

    def stop(self):
        print "Stopping service %s" % self.name
        print "noop: not implemented"

    def restart(self):
        self.stop()
        self.start()
        return self.status()

    def status(self):
        print "Getting status for service %s" % self.name
        print "noop: not implemented"
