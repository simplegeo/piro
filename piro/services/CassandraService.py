from piro.services import Service

def get_instance(args):
    return CassandraService(args)

class CassandraService(Service):
    pass
