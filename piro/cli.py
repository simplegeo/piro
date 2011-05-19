from argparse import ArgumentParser
import sys

from piro.services import Service

ACTIONS=['start', 'stop', 'restart', 'status']
SERVICES=['cassandra']

def main():
    parser = ArgumentParser(description='Intelligently control services.')
    parser.add_argument('action', choices=ACTIONS,
                        help='The action you wish to apply to the service.')
    parser.add_argument('service', choices=SERVICES,
                        help='Name of the service you wish to control.')
    parser.add_argument('host', nargs='+',
                        help='The host(s) which run the service you wish to control.')
    args = parser.parse_args()
    service = Service.get_service(args.service, args)
    return getattr(service, args.action)()
