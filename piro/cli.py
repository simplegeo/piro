from argparse import ArgumentParser
import sys

import piro.clustohttp as clusto
import piro.service as service

CLUSTO = clusto.ClustoProxy('http://clusto.simplegeo.com/api')

def get_contents(pool):
    return set(CLUSTO.get_by_name(pool).contents())

def get_hosts(pools):
    return reduce(lambda p, i: p.intersection(i), map(get_contents, pools))

def main():
    parser = ArgumentParser(description='Intelligently control services.')
    parser.add_argument('action', choices=service.ACTIONS,
                        help='The action you wish to apply to the service.')
    parser.add_argument('service', choices=service.SERVICES,
                        help='Name of the service you wish to control.')
    parser.add_argument('pool', nargs='+', help='Set of clusto pools '
                        'describing the hosts on which you wish to '
                        'perform the service control action.')
    args = parser.parse_args()
    args.prod = 'production' in args.pool
    args.service = args.service.lower().replace('-', '_')
    hosts = get_hosts(args.pool)
    return getattr(sys.modules['piro.service'],
                   "%s_%s" % (args.action, args.service))(hosts, prod=args.prod)
