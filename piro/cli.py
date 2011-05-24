"""CLI interface for piro."""

from argparse import ArgumentParser
import sys

import piro.clustohttp as clusto
import piro.service
import piro.util as util

ACTIONS = ['start', 'stop', 'restart', 'status']
CLUSTO = clusto.ClustoProxy('http://clusto.simplegeo.com/api')


def main():
    """Main entry point for the 'piro' command."""
    parser = ArgumentParser(description='Intelligently control services.')
    parser.add_argument('action', choices=ACTIONS,
                        help='The action you wish to apply to the service.')
    parser.add_argument('service',
                        help='Name of the service you wish to control.')
    parser.add_argument('pool', nargs='+', help='Set of clusto pools '
                        'describing the hosts on which you wish to '
                        'perform the service control action.')
    parser.add_argument('-t', '--timeout', default=120, type=int,
                        help='Timeout (for services/actions that support it.)')
    args = parser.parse_args()
    args.prod = 'production' in args.pool
    hosts = util.get_hosts(args.pool)
    return getattr(sys.modules['piro.service'],
                   args.action)(hosts, service=args.service, args=args)
