"""piro aims to be a flexible, extensible command-line tool for
   intelligently controlling services."""

class NoContentException(Exception):
    """
    Exception class for when a call to an HTTP endpoint returns an
    empty response.
    """
    pass

