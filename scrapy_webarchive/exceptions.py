class WaczMiddlewareException(Exception):
    """Indicates a critical issue in the middleware."""

    pass


class UnsupportedURIException(Exception):
    """Raised when the given URI scheme is not supported by the factory."""

    pass
