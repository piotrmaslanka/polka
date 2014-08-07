class PolkaException(Exception):
    pass

class IOException(PolkaException):
    pass

class SeriesNotFoundException(PolkaException):
    pass

class IllegalArgumentException(PolkaException):
    pass