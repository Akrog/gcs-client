# -*- coding: utf-8 -*-
# Copyright 2015 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from __future__ import absolute_import

from six.moves import http_client as httplib
import sys


class Error(Exception):
    """Base error for all gcs_client operations."""
    pass


class Credentials(Error):
    """Credentials errors."""
    pass


class Http(Error):
    """HTTP specific errors."""
    code = None

    def __init__(self, message=None, code=None):
        if code:
            self.code = code
        self.message = message

    def __str__(self):
        msg = 'HTTP Error %s' % self.code
        if self.message:
            msg += ': %s' % self.message
        return msg


class Fatal(Http):
    """Fatal HTTP exceptions."""
    pass


class Transient(Http):
    """Transient  HTTP exceptions."""
    pass


http_errors = {
    httplib.REQUEST_TIMEOUT: ('RequestTimeout', Transient),
    httplib.INTERNAL_SERVER_ERROR: ('InternalServer', Transient),
    httplib.BAD_GATEWAY: ('BadGateway', Transient),
    httplib.SERVICE_UNAVAILABLE: ('ServiceUnavailable', Transient),
    httplib.GATEWAY_TIMEOUT: ('GatewayTimeout', Transient),
    httplib.NOT_FOUND: ('NotFound', Fatal),
    httplib.BAD_REQUEST: ('BadRequest', Fatal),
    httplib.FORBIDDEN: ('Forbidden', Fatal),
    httplib.UNAUTHORIZED: ('Unauthorized', Fatal),
    httplib.REQUESTED_RANGE_NOT_SATISFIABLE: ('InvalidRange', Fatal),
    429: ('TooManyRequests', Transient),
}


def create_http_exception(status_code, message=None):
    """Create an http exception.

    Create an Http exception instance as specific as possible.

    For status codes that have specific exceptions, like with 408
    (RequestTimeout class), those will be returned, but for those that we don't
    have one we will return a generic Http error with the right status code.

    :param status_code: Status code of the http error
    :type status_code: int or string
    :param message: Detailed message for the error
    :type message: str
    :returns: Http exception instance as specific as possible
    :rtype: Http or subclass
    """
    # Try to convert status_code to an integer if it's not one already
    if not isinstance(status_code, int):
        try:
            status_code = int(status_code)
        except ValueError:
            pass

    # Get specific exception if possible
    cls_name, __ = http_errors.get(status_code, (None, None))
    if cls_name:
        return globals()[cls_name](message)

    # Return generic exception
    return Http(message, status_code)


# Dynamically create all HTTP error classes from http_errors dictionary
for status_code, (name, error_class) in http_errors.items():
    new_class = type(name, (error_class,),
                     {'__module__': __name__, 'code': status_code})
    sys.modules[__name__ + '.' + name] = new_class
    globals()[new_class.__name__] = new_class
