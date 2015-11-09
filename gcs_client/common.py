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

from functools import wraps
import math
import random
import six
import time

from apiclient import discovery
from apiclient import errors

from gcs_client import errors as gcs_errors


def is_complete(f):
    @wraps(f)
    def wrapped(self, *args, **kwargs):
        attributes = getattr(self, '_required_attributes') or []
        for attribute in attributes:
            if not getattr(self, attribute, None):
                raise Exception('%(func_name)s needs %(attr)s to be set.' %
                                {'func_name': f.__name__, 'attr': attribute})
        return f(self, *args, **kwargs)
    return wrapped


class GCS(object):
    api_version = 'v1'

    _required_attributes = ['credentials']

    def __init__(self, credentials, retry_params=None):
        """Base GCS initialization.

        :param credentials: credentials to use for accessing GCS
        :type credentials: Credentials
        :param retry_params: retry configuration used for communications with
                             GCS.  If not specified RetryParams.getdefault()
                             will be used.
        :type retry_params: RetryParams
        :returns: None
        """
        self.credentials = credentials
        self._retry_params = retry_params or RetryParams.get_default()

    @property
    def retry_params(self):
        """Get retry configuration used by this instance for accessing GCS."""
        return self._retry_params

    @retry_params.setter
    def retry_params(self, retry_params):
        """Set retry configuration used by this instance for accessing GCS.

        :param retry_params: retry configuration used for communications with
                             GCS.  If None is passed retries will be disabled.
        :type retry_params: RetryParams or NoneType
        """
        assert isinstance(retry_params, (type(None), RetryParams))
        self._retry_params = retry_params

    @property
    def credentials(self):
        return self._credentials

    @credentials.setter
    def credentials(self, value):
        if value == getattr(self, '_credentials', not value):
            return

        self._credentials = value
        self._service = discovery.build('storage', self.api_version,
                                        credentials=self._credentials)


class Fillable(GCS):
    def __init__(self, credentials, retry_params=None):
        # We need to set a default value for _credentials, otherwise we would
        # end up calling __get_attr__ on GCS base class
        self._credentials = not credentials
        super(Fillable, self).__init__(credentials, retry_params)
        self._data_retrieved = False
        self._exists = None

    @classmethod
    def obj_from_data(cls, data, credentials=None, retry_params=None):
        obj = cls(credentials=credentials, retry_params=retry_params)
        obj._fill_with_data(data)
        return obj

    def __getattr__(self, name):
        if self._data_retrieved or self._exists is False:
            raise AttributeError

        try:
            data = self._get_data()
            self._exists = True
        except gcs_errors.NotFound:
            self._exists = False
            raise AttributeError

        self._fill_with_data(data)
        return getattr(self, name)

    def _fill_with_data(self, data):
        self._data_retrieved = True
        for k, v in data.items():
            if hasattr(self, k) and getattr(self, k):
                continue
            if isinstance(v, dict) and len(v) == 1:
                if six.PY3:
                    v = tuple(v.values())[0]
                else:
                    v = v.values()[0]
            setattr(self, k, v)

    def _get_data(self):
        raise NotImplementedError


class RetryParams(object):
    """Truncated Exponential Backoff configuration class.

    This configuration is used to provide truncated exponential backoff retries
    for communications.

    The algorithm requires 4 arguments: max retries, initial delay, max backoff
    wait time and backoff factor.

    As long as we have pending retries we will wait
        (backoff_factor ^ n-1) * initial delay
    Where n is the number of retry.
    As long as this wait is not greater than max backoff wait time, if it is
    max backoff time wait will be used.

    We'll add a random wait time to this delay to help avoid cases where many
    clients get synchronized by some situation and all retry at once, sending
    requests in synchronized waves.

    For example with default values of max_retries=5, initial_delay=1,
    max_backoff=32 and backoff_factor=2

    1st failure: 1 second + random delay [ (2^(1-1)) * 1 ]
    2nd failure: 2 seconds + random delay [ (2^(2-1)) * 1 ]
    3rd failure: 4 seconds + random delay [ (2^(3-1)) * 1 ]
    4th failure: 8 seconds + random delay [ (2^(4-1)) * 1 ]
    5th failure: 16 seconds + random delay [ (2^(5-1)) * 1 ]
    6th failure: Fail operation
    """

    def __init__(self, max_retries=5, initial_delay=1, max_backoff=32,
                 backoff_factor=2, randomize=True):
        """Initialize retry configuration.

        :param max_retries: Maximum number of retries before giving up.
        :type max_retries: int
        :param initial_delay: Seconds to wait for the first retry.
        :type initial_delay: int or float
        :param max_backoff: Maximum number of seconds to wait between retries.
        :type max_backoff: int or float
        :param backoff_factor: Base to use for the power used to calculate the
                               delay for the backoff.
        :type backoff_factor: int or float
        :param randomize: Whether to use randomization of the delay time to
                          avoid synchronized waves.
        :type randomize: bool
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_backoff = max_backoff
        self.backoff_factor = backoff_factor
        self.randomize = randomize

    @classmethod
    def get_default(cls):
        """Return default configuration (simpleton patern)."""
        if not hasattr(cls, 'default'):
            cls.default = cls()
        return cls.default

    @classmethod
    def set_default(cls, *args, **kwargs):
        """Set default retry configuration.

        Methods acepts a RetryParams instance or the same arguments as the
        __init__ method.
        """
        default = cls.get_default()
        # For RetryParams argument copy dictionary to default instance so all
        # references to the default configuration will have new values.
        if len(args) == 1 and isinstance(args[0], RetryParams):
            default.__dict__.update(args[0].__dict__)

        # For individual arguments call __init__ method on default instance
        else:
            default.__init__(*args, **kwargs)


# Generate default codes to retry from transient HTTP errors
DEFAULT_RETRY_CODES = tuple(
    code for code, (cls_name, cls) in gcs_errors.http_errors.items()
    if cls is gcs_errors.Transient)


def retry(param='_retry_params', error_codes=DEFAULT_RETRY_CODES):
    """Truncated Exponential Backoff decorator.

    There are multiple ways to use this decorator:

    @retry
    def my_func(self):
        In this case we will try to use `self._retry_params` and if that's not
        available we'll use default retry configuration and retry on
        DEFAULT_RETRY_CODES status codes.

    @retry('_retry_cfg')
    def my_func(self):
        In this case we will try to use `self._retry_cfg` and if that's
        not available we'll use default retry configuration and retry on
        DEFAULT_RETRY_CODES status codes.

    @retry(RetryParams(5, 1, 32, 2, False))
    def my_func(self):
        In this case we will use a specific retry configuration and retry on
        DEFAULT_RETRY_CODES status codes.

    @retry('_retry_cfg', [408, 504])
    def my_func(self):
        In this case we will try to use `self._retry_cfg` and if that's
        not available we'll use default retry configuration and retry only on
        timeout status codes.

    @retry(RetryParams(5, 1, 32, 2, False), [408, 504])
    def my_func(self):
        In this case we will use a specific retry configuration and retry only
        on timeout status codes.

    @retry(error_codes=[408, 504])
    def my_func(self):
        In this case we will try to use `self._retry_params` and if that's not
        available we'll use default retry configuration and retry only on
        timeout status codes.

    If we pass None as the retry parameter or the value of the attribute on the
    instance is None we will not do any retries.
    """
    def _retry(f):
        @wraps(f)
        def wrapped(self, *args, **kwargs):
            # If retry configuration is none or a RetryParams instance, use it
            if isinstance(param, (type(None), RetryParams)):
                retry_params = param
            # If it's an attribute name try to retrieve it
            else:
                retry_params = getattr(self, param, RetryParams.get_default())
            delay = 0
            random_delay = 0

            n = 0  # Retry number
            while True:
                try:
                    result = f(self, *args, **kwargs)
                    return result
                except gcs_errors.Http as exc:
                    if (not retry_params or n >= retry_params.max_retries or
                            exc.code not in error_codes):
                        raise exc
                n += 1
                # If we haven't reached maximum backoff yet calculate new delay
                if delay < retry_params.max_backoff:
                    backoff = (math.pow(retry_params.backoff_factor, n-1)
                               * retry_params.initial_delay)
                    delay = min(retry_params.max_backoff, backoff)

                if retry_params.randomize:
                    random_delay = random.random() * retry_params.initial_delay
                time.sleep(delay + random_delay)

        return wrapped

    # If no argument has been used
    if callable(param):
        f, param = param, '_retry_params'
        return _retry(f)

    return _retry


def convert_exception(f):
    """Decorator to convert from Google's apiclient Http exceptions to ours."""
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except errors.HttpError as exc:
            raise gcs_errors.create_http_exception(exc.resp['status'], exc)
    return wrapped
