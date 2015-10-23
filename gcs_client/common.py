# -*- coding: utf-8 -*-
from __future__ import absolute_import

from functools import wraps

from apiclient import discovery
from apiclient import errors


def is_complete(f):
    @wraps(f)
    def wrapped(self, *args, **kwargs):
        attributes = getattr(self, '_required_attributes', [])
        for attribute in attributes:
            if not getattr(self, attribute, None):
                raise Exception('%(func_name)s needs %(attr)s to be set.' %
                                {'func_name': f.__name__, 'attr': attribute})
        return f(self, *args, **kwargs)
    return wrapped


class GCS(object):
    api_version = 'v1'

    _required_attributes = ['credentials']

    def __init__(self, credentials):
        self.credentials = credentials

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
    def __init__(self, credentials):
        # We need to set a default value for _credentials, otherwise we would
        # end up calling __get_attr__ on GCS base class
        self._credentials = not credentials
        super(Fillable, self).__init__(credentials)
        self._data_retrieved = False
        self._exists = None

    @classmethod
    def obj_from_data(cls, data, credentials=None):
        obj = cls(credentials=credentials)
        obj._fill_with_data(data)
        return obj

    def __getattr__(self, name):
        if self._data_retrieved or self._exists is False:
            raise AttributeError

        try:
            data = self._get_data()
            self._exists = True
        except errors.HttpError as exc:
            if exc.resp.status == 404:
                self._exists = False
                raise AttributeError
            else:
                raise

        self._fill_with_data(data)
        return getattr(self, name)

    def _fill_with_data(self, data):
        self._data_retrieved = True
        for k, v in data.items():
            if hasattr(self, k) and getattr(self, k):
                continue
            if isinstance(v, dict) and len(v) == 1:
                v = v.values()[0]
            setattr(self, k, v)

    def _get_data(self):
        raise NotImplementedError
