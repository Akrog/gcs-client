# -*- coding: utf-8 -*-
from __future__ import absolute_import

from functools import wraps

from apiclient import discovery


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
        self._service = None
        self._credentials = None
        self.credentials = credentials

    @property
    def credentials(self):
        return self._credentials

    @credentials.setter
    def credentials(self, value):
        old_credentials = self.credentials
        self._credentials = value
        if not value or value == old_credentials:
            return

        self._service = discovery.build('storage', self.api_version,
                                        credentials=self._credentials)


class Fillable(GCS):
    def __init__(self, credentials):
        super(Fillable, self).__init__(credentials)
        self._data_was_retrieved = False

    @classmethod
    def obj_from_data(cls, data, credentials=None):
        obj = cls(credentials=credentials)
        obj._fill_with_data(data)
        return obj

    def __getattr__(self, name):
        if self._data_was_retrieved:
            raise AttributeError

        data = self._get_data()
        self._fill_with_data(data)
        return getattr(self, name)

    def _fill_with_data(self, data):
        for k, v in data.items():
            if hasattr(self, k) and getattr(self, k):
                continue
            if isinstance(v, dict) and len(v) == 1:
                v = v.values()[0]
            setattr(self, k, v)
        self._data_was_retrieved = True

    def _get_data(self):
        raise NotImplementedError
