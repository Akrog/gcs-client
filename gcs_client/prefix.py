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

from gcs_client import base


class Prefix(base.Listable):

    kind = 'storage#prefix'
    _required_attributes = base.GCS._required_attributes + ['name',
                                                            'prefix']

    def __init__(self, name, prefix, delimiter=None, credentials=None,
                 retry_params=None):
        super(Prefix, self).__init__(credentials, retry_params)
        self.URL = '%s/%s/o' % (base.GCS.URL, name)
        self.name = name
        self.prefix = prefix
        self.delimiter = delimiter

    def list(self, prefix=None, maxResults=None, versions=None, delimiter=None,
             projection=None, pageToken=None):
        if delimiter is None:
            delimiter = self.delimiter
        return self._list(_list_url=self.URL, prefix=prefix or self.prefix,
                          maxResults=maxResults, versions=versions,
                          delimiter=delimiter,
                          projection=projection,
                          pageToken=pageToken)

    def __str__(self):
        return self.prefix

    def __repr__(self):
        return ("%s.%s('%s', '%s')" % (self.__module__,
                self.__class__.__name__, self.name, self.prefix))
