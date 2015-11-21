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

import requests

from gcs_client import base
from gcs_client import common
from gcs_client import gcs_object


class Bucket(base.Fillable, base.Listable):
    """GCS Bucket Object representation."""

    kind = 'storage#buckets'
    _required_attributes = base.GCS._required_attributes + ['name']
    URL = base.Fillable.URL + '/%s'

    def __init__(self, name=None, credentials=None, retry_params=None):
        """Initialize a Bucket object.

        :param name: Name of the bucket to use.
        :param credentials: A credentials object to authorize the connection.
        """
        super(Bucket, self).__init__(credentials, retry_params)
        self.name = name

    @common.retry
    def _get_data(self):
        r = self._request(parse=True)
        return r.json()

    def list(self, prefix=None, maxResults=None, versions=None, delimiter=None,
             projection=None, pageToken=None):
        return self._list(_list_url=(self.URL % self.name) + '/o',
                          prefix=prefix,
                          maxResults=maxResults, versions=versions,
                          delimiter=delimiter, projection=projection,
                          pageToken=pageToken)

    @common.retry
    def delete(self, if_metageneration_match=None,
               if_metageneration_not_match=None):
        self._request(op='DELETE', ok=(requests.codes.no_content,),
                      ifMetagenerationMatch=if_metageneration_match,
                      ifMetagenerationNotMatch=if_metageneration_not_match)

    def open(self, name, mode='r', generation=None, chunksize=None):
        obj = gcs_object.Object(self.name, name, generation, self.credentials,
                                self.retry_params, chunksize)
        return obj.open(mode)

    def __str__(self):
        return self.name

    def __repr__(self):
        return ("%s.%s('%s') #etag: %s" %
                (self.__module__, self.__class__.__name__, self.name,
                 getattr(self, 'etag', '?')))
