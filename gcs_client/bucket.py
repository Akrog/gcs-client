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

from gcs_client import common
from gcs_client import gcs_object


class Bucket(common.Fillable, common.Listable):
    """GCS Bucket Object representation."""

    _required_attributes = common.GCS._required_attributes + ['name']
    URL = common.Fillable.URL + '/%s/o'

    def __init__(self, name=None, credentials=None, retry_params=None):
        """Initialize a Bucket object.

        :param name: Name of the bucket to use.
        :param credentials: A credentials object to authorize the connection.
        """
        super(Bucket, self).__init__(credentials, retry_params)
        self.name = name

    @common.retry
    @common.convert_exception
    def _get_data(self):
        req = self._service.buckets().get(bucket=self.name)
        return req.execute()

    @property
    def _child_info(self):
        return (self._service.objects, gcs_object.Object)

    def list(self, prefix=None, maxResults=None, versions=None, delimiter=None,
             projection=None, pageToken=None):
        return self._list(bucket=self.name, prefix=prefix,
                          maxResults=maxResults, versions=versions,
                          delimiter=delimiter, projection=projection,
                          pageToken=pageToken)

    @common.retry
    @common.convert_exception
    def delete(self, if_metageneration_math=None,
               if_metageneration_not_match=None):
        req = self._service.buckets().delete(bucket=self.name)
        req.execute()

    def open(self, name, mode='r', generation=None):
        obj = gcs_object.Object(self.name, name, generation, self.credentials,
                                self.retry_params)
        return obj.open(mode, generation)

    def __str__(self):
        return self.name

    def __repr__(self):
        return ("%s.%s('%s') #etag: %s" %
                (self.__module__, self.__class__.__name__, self.name,
                 getattr(self, 'etag', '?')))
