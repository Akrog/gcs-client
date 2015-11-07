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
from gcs_client.constants import projection as gcs_projection
from gcs_client import gcs_object


class Bucket(common.Fillable):
    """GCS Bucket Object representation."""

    _required_attributes = common.GCS._required_attributes + ['name']

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

    @common.is_complete
    @common.retry
    @common.convert_exception
    def list(self, prefix=None, maxResults=None, versions=False,
             delimiter=None, projection=gcs_projection.SIMPLE):
        objs = self._service.objects()
        req = objs.list(bucket=self.name, delimiter=delimiter, prefix=prefix,
                        maxResults=maxResults, projection=projection,
                        versions=versions)

        objects_list = []
        while req:
            resp = req.execute()
            items = map(
                lambda b: gcs_object.Object.obj_from_data(b, self.credentials,
                                                          self.retry_params),
                resp.get('items', []))
            objects_list.extend(items)
            req = objs.list_next(req, resp)

        return objects_list

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
