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

from gcs_client import bucket
from gcs_client import common
from gcs_client.constants import projection as gcs_projection
from gcs_client.constants import storage_class


class Project(common.Listable):
    """GCS Project Object representation."""

    _required_attributes = common.GCS._required_attributes + ['project_id']
    URL = common.Fillable.URL + '?project=%s'

    def __init__(self, project_id, credentials=None, retry_params=None):
        """Initialize a Project object.

        :param project_id: Project id as listed in Google's project management
                           https://console.developers.google.com/project.
        :param credentials: A credentials object to authorize the connection.
        """
        super(Project, self).__init__(credentials, retry_params)
        self.project_id = project_id

    @property
    def default_bucket_name(self):
        if not self.project_id:
            return None
        return self.project_id + '.appspot.com'

    @property
    def _child_info(self):
        return (self._service.buckets, bucket.Bucket)

    def list(self, fields=None, maxResults=None, projection=None, prefix=None,
             pageToken=None):
        return self._list(project=self.project_id, fields=fields,
                          maxResults=maxResults, projection=projection,
                          prefix=prefix, pageToken=pageToken)

    @common.is_complete
    @common.retry
    @common.convert_exception
    def create_bucket(self, name, location='US',
                      storage_class=storage_class.NEARLINE,
                      predefined_acl=None,
                      predefined_default_obj_acl=None,
                      projection=gcs_projection.SIMPLE, **kwargs):
        kwargs['name'] = name
        kwargs['location'] = location
        kwargs['storageClass'] = storage_class

        req = self._service.buckets().insert(
            project=self.project_id,
            predefinedAcl=predefined_acl,
            predefinedDefaultObjectAcl=predefined_default_obj_acl,
            projection=projection,
            body=kwargs)

        resp = req.execute()
        return bucket.Bucket.obj_from_data(resp, self.credentials)

    def __str__(self):
        return self.project_id

    __repr__ = __str__
