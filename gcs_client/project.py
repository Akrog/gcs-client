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
from gcs_client import bucket
from gcs_client import common
from gcs_client.constants import projection as gcs_projection
from gcs_client.constants import storage_class


class Project(base.Listable):
    """GCS Project Object representation."""

    _required_attributes = base.GCS._required_attributes + ['project_id']
    URL = base.Fillable.URL + '?project=%s'
    _list_url = base.Fillable.URL

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

    def list(self, fields=None, maxResults=None, projection=None, prefix=None,
             pageToken=None):
        return self._list(project=self.project_id, fields=fields,
                          maxResults=maxResults, projection=projection,
                          prefix=prefix, pageToken=pageToken)

    @common.is_complete
    @common.retry
    def create_bucket(self, name, location='US',
                      storage_class=storage_class.NEARLINE,
                      predefined_acl=None,
                      predefined_default_obj_acl=None,
                      projection=gcs_projection.SIMPLE, **kwargs):

        r = self._request(
            parse=True,
            op='POST',
            predefinedAcl=predefined_acl,
            predefinedDefaultObjectAcl=predefined_default_obj_acl,
            projection=projection,
            body={'name': name, 'location': location,
                  'storageClass': storage_class})
        return bucket.Bucket.obj_from_data(r.json(), self.credentials)

    def __str__(self):
        return self.project_id

    __repr__ = __str__
