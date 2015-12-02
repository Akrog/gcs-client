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
from gcs_client import constants


class Project(base.Listable):
    """GCS Project Object representation."""

    _required_attributes = base.GCS._required_attributes + ['project_id']
    _URL = base.Fillable._URL + '?project={project_id}'
    _list_url = base.Fillable._URL

    def __init__(self, project_id, credentials=None, retry_params=None):
        """Initialize a Project object.

        :param project_id: Project id as listed in Google's project management
                           https://console.developers.google.com/project.
        :type project_id: String
        :param credentials: A credentials object to authorize the connection.
        :type credentials: gcs_client.Credentials
        :param retry_params: Retry configuration used for communications with
                             GCS.  If None is passed default retries will be
                             used.
        :type retry_params: RetryParams or NoneType
        """
        super(Project, self).__init__(credentials, retry_params)
        self.project_id = project_id

    @property
    def default_bucket_name(self):
        """Bucket name of the default bucket for the project."""
        if not self.project_id:
            return None
        return self.project_id + '.appspot.com'

    def list(self, fields=None, maxResults=None, projection=None, prefix=None,
             pageToken=None):
        """Retrieves a list of buckets for a given project.

        The authenticated user in credentials must be a member of the
        project's team.

        Bucket list operations are eventually consistent.  This means that if
        you create a bucket and then immediately perform a list operation,
        the newly-created bucket will be immediately available for use, but
        the bucket might not immediately appear in the returned list of
        buckets.

        :param fields: Limit retrieved data for each bucket to these fields.
        :type fields: list of strings
        :param maxResults: Maximum number of buckets to return.
        :type maxResults: Unsigned integer
        :param projection: Set of properties to return. Defaults to noAcl.
                           Acceptable values are:
                               "full": Include all properties.
                               "noAcl": Omit the acl property.
        :type projection: String
        :param prefix: Filter results to buckets whose names begin with this
                       prefix.
        :type prefix: String
        :param pageToken:  A previously-returned page token representing part
                           of the larger set of results to view.  The pageToken
                           is an encoded field representing the name and
                           generation of the last bucket in the returned list.
                           In a subsequent request using the pageToken, items
                           that come after the pageToken are shown (up to
                           maxResults).  Bucket list operations are eventually
                           consistent. In addition, if you start a listing and
                           then create a new bucket before using a pageToken to
                           continue listing, you will not see the new bucket in
                           subsequent listing results if it is in part of the
                           bucket namespace already listed.
        :type pageToken: String
        :returns: List of buckets that match the criteria.
        :rtype: List of gcs_client.Bucket.
        """
        return self._list(project=self.project_id, fields=fields,
                          maxResults=maxResults, projection=projection,
                          prefix=prefix, pageToken=pageToken)

    @common.is_complete
    @common.retry
    def create_bucket(self, name, location='US',
                      storage_class=constants.STORAGE_NEARLINE,
                      predefined_acl=None,
                      predefined_default_obj_acl=None,
                      projection=constants.PROJECTION_SIMPLE, **kwargs):
        """Create a new bucket in the project.

        Google Cloud Storage uses a flat namespace, so you can't create a
        bucket with a name that is already in use.

        For more information, see Bucket Naming Guidelines:
        https://cloud.google.com/storage/docs/bucket-naming#requirements

        The authenticated user in credentials must be a member of the project's
        team as an editor or owner.

        :param name: The name of the bucket.
        :type name: String
        :param location: The location of the bucket. Object data for objects in
                         the bucket resides in physical storage within this
                         region.  Defaults to US. See the developer's guide for
                         the authoritative list:
                         https://cloud.google.com/storage/docs/bucket-locations
        :type location: String
        :param storage_class: The bucket's storage class. This defines how
                              objects in the bucket are stored and determines
                              the SLA and the cost of storage.  Value must be
                              one of gcs_client.constants.STORAGE_*, and
                              they include STANDARD, NEARLINE and
                              DURABLE_REDUCED_AVAILABILITY.  Defaults to
                              gcs_client.constants.STORAGE_NEARLINE.
        :type storage_class: String
        :param predefined_acl: Apply a predefined set of access controls to
                               this bucket.  Acceptable values from
                               gcs_client.ACL_* are ACL_AUTH_READ, ACL_PRIVATE,
                               ACL_PROJECT_PRIVATE, ACL_PUBLIC_R, and
                               ACL_PUBLIC_RW
        :type predefined_acl: String
        :param predefined_default_obj_acl: Apply a predefined set of default
                                           object access controls to this
                                           bucket.  Acceptable values from
                                           gcs_client.constants.ACL_* are
                                           ACL_AUTH_READ, ACL_OWNER_FULL,
                                           ACL_OWNER_READ, ACL_PRIVATE,
                                           ACL_PROJECT_PRIVATE, and
                                           ACL_PUBLIC_R
        :type predefined_default_obj_acl: String
        :param projection: Set of properties to return. Defaults to
                           PROJECTION_SIMPLE.
                           Acceptable values from gcs_client.constants are:
                               - PROJECTION_FULL ('full'): Include all
                                 properties.
                               - PROJECTION_SIMPLE ('noAcl'): Omit the acl
                                 property.
        :type projection: String
        :returns: A new Bucket instance
        :rtype: gcs_client.Bucket
        """
        r = self._request(
            parse=True,
            op='POST',
            predefinedAcl=predefined_acl,
            predefinedDefaultObjectAcl=predefined_default_obj_acl,
            projection=projection,
            body={'name': name, 'location': location,
                  'storageClass': storage_class})
        return bucket.Bucket._obj_from_data(r.json(), self.credentials)

    def __str__(self):
        return self.project_id

    __repr__ = __str__
