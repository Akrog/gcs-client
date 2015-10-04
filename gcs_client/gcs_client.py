# -*- coding: utf-8 -*-
from __future__ import absolute_import

from apiclient import discovery

from gcs_client import utils


__all__ = ('Bucket',)


class Bucket(object):
    """GCS Bucket Object representation."""

    api_version = 'v1'

    def __init__(self, bucket_name=None, project_id=None, credentials=None):
        """Initialize a Bucket object.

        :param bucket_name: Name of the bucket to use, if it is provided but a
                            project_id is, then default bucket will be used.
        :param project_id: Project id as listed in Google's project management
                           https://console.developers.google.com/project. Not
                           necessary if bucket_name is provided.
        :param credentials: A credentials object to authorize the connection.
        """
        self.project_id = project_id
        self.credentials = credentials
        self._bucket_name = bucket_name
        self._service = None

    @property
    def credentials(self):
        return getattr(self, '_credentials', None)

    @credentials.setter
    def credentials(self, value):
        old_credentials = self.credentials
        self._credentials = value
        if not value or value == old_credentials:
            return

        self._service = discovery.build('storage', self.api_version,
                                        credentials=self._credentials)

    @property
    def default_bucket_name(self):
        if not self.project_id:
            return None
        return self.project_id + '.appspot.com'

    @property
    def bucket_name(self):
        return self._bucket_name or self.default_bucket_name

    @bucket_name.setter
    def bucket_name(self, value):
        self._bucket_name = value

    def _is_complete(f):
        return utils.has_attributes(f, ('bucket_name', 'credentials')

    @_is_complete
    def list(self):
        return [1, 2, 3]
