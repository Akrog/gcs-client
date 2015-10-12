# -*- coding: utf-8 -*-
from __future__ import absolute_import

from apiclient import discovery

from gcs_client import utils


__all__ = ('Project')


class GCS(object):
    api_version = 'v1'

    def __init__(self, credentials):
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


class Project(GCS):
    def __init__(self, project_id, credentials=None):
        super(Project, self).__init__(credentials)
        self.project_id = project_id

    def _has_project_id(f):
        return utils.has_attributes(f, ('project_id', 'credentials'))

    @property
    def default_bucket_name(self):
        if not self.project_id:
            return None
        return self.project_id + '.appspot.com'

    @_has_project_id
    def list_buckets(self, fields_to_return=None, max_results=None):
        items = []
        buckets = self._service.buckets()

        req = buckets.list(project=self.project_id,
                           fields=fields_to_return,
                           maxResults=max_results)

        while req:
            resp = req.execute()
            items.extend(resp.get('items', []))
            req = buckets.list_next(req, resp)

        return items
