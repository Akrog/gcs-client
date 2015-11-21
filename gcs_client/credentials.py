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

import json

from oauth2client import client as oauth2_client

from gcs_client.constants import scope as gcs_scope
from gcs_client import errors


class Credentials(oauth2_client.SignedJwtAssertionCredentials):
    common_url = 'https://www.googleapis.com/auth/'
    scope_urls = {
        gcs_scope.READER: 'devstorage.read_only',
        gcs_scope.WRITER: 'devstorage.read_write',
        gcs_scope.OWNER: 'devstorage.full_control',
        gcs_scope.CLOUD: 'cloud-platform',
    }

    def __init__(self, key_file_name, email=None, scope=gcs_scope.OWNER):
        if scope not in self.scope_urls:
            raise errors.Credentials('scope must be one of %s' %
                                     self.scope_urls.keys())
        self.scope = scope

        try:
            with open(key_file_name, 'r') as f:
                key_data = f.read()
        except IOError:
            raise errors.Credentials(
                'Could not read data from private key file %s.', key_file_name)

        try:
            json_data = json.loads(key_data)
            key_data = json_data['private_key']
            email = json_data['client_email']
        except Exception:
            if not email:
                raise errors.Credentials(
                    'Non JSON private key needs email, but it was missing')

        url = self.common_url + self.scope_urls[scope]
        super(Credentials, self).__init__(email, key_data, url)

    @property
    def authorization(self):
        """Authorization header value for GCS requests."""
        if not self.access_token or self.access_token_expired:
            self.get_access_token()
        return 'Bearer ' + self.access_token
