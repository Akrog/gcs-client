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

from gcs_client import constants
from gcs_client import errors


class Credentials(oauth2_client.SignedJwtAssertionCredentials):
    """GCS Credentials used to access servers."""

    common_url = 'https://www.googleapis.com/auth/'
    scope_urls = {
        constants.SCOPE_READER: 'devstorage.read_only',
        constants.SCOPE_WRITER: 'devstorage.read_write',
        constants.SCOPE_OWNER: 'devstorage.full_control',
        constants.SCOPE_CLOUD: 'cloud-platform',
    }

    def __init__(self, key_file_name, email=None, scope=constants.SCOPE_OWNER):
        """Initialize credentials used for all GCS operations.

        Create OAuth 2.0 credentials to access GCS from a JSON file or a P12
        and email address.

        Since this library is meant to work outside of Google App Engine and
        Google Compute Engine, you must obtain these credential files in the
        Google Developers Console.  To generate service-account credentials,
        or to view the public credentials that you've already generated, do the
        following:

        1. Open the Credentials page.
        2. To set up a new service account, do the following:

           a. Click Add credentials > Service account.
           b. Choose whether to download the service account's public/private
              key as a JSON file (preferred) or standard P12 file.

           Your new public/private key pair is generated and downloaded to your
           machine; it serves as the only copy of this key. You are responsible
           for storing it securely.

        You can return to the Developers Console at any time to view the client
        ID, email address, and public key fingerprints, or to generate
        additional public/private key pairs. For more details about service
        account credentials in the Developers Console, see Service accounts in
        the Developers Console help file.

        :param key_file_name: Name of the file with the credentials to use.
        :type key_file_name: String
        :param email: Service account's Email address to use with P12 file.
                      When using JSON files this argument will be ignored.
        :type email: String
        :param scope: Scopes that the credentials should be granted access to.
                      Value must be one of Credentials.scope_urls.keys()
        :type scope: String

        """
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
