# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from oauth2client import client as oauth2_client

from gcs_client.constants import scope as gcs_scope


class GCSCredential(oauth2_client.SignedJwtAssertionCredentials):
    common_url = 'https://www.googleapis.com/auth/'
    scope_urls = {
        gcs_scope.READER: 'devstorage.read_only',
        gcs_scope.WRITER: 'devstorage.read_write',
        gcs_scope.OWNER: 'devstorage.full_control',
        gcs_scope.CLOUD: 'cloud-platform',
    }

    def __init__(self, key_file_name, email=None, scope=gcs_scope.OWNER):
        if scope not in self.scope_urls:
            raise Exception('scope must be one of %s' % self.scope_urls.keys())
        self.scope = scope

        try:
            with open(key_file_name, 'r') as f:
                key_data = f.read()
        except IOError:
            raise Exception('Could not read data from private key file %s.',
                            key_file_name)

        try:
            json_data = json.loads(key_data)
            key_data = json_data['private_key']
            email = json_data['client_email']
        except Exception:
            if not email:
                raise Exception('Non JSON private key needs email, but it was '
                                'not provided')

        url = self.common_url + self.scope_urls[scope]
        super(GCSCredential, self).__init__(email, key_data, url)
