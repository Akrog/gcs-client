#!/usr/bin/env python
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

"""
test_credentials
----------------------------------

Tests GCSCredentials class.
"""

import unittest
from six import moves

import mock

from gcs_client import credentials
from gcs_client import errors


class TestErrors(unittest.TestCase):

    def test_init_wrong_scope(self):
        """Test init wrong scope."""
        self.assertRaises(errors.Credentials,
                          credentials.GCSCredential, 'priv.json', scope='fake')

    @mock.patch.object(credentials.oauth2_client.SignedJwtAssertionCredentials,
                       '__init__')
    def test_init_nonexistent_file(self, mock_creds):
        """Test init with non existent file."""
        with mock.patch.object(moves.builtins, 'open', side_effect=IOError()):
            self.assertRaises(errors.Credentials,
                              credentials.GCSCredential, 'key.json')
            self.assertFalse(mock_creds.called)

    @mock.patch.object(credentials.oauth2_client.SignedJwtAssertionCredentials,
                       '__init__')
    def test_init_json(self, mock_creds):
        """Test init with json key info."""
        pk = "pk"
        email = "email"
        file_data = '{"private_key": "%s", "client_email": "%s"}' % (pk, email)

        file_mock = mock.mock_open(read_data=file_data)
        with mock.patch.object(moves.builtins, 'open', file_mock):
            credentials.GCSCredential('key.json')
            mock_creds.assert_called_once_with(email, pk, mock.ANY)

    @mock.patch.object(credentials.oauth2_client.SignedJwtAssertionCredentials,
                       '__init__')
    def test_init_non_json_missing_email(self, mock_creds):
        """Test init with non json key file and missing email."""
        file_data = 'non json file data'
        file_mock = mock.mock_open(read_data=file_data)
        with mock.patch.object(moves.builtins, 'open', file_mock):
            self.assertRaises(errors.Credentials,
                              credentials.GCSCredential, 'key.json')
            self.assertFalse(mock_creds.called)

    @mock.patch.object(credentials.oauth2_client.SignedJwtAssertionCredentials,
                       '__init__')
    def test_init_non_json(self, mock_creds):
        """Test init with non json key file."""
        file_data = 'non json file data'
        file_mock = mock.mock_open(read_data=file_data)
        with mock.patch.object(moves.builtins, 'open', file_mock):
            credentials.GCSCredential('key.json', mock.sentinel.email)
            mock_creds.assert_called_once_with(mock.sentinel.email, file_data,
                                               mock.ANY)

    @mock.patch.object(credentials.oauth2_client.SignedJwtAssertionCredentials,
                       '__init__')
    def test_init_error_reading(self, mock_creds):
        """Test init with an error reading the file."""
        file_mock = mock.mock_open()
        file_mock.return_value.read.side_effect = IOError()
        with mock.patch.object(moves.builtins, 'open', file_mock):
            self.assertRaises(errors.Credentials, credentials.GCSCredential,
                              'gorka')
