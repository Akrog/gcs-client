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

Tests Credentials class.
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
                          credentials.Credentials, 'priv.json', scope='fake')

    @mock.patch.object(credentials.oauth2_client.SignedJwtAssertionCredentials,
                       '__init__')
    def test_init_nonexistent_file(self, mock_creds):
        """Test init with non existent file."""
        with mock.patch.object(moves.builtins, 'open', side_effect=IOError()):
            self.assertRaises(errors.Credentials,
                              credentials.Credentials, 'key.json')
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
            credentials.Credentials('key.json')
            mock_creds.assert_called_once_with(email, pk, mock.ANY)

    @mock.patch.object(credentials.oauth2_client.SignedJwtAssertionCredentials,
                       '__init__')
    def test_init_non_json_missing_email(self, mock_creds):
        """Test init with non json key file and missing email."""
        file_data = 'non json file data'
        file_mock = mock.mock_open(read_data=file_data)
        with mock.patch.object(moves.builtins, 'open', file_mock):
            self.assertRaises(errors.Credentials,
                              credentials.Credentials, 'key.json')
            self.assertFalse(mock_creds.called)

    @mock.patch.object(credentials.oauth2_client.SignedJwtAssertionCredentials,
                       '__init__')
    def test_init_non_json(self, mock_creds):
        """Test init with non json key file."""
        file_data = 'non json file data'
        file_mock = mock.mock_open(read_data=file_data)
        with mock.patch.object(moves.builtins, 'open', file_mock):
            credentials.Credentials('key.json', mock.sentinel.email)
            mock_creds.assert_called_once_with(mock.sentinel.email, file_data,
                                               mock.ANY)

    @mock.patch.object(credentials.oauth2_client.SignedJwtAssertionCredentials,
                       '__init__')
    def test_init_error_reading(self, mock_creds):
        """Test init with an error reading the file."""
        file_mock = mock.mock_open()
        file_mock.return_value.read.side_effect = IOError()
        with mock.patch.object(moves.builtins, 'open', file_mock):
            self.assertRaises(errors.Credentials, credentials.Credentials,
                              'filename')

    def _get_access_token(self, http=None):
        # Original get_access_token would set access_token attribute
        call_num = getattr(self, 'call_num', 0) + 1
        self.access_token = 'access_token' + str(call_num)
        self.call_num = call_num

    @mock.patch.object(credentials.Credentials, 'access_token_expired',
                       mock.PropertyMock(return_value=False))
    @mock.patch.object(credentials.Credentials, 'get_access_token',
                       side_effect=_get_access_token, autospec=True)
    @mock.patch.object(credentials.Credentials, '__init__',
                       return_value=None)
    def test_authorization_one_call(self, mock_init, mock_get_token):
        """Test authorization property calls get_access_token."""
        creds = credentials.Credentials('file')
        # On real init we would have had access_token set to None
        creds.access_token = None

        auth = creds.authorization
        self.assertEqual('Bearer access_token1', auth)
        mock_get_token.assert_called_once_with(creds)

    @mock.patch.object(credentials.Credentials, 'access_token_expired',
                       mock.PropertyMock(return_value=False))
    @mock.patch.object(credentials.Credentials, 'get_access_token',
                       side_effect=_get_access_token, autospec=True)
    @mock.patch.object(credentials.Credentials, '__init__',
                       return_value=None)
    def test_authorization_multiple_accesses(self, mock_init, mock_get_token):
        """Test authorization property calls get_access_token only once."""
        creds = credentials.Credentials('file')
        # On real init we would have had access_token set to None
        creds.access_token = None

        auth = creds.authorization
        mock_get_token.reset_mock()
        # Second access to authorization property shouldn't call
        # get_access_token
        auth2 = creds.authorization
        self.assertEqual('Bearer access_token1', auth2)
        self.assertEqual(auth, auth2)
        self.assertFalse(mock_get_token.called)

    @mock.patch.object(credentials.Credentials, 'access_token_expired',
                       mock.PropertyMock(side_effect=[True, False]))
    @mock.patch.object(credentials.Credentials, 'get_access_token',
                       side_effect=_get_access_token, autospec=True)
    @mock.patch.object(credentials.Credentials, '__init__',
                       return_value=None)
    def test_authorization_multiple_calls(self, mock_init, mock_get_token):
        """Test authorization calls get_access_token on expiration."""
        creds = credentials.Credentials('file')
        # On real init we would have had access_token set to None
        creds.access_token = None

        auth = creds.authorization
        self.assertEqual('Bearer access_token1', auth)
        # Second access to authorization property should call get_access_token
        auth = creds.authorization
        self.assertEqual('Bearer access_token2', auth)
        self.assertEqual(2, mock_get_token.call_count)
        # Third access will not trigger another call to get_access_token
        auth2 = creds.authorization
        self.assertEqual(auth, auth2)
        self.assertEqual(2, mock_get_token.call_count)
