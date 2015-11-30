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
test_prefix
----------------------------------

Tests prefix class
"""

import unittest

import mock

from gcs_client import prefix


class TestPrefix(unittest.TestCase):
    """Test Prefix class."""

    @mock.patch('gcs_client.base.GCS.__init__')
    def test_init(self, mock_init):
        """Test init method."""

        name = 'bucket_name'
        prefx = prefix.Prefix(name, mock.sentinel.prefix,
                              mock.sentinel.delimiter,
                              mock.sentinel.credentials,
                              mock.sentinel.retry_params)

        mock_init.assert_called_once_with(mock.sentinel.credentials,
                                          mock.sentinel.retry_params)

        self.assertEqual(
            'https://www.googleapis.com/storage/v1/b/bucket_name/o',
            prefx._URL)
        self.assertEqual(name, prefx.name)
        self.assertEqual(mock.sentinel.prefix, prefx.prefix)
        self.assertEqual(mock.sentinel.delimiter, prefx.delimiter)

    @mock.patch('gcs_client.base.GCS.__init__')
    def test_repr(self, mock_init):
        """Test repr method for Prefix class."""
        prefx = prefix.Prefix('bucket_name', 'prefix', mock.sentinel.delimiter,
                              mock.sentinel.credentials,
                              mock.sentinel.retry_params)

        self.assertEqual("gcs_client.prefix.Prefix('bucket_name', 'prefix')",
                         repr(prefx))

    @mock.patch('gcs_client.base.GCS.__init__')
    def test_str(self, mock_init):
        """Test str method for Prefix class."""
        pref = 'prefix'
        prefx = prefix.Prefix(mock.sentinel.name, pref,
                              mock.sentinel.delimiter,
                              mock.sentinel.credentials,
                              mock.sentinel.retry_params)

        self.assertEqual(pref, str(prefx))

    @mock.patch('gcs_client.base.Listable._list',
                return_value=mock.sentinel.list_result)
    @mock.patch('gcs_client.base.GCS.__init__')
    def test_list_defaults(self, mock_init, mock_list):
        """Test list method with default values."""
        name = 'bucket_name'
        prefx = prefix.Prefix(name, 'var/',
                              mock.sentinel.delimiter,
                              mock.sentinel.credentials,
                              mock.sentinel.retry_params)

        self.assertEqual(mock.sentinel.list_result, prefx.list())
        mock_list.assert_called_once_with(
            _list_url='https://www.googleapis.com/storage/v1/b/bucket_name/o',
            prefix='var/', maxResults=None, versions=None,
            delimiter=mock.sentinel.delimiter, projection=None, pageToken=None)

    @mock.patch('gcs_client.base.Listable._list',
                return_value=mock.sentinel.list_result)
    @mock.patch('gcs_client.base.GCS.__init__')
    def test_list(self, mock_init, mock_list):
        """Test list method with default values."""
        name = 'bucket_name'
        prefx = prefix.Prefix(name, 'var/',
                              mock.sentinel.delimiter,
                              mock.sentinel.credentials,
                              mock.sentinel.retry_params)

        self.assertEqual(mock.sentinel.list_result,
                         prefx.list('log/',
                                    mock.sentinel.max, mock.sentinel.version,
                                    mock.sentinel.new_delimiter,
                                    mock.sentinel.projection,
                                    mock.sentinel.page_token))
        mock_list.assert_called_once_with(
            _list_url='https://www.googleapis.com/storage/v1/b/bucket_name/o',
            prefix='var/log/', maxResults=mock.sentinel.max,
            versions=mock.sentinel.version,
            delimiter=mock.sentinel.new_delimiter,
            projection=mock.sentinel.projection,
            pageToken=mock.sentinel.page_token)
