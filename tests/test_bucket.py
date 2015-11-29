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
test_bucket
----------------------------------

Tests for Bucket class.
"""

import unittest

import mock
import requests

from gcs_client import bucket
from gcs_client import common
from gcs_client import prefix


class TestBucket(unittest.TestCase):

    @mock.patch('gcs_client.base.GCS.__init__')
    def test_init(self, mock_init):
        """Test init providing all arguments."""
        bukt = bucket.Bucket(mock.sentinel.name, mock.sentinel.credentials,
                             mock.sentinel.retry_params)
        mock_init.assert_called_once_with(mock.sentinel.credentials,
                                          mock.sentinel.retry_params)
        self.assertEqual(mock.sentinel.name, bukt.name)

    @mock.patch('gcs_client.base.GCS.__init__')
    def test_init_defaults(self, mock_init):
        """Test init providing only required arguments."""
        bukt = bucket.Bucket(mock.sentinel.name)
        mock_init.assert_called_once_with(None, None)
        self.assertEqual(mock.sentinel.name, bukt.name)

    @mock.patch('gcs_client.base.GCS._request')
    def test_get_data(self, request_mock):
        """Test _get_data used when accessing non existent attributes."""
        bukt = bucket.Bucket(mock.sentinel.name)

        result = bukt._get_data()
        request_mock.assert_called_once_with(parse=True)
        self.assertEqual(request_mock.return_value.json.return_value,
                         result)

    def test_str(self):
        """Test string representation."""
        name = 'name'
        bukt = bucket.Bucket(name)
        self.assertEqual(name, str(bukt))

    @mock.patch('gcs_client.bucket.Bucket._get_data')
    def test_repr(self, mock_get_data):
        """Test repr representation."""
        mock_get_data.return_value = {'items': []}
        name = 'name'
        bukt = bucket.Bucket(name)
        self.assertEqual("gcs_client.bucket.Bucket('%s') #etag: ?" % name,
                         repr(bukt))

    @mock.patch('gcs_client.bucket.Bucket._request')
    @mock.patch('gcs_client.gcs_object.Object._obj_from_data')
    def test_list(self, obj_mock, mock_request):
        """Test bucket listing."""
        expected = [{'kind': 'storage#objects',
                     'items': [mock.sentinel.result1, mock.sentinel.result2],
                     'nextPageToken': mock.sentinel.next_token},
                    {'kind': 'storage#objects',
                     'items': [mock.sentinel.result3]}]
        mock_request.return_value.json.side_effect = expected

        expected2 = [mock.sentinel.result4, mock.sentinel.result5]
        obj_mock.side_effect = expected2

        creds = mock.Mock()
        retry_params = common.RetryParams.get_default()
        bukt = bucket.Bucket('name', creds)

        result = bukt.list(mock.sentinel.prefix, mock.sentinel.max_results,
                           mock.sentinel.version, mock.sentinel.delimiter,
                           mock.sentinel.projection, mock.sentinel.page_token)
        self.assertEqual(expected2, result)

        self.assertListEqual(
            [mock.call(parse=True,
                       url='https://www.googleapis.com/storage/v1/b/name/o',
                       prefix=mock.sentinel.prefix,
                       maxResults=mock.sentinel.max_results,
                       versions=mock.sentinel.version,
                       delimiter=mock.sentinel.delimiter,
                       projection=mock.sentinel.projection,
                       pageToken=mock.sentinel.page_token),
             mock.call(parse=True,
                       url='https://www.googleapis.com/storage/v1/b/name/o',
                       prefix=mock.sentinel.prefix,
                       maxResults=mock.sentinel.max_results,
                       versions=mock.sentinel.version,
                       delimiter=mock.sentinel.delimiter,
                       projection=mock.sentinel.projection,
                       pageToken=mock.sentinel.next_token)],
            mock_request.call_args_list)
        self.assertListEqual(
            [mock.call(mock.sentinel.result1, creds, retry_params),
             mock.call(mock.sentinel.result2, creds, retry_params),
             mock.call(mock.sentinel.result3, creds, retry_params)],
            obj_mock.call_args_list)

    @mock.patch('gcs_client.bucket.Bucket._request')
    @mock.patch('gcs_client.prefix.Prefix.__init__', return_value=None)
    def test_list_prefix(self, mock_init, mock_request):
        """Test bucket listing."""
        prefixes = ['prefix1/', 'prefix2/']
        mock_request.return_value.json.side_effect = [
            {'kind': 'storage#objects',
             'items': [],
             'prefixes': prefixes}]

        creds = mock.Mock()
        retry_params = common.RetryParams.get_default()
        name = 'bucket_name'
        bukt = bucket.Bucket(name, creds)

        result = bukt.list(delimiter=mock.sentinel.delimiter)

        self.assertEqual(len(prefixes), len(result))
        for prefx in result:
            self.assertIsInstance(prefx, prefix.Prefix)
        self.assertListEqual(
            [mock.call(name, pref, mock.sentinel.delimiter,
                       creds, retry_params) for pref in prefixes],
            mock_init.call_args_list)

    @mock.patch('gcs_client.base.GCS._request')
    def test_delete(self, request_mock):
        """Test bucket delete."""
        bukt = bucket.Bucket(mock.sentinel.name, mock.Mock())

        bukt.delete()
        request_mock.assert_called_once_with(op='DELETE',
                                             ok=(requests.codes.no_content,),
                                             ifMetagenerationMatch=None,
                                             ifMetagenerationNotMatch=None)

    @mock.patch('gcs_client.gcs_object.Object')
    def test_open(self, mock_obj):
        """Test open object from a bucket."""
        name = mock.sentinel.name
        creds = mock.Mock()
        retry = mock.sentinel.retry_params
        file_name = mock.sentinel.file_name
        mode = mock.sentinel.mode
        generation = mock.sentinel.generation
        chunksize = mock.sentinel.chunksize
        bukt = bucket.Bucket(name, creds, retry)
        result = bukt.open(file_name, mode, generation, chunksize)
        self.assertEqual(mock_obj.return_value.open.return_value, result)
        mock_obj.assert_called_once_with(name, file_name, generation, creds,
                                         retry, chunksize)
        mock_obj.return_value.open.assert_called_once_with(mode)
