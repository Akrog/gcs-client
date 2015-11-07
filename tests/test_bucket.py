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

from gcs_client import bucket


class TestBucket(unittest.TestCase):

    @mock.patch('gcs_client.common.Fillable.__init__')
    def test_init(self, mock_init):
        """Test init providing all arguments."""
        bukt = bucket.Bucket(mock.sentinel.name, mock.sentinel.credentials,
                             mock.sentinel.retry_params)
        mock_init.assert_called_once_with(mock.sentinel.credentials,
                                          mock.sentinel.retry_params)
        self.assertEqual(mock.sentinel.name, bukt.name)

    @mock.patch('gcs_client.common.Fillable.__init__')
    def test_init_defaults(self, mock_init):
        """Test init providing only required arguments."""
        bukt = bucket.Bucket(mock.sentinel.name)
        mock_init.assert_called_once_with(None, None)
        self.assertEqual(mock.sentinel.name, bukt.name)

    def test_get_data(self):
        """Test _get_data used when accessing non existent attributes."""
        bukt = bucket.Bucket(mock.sentinel.name)
        serv = mock.Mock()
        get_mock = serv.buckets.return_value.get
        get_mock.return_value.execute.return_value = mock.sentinel.ret_val
        bukt._service = serv

        result = bukt._get_data()
        self.assertEqual(mock.sentinel.ret_val, result)
        get_mock.assert_called_once_with(bucket=mock.sentinel.name)
        get_mock.return_value.execute.assert_called_once_with()

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

    @mock.patch('gcs_client.gcs_object.Object.obj_from_data')
    def test_list(self, obj_mock):
        """Test bucket listing."""
        expected = [mock.sentinel.result1, mock.sentinel.result2]
        obj_mock.side_effect = expected
        serv = mock.Mock()
        items = {'items': [mock.sentinel.obj1, mock.sentinel.obj2]}
        objs_mock = serv.objects.return_value
        objs_mock.list.return_value.execute.return_value = items
        objs_mock.list_next.return_value = None

        bukt = bucket.Bucket(mock.sentinel.name, mock.Mock())
        bukt._service = serv

        prefix = mock.sentinel.prefix
        limit = mock.sentinel.max_results
        vers = mock.sentinel.versions
        delim = mock.sentinel.delimiter
        projection = mock.sentinel.projection

        result = bukt.list(prefix, limit, vers, delim, projection)
        self.assertEqual(expected, result)

        serv.objects.assert_called_once_with()
        objs_mock.list.assert_called_once_with(bucket=mock.sentinel.name,
                                               delimiter=delim, prefix=prefix,
                                               maxResults=limit,
                                               projection=projection,
                                               versions=vers)
        self.assertEqual(2, obj_mock.call_count)
        objs_mock.list_next.assert_called_once_with(
            objs_mock.list.return_value, items)

    def test_delete(self):
        """Test bucket delete."""
        bukt = bucket.Bucket(mock.sentinel.name, mock.Mock())
        bukt._service = mock.Mock()

        bukt.delete()
        delete = bukt._service.buckets.return_value.delete
        delete.assert_called_once_with(bucket=mock.sentinel.name)
        delete.return_value.execute.assert_called_once_with()

    @mock.patch('gcs_client.gcs_object.Object')
    def test_open(self, mock_obj):
        """Test open object from a bucket."""
        name = mock.sentinel.name
        creds = mock.Mock()
        retry = mock.sentinel.retry_params
        file_name = mock.sentinel.file_name
        mode = mock.sentinel.mode
        generation = mock.sentinel.generation

        bukt = bucket.Bucket(name, creds, retry)
        result = bukt.open(file_name, mode, generation)
        self.assertEqual(mock_obj.return_value.open.return_value, result)
        mock_obj.assert_called_once_with(name, file_name, generation, creds,
                                         retry)
        mock_obj.return_value.open.assert_called_once_with(mode, generation)
