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
test_object
----------------------------------

Tests for Object class.
"""

import unittest

import mock
import requests

from gcs_client import gcs_object


class TestObject(unittest.TestCase):
    """Tests for Object class."""

    @mock.patch('gcs_client.base.GCS.__init__')
    def test_init(self, mock_init):
        """Test init providing all arguments."""
        creds = mock.Mock()
        obj = gcs_object.Object(mock.sentinel.bucket, mock.sentinel.name,
                                mock.sentinel.generation,
                                creds,
                                mock.sentinel.retry_params)
        mock_init.assert_called_once_with(creds, mock.sentinel.retry_params)
        self.assertEqual(mock.sentinel.name, obj.name)
        self.assertEqual(mock.sentinel.bucket, obj.bucket)
        self.assertEqual(mock.sentinel.generation, obj.generation)

    @mock.patch('gcs_client.base.GCS.__init__')
    def test_init_defaults(self, mock_init):
        """Test init providing only required arguments."""
        obj = gcs_object.Object()
        mock_init.assert_called_once_with(None, None)
        self.assertIsNone(obj.name)
        self.assertIsNone(obj.bucket)
        self.assertIsNone(obj.generation)

    @mock.patch('gcs_client.base.GCS._request')
    def test_get_data(self, request_mock):
        """Test _get_data used when accessing non existent attributes."""
        bucket = 'bucket'
        name = 'name'
        request_mock.return_value.json.return_value = {'size': '1'}
        obj = gcs_object.Object(bucket, name, mock.sentinel.generation,
                                mock.Mock(), mock.sentinel.retry_params)

        result = obj._get_data()
        self.assertEqual({'size': '1'}, result)
        request_mock.assert_called_once_with(
            parse=True, generation=mock.sentinel.generation)

    def test_str(self):
        """Test string representation."""
        obj = gcs_object.Object('bucket', 'name')
        self.assertEqual('bucket/name', str(obj))

    @mock.patch('gcs_client.gcs_object.Object._get_data')
    def test_repr(self, mock_get_data):
        """Test repr representation."""
        mock_get_data.return_value = {'items': []}
        bucket = 'bucket'
        name = 'name'
        generation = 'generation'
        obj = gcs_object.Object(bucket, name, generation)
        self.assertEqual("gcs_client.gcs_object.Object('%s', '%s', '%s') "
                         "#etag: ?" % (bucket, name, generation), repr(obj))

    @mock.patch('gcs_client.base.GCS._request')
    def test_delete(self, request_mock):
        """Test object delete."""
        bucket = 'bucket'
        name = 'filename'
        obj = gcs_object.Object(bucket, name, mock.sentinel.generation,
                                mock.Mock(), mock.sentinel.retry_params)

        obj.delete(mock.sentinel.specific_generation,
                   mock.sentinel.if_generation_match,
                   mock.sentinel.if_generation_not_match,
                   mock.sentinel.if_metageneration_match,
                   mock.sentinel.if_metageneration_not_match)

        request_mock.assert_called_once_with(
            op='DELETE', ok=(requests.codes.no_content,),
            generation=mock.sentinel.specific_generation,
            ifGenerationMatch=mock.sentinel.if_generation_match,
            ifGenerationNotMatch=mock.sentinel.if_generation_not_match,
            ifMetagenerationMatch=mock.sentinel.if_metageneration_match,
            ifMetagenerationNotMatch=mock.sentinel.if_metageneration_not_match)

    @mock.patch('gcs_client.gcs_object.GCSObjFile')
    def test_open(self, mock_file):
        """Test open object."""
        creds = mock.Mock()
        obj = gcs_object.Object(mock.sentinel.bucket, mock.sentinel.name,
                                mock.sentinel.generation, creds,
                                mock.sentinel.retry_params,
                                mock.sentinel.chunksize)
        self.assertEqual(mock_file.return_value, obj.open(mock.sentinel.mode))
        mock_file.assert_called_once_with(mock.sentinel.bucket,
                                          mock.sentinel.name, creds,
                                          mock.sentinel.mode,
                                          mock.sentinel.chunksize,
                                          mock.sentinel.retry_params,
                                          mock.sentinel.generation)

    @mock.patch('gcs_client.gcs_object.GCSObjFile')
    def test_open_with_chunksize(self, mock_file):
        """Test open object passing chunk in the object."""
        creds = mock.Mock()
        obj = gcs_object.Object(mock.sentinel.bucket, mock.sentinel.name,
                                mock.sentinel.generation, creds,
                                mock.sentinel.retry_params,
                                mock.sentinel.chunksize)
        self.assertEqual(mock_file.return_value,
                         obj.open(mock.sentinel.mode, mock.sentinel.new_cs))
        mock_file.assert_called_once_with(mock.sentinel.bucket,
                                          mock.sentinel.name, creds,
                                          mock.sentinel.mode,
                                          mock.sentinel.new_cs,
                                          mock.sentinel.retry_params,
                                          mock.sentinel.generation)
