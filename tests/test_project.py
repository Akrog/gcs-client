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
test_project
----------------------------------

Tests for Project class.
"""

import unittest

import mock

from gcs_client import common
from gcs_client import project


class TestProject(unittest.TestCase):

    @mock.patch('gcs_client.base.GCS.__init__')
    def test_init(self, mock_init):
        """Test init providing all arguments."""
        prj = project.Project(mock.sentinel.project_id,
                              mock.sentinel.credentials,
                              mock.sentinel.retry_params)
        mock_init.assert_called_once_with(mock.sentinel.credentials,
                                          mock.sentinel.retry_params)
        self.assertEqual(mock.sentinel.project_id, prj.project_id)

    @mock.patch('gcs_client.base.GCS.__init__')
    def test_init_defaults(self, mock_init):
        """Test init providing only required arguments."""
        prj = project.Project(mock.sentinel.project_id)
        mock_init.assert_called_once_with(None, None)
        self.assertEqual(mock.sentinel.project_id, prj.project_id)

    def test_get_default_bucket(self):
        """Test getting the default bucket name for a project."""
        name = 'project_name'
        prj = project.Project(name)
        expected = name + '.appspot.com'
        self.assertEqual(expected, prj.default_bucket_name)

    def test_get_default_bucket_none(self):
        """Test getting the default bucket name for a project."""
        prj = project.Project(None)
        self.assertEqual(None, prj.default_bucket_name)

    def test_str(self):
        """Test string representation."""
        name = 'project_name'
        prj = project.Project(name)
        self.assertEqual(name, str(prj))

    @mock.patch('gcs_client.project.Project._request')
    @mock.patch('gcs_client.bucket.Bucket.obj_from_data')
    def test_list(self, obj_mock, mock_request):
        """Test default bucket listing."""
        expected = [{'kind': 'storage#buckets',
                     'items': [mock.sentinel.result1, mock.sentinel.result2],
                     'nextPageToken': mock.sentinel.next_token},
                    {'kind': 'storage#buckets',
                     'items': [mock.sentinel.result3]}]
        mock_request.return_value.json.side_effect = expected

        expected2 = [mock.sentinel.result4, mock.sentinel.result5]
        obj_mock.side_effect = expected2

        name = 'project_name'
        creds = mock.Mock()
        retry_params = common.RetryParams.get_default()
        prj = project.Project(name, creds)

        result = prj.list(mock.sentinel.fields, mock.sentinel.max_results,
                          mock.sentinel.projection, mock.sentinel.prefix,
                          mock.sentinel.page_token)
        self.assertEqual(expected2, result)

        self.assertListEqual(
            [mock.call(parse=True,
                       url='https://www.googleapis.com/storage/v1/b',
                       project=name,
                       fields=mock.sentinel.fields,
                       maxResults=mock.sentinel.max_results,
                       projection=mock.sentinel.projection,
                       prefix=mock.sentinel.prefix,
                       pageToken=mock.sentinel.page_token),
             mock.call(parse=True,
                       url='https://www.googleapis.com/storage/v1/b',
                       project=name,
                       fields=mock.sentinel.fields,
                       maxResults=mock.sentinel.max_results,
                       projection=mock.sentinel.projection,
                       prefix=mock.sentinel.prefix,
                       pageToken=mock.sentinel.next_token)],
            mock_request.call_args_list)
        self.assertListEqual(
            [mock.call(mock.sentinel.result1, creds, retry_params),
             mock.call(mock.sentinel.result2, creds, retry_params),
             mock.call(mock.sentinel.result3, creds, retry_params)],
            obj_mock.call_args_list)

    @mock.patch('gcs_client.bucket.Bucket.obj_from_data')
    def test_create_buckets(self, obj_mock):
        """Test bucket creation."""
        obj_mock.return_value = mock.sentinel.result

        serv = mock.Mock()
        insert_mock = serv.buckets.return_value.insert

        credentials = mock.Mock()
        prj = project.Project(mock.sentinel.project, credentials)
        prj._service = serv

        result = prj.create_bucket(mock.sentinel.name, mock.sentinel.location,
                                   mock.sentinel.storage, mock.sentinel.acl,
                                   mock.sentinel.def_acl,
                                   mock.sentinel.projection)
        self.assertEqual(mock.sentinel.result, result)

        serv.buckets.assert_called_once_with()
        insert_mock.assert_called_once_with(
            project=mock.sentinel.project,
            predefinedAcl=mock.sentinel.acl,
            predefinedDefaultObjectAcl=mock.sentinel.def_acl,
            projection=mock.sentinel.projection,
            body={'name': mock.sentinel.name,
                  'location': mock.sentinel.location,
                  'storageClass': mock.sentinel.storage})
        obj_mock.assert_called_once_with(
            insert_mock.return_value.execute.return_value,
            credentials)
