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

from gcs_client import project


class TestProject(unittest.TestCase):

    @mock.patch('gcs_client.common.GCS.__init__')
    def test_init(self, mock_init):
        """Test init providing all arguments."""
        prj = project.Project(mock.sentinel.project_id,
                              mock.sentinel.credentials,
                              mock.sentinel.retry_params)
        mock_init.assert_called_once_with(mock.sentinel.credentials,
                                          mock.sentinel.retry_params)
        self.assertEqual(mock.sentinel.project_id, prj.project_id)

    @mock.patch('gcs_client.common.GCS.__init__')
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

    @mock.patch('gcs_client.bucket.Bucket.obj_from_data')
    def test_list(self, obj_mock):
        """Test default bucket listing."""
        expected = [mock.sentinel.result1, mock.sentinel.result2]
        obj_mock.side_effect = expected
        serv = mock.Mock()
        items = {'items': [mock.sentinel.bucket1, mock.sentinel.bucket2]}
        buckets_mock = serv.buckets.return_value
        buckets_mock.list.return_value.execute.return_value = items
        buckets_mock.list_next.return_value = None

        name = 'project_name'
        prj = project.Project(name, mock.Mock())
        prj._service = serv

        fields = mock.sentinel.fields_to_return
        limit = mock.sentinel.max_results
        result = prj.list(fields, limit)
        self.assertEqual(expected, result)

        serv.buckets.assert_called_once_with()
        buckets_mock.list.assert_called_once_with(project=name, fields=fields,
                                                  maxResults=limit)
        self.assertEqual(2, obj_mock.call_count)
        buckets_mock.list_next.assert_called_once_with(
            buckets_mock.list.return_value, items)

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
