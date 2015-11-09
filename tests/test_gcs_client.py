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
test_gcs_client
----------------------------------

Tests for `gcs_client` module.
"""

import unittest

import gcs_client


class TestGcs_client(unittest.TestCase):

    def test_bucket_accessible(self):
        from gcs_client import bucket
        self.assertIs(bucket.Bucket, gcs_client.Bucket)

    def test_project_accessible(self):
        from gcs_client import project
        self.assertIs(project.Project, gcs_client.Project)

    def test_credentials_accessible(self):
        from gcs_client import credentials
        self.assertIs(credentials.Credentials, gcs_client.Credentials)

    def test_object_accessible(self):
        from gcs_client import gcs_object
        self.assertIs(gcs_object.Object, gcs_client.Object)

    def test_retry_params_accessible(self):
        from gcs_client import common
        self.assertIs(common.RetryParams, gcs_client.RetryParams)

    def test_errors_accessible(self):
        from gcs_client import errors
        self.assertIs(errors, gcs_client.errors)
