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
test_errors
----------------------------------

Tests errors classes.
"""

import unittest

import mock

from gcs_client import errors as gcs_errors


class TestErrors(unittest.TestCase):

    def test_init(self):
        """Test init providing all arguments."""
        http = gcs_errors.Http(mock.sentinel.message, mock.sentinel.code)
        self.assertEqual(mock.sentinel.message, http.message)
        self.assertEqual(mock.sentinel.code, http.code)

    def test_str(self):
        """Test str conversio."""
        message = 'message'
        code = 'code'
        http = gcs_errors.Http(message, code)
        self.assertIn(message, str(http))
        self.assertIn(code, str(http))

    def test_str_no_message(self):
        """Test str conversio with missing message."""
        code = 'code'
        http = gcs_errors.Http(code=code)
        self.assertIn(code, str(http))
        self.assertNotIn(':', str(http))

    def test_check_classes(self):
        """Test that error classes are dynamically created."""
        for code, (cls_name, cls_parent) in gcs_errors.http_errors.items():
            cls = getattr(gcs_errors, cls_name)
            self.assertEqual(code, cls.code)
            self.assertIn(cls_parent, cls.__bases__)

    def test_create_http_exception(self):
        """Test that create_http_exception creates specific exceptions."""
        for code, (cls_name, cls_parent) in gcs_errors.http_errors.items():
            exc = gcs_errors.create_http_exception(code, mock.sentinel.message)
            self.assertEqual(code, exc.code)
            self.assertTrue(isinstance(exc, getattr(gcs_errors, cls_name)))
            self.assertEqual(mock.sentinel.message, exc.message)

    def test_create_http_exception_str_code(self):
        """Test create_http_exception creates exceptions from str codes."""
        for code, (cls_name, cls_parent) in gcs_errors.http_errors.items():
            exc = gcs_errors.create_http_exception(str(code),
                                                   mock.sentinel.message)
            self.assertEqual(code, exc.code)
            self.assertTrue(isinstance(exc, getattr(gcs_errors, cls_name)))
            self.assertEqual(mock.sentinel.message, exc.message)

    def test_create_http_exception_non_int_code(self):
        """Test create_http_exception creates exceptions from non int codes."""
        for code, (cls_name, cls_parent) in gcs_errors.http_errors.items():
            exc = gcs_errors.create_http_exception('code',
                                                   mock.sentinel.message)
            self.assertEqual('code', exc.code)
            self.assertIs(gcs_errors.Http, type(exc))
            self.assertEqual(mock.sentinel.message, exc.message)

    def test_create_http_exception_non_specific(self):
        """Test create_http_exception creates non specific exceptions."""
        exc = gcs_errors.create_http_exception(1, mock.sentinel.message)
        self.assertEqual(1, exc.code)
        self.assertIs(gcs_errors.Http, type(exc))
        self.assertEqual(mock.sentinel.message, exc.message)
