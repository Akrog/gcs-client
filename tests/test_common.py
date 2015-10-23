#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_common
----------------------------------

Tests common classes.
"""
import unittest

import mock

from gcs_client import common


class Test_GCS(unittest.TestCase):
    """Test Google Cloud Service base class."""

    @mock.patch.object(common.discovery, 'build')
    def test_init_without_credentials(self, mock_build):
        """Test init without credentials tries to do discovery."""
        gcs = common.GCS(None)
        self.assertEqual(None, gcs.credentials)
        self.assertEqual(mock_build.return_value, gcs._service)
        mock_build.assert_called_once_with( 'storage', common.GCS.api_version,
                                            credentials=None)

    @mock.patch.object(common.discovery, 'build')
    def test_init_with_credentials(self, mock_build):
        """Test init with credentials does discovery."""
        gcs = common.GCS(mock.sentinel.credentials)
        self.assertEqual(mock.sentinel.credentials, gcs.credentials)
        self.assertEqual(mock_build.return_value, gcs._service)
        mock_build.assert_called_once_with(
            'storage', common.GCS.api_version,
            credentials=mock.sentinel.credentials)

    @mock.patch.object(common.discovery, 'build')
    def test_set_credentials(self, mock_build):
        """Setting credentials does discovery."""
        gcs = common.GCS(None)
        mock_build.reset_mock()
        gcs.credentials = mock.sentinel.new_credentials
        self.assertEqual(mock.sentinel.new_credentials, gcs.credentials)
        mock_build.assert_called_once_with(
            'storage', common.GCS.api_version,
            credentials=mock.sentinel.new_credentials)

    @mock.patch.object(common.discovery, 'build')
    def test_change_credentials(self, mock_build):
        """Changing credentials does discovery with new credentials."""
        gcs = common.GCS(mock.sentinel.credentials)
        mock_build.reset_mock()
        gcs.credentials = mock.sentinel.new_credentials
        self.assertEqual(mock.sentinel.new_credentials, gcs.credentials)
        mock_build.assert_called_once_with(
            'storage', common.GCS.api_version,
            credentials=mock.sentinel.new_credentials)

    @mock.patch.object(common.discovery, 'build')
    def test_set_same_credentials(self, mock_build):
        """Setting same credentials doesn't do discovery."""
        gcs = common.GCS(mock.sentinel.credentials)
        mock_build.reset_mock()
        gcs.credentials = mock.sentinel.credentials
        self.assertEqual(mock.sentinel.credentials, gcs.credentials)
        self.assertFalse(mock_build.called)

    @mock.patch.object(common.discovery, 'build')
    def test_clear_credentials(self, mock_build):
        """Clearing credentials removes service."""
        gcs = common.GCS(mock.sentinel.credentials)
        mock_build.reset_mock()
        gcs.credentials = None
        self.assertEqual(None, gcs.credentials)
        mock_build.assert_called_once_with( 'storage', common.GCS.api_version,
                                            credentials=None)
