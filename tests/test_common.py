#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_common
----------------------------------

Tests common classes.
"""
import unittest

import mock
from apiclient import errors

from gcs_client import common


class TestGCS(unittest.TestCase):
    """Test Google Cloud Service base class."""

    def setUp(self):
        self.test_class = common.GCS

    @mock.patch.object(common.discovery, 'build')
    def test_init_without_credentials(self, mock_build):
        """Test init without credentials tries to do discovery."""
        self.gcs = self.test_class(None)
        self.assertIsNone(self.gcs.credentials)
        self.assertEqual(mock_build.return_value, self.gcs._service)
        mock_build.assert_called_once_with('storage',
                                           self.test_class.api_version,
                                           credentials=None)

    @mock.patch.object(common.discovery, 'build')
    def test_init_with_credentials(self, mock_build):
        """Test init with credentials does discovery."""
        gcs = self.test_class(mock.sentinel.credentials)
        self.assertEqual(mock.sentinel.credentials, gcs.credentials)
        self.assertEqual(mock_build.return_value, gcs._service)
        mock_build.assert_called_once_with(
            'storage', self.test_class.api_version,
            credentials=mock.sentinel.credentials)

    @mock.patch.object(common.discovery, 'build')
    def test_set_credentials(self, mock_build):
        """Setting credentials does discovery."""
        gcs = self.test_class(None)
        mock_build.reset_mock()
        gcs.credentials = mock.sentinel.new_credentials
        self.assertEqual(mock.sentinel.new_credentials, gcs.credentials)
        mock_build.assert_called_once_with(
            'storage', self.test_class.api_version,
            credentials=mock.sentinel.new_credentials)

    @mock.patch.object(common.discovery, 'build')
    def test_change_credentials(self, mock_build):
        """Changing credentials does discovery with new credentials."""
        gcs = self.test_class(mock.sentinel.credentials)
        mock_build.reset_mock()
        gcs.credentials = mock.sentinel.new_credentials
        self.assertEqual(mock.sentinel.new_credentials, gcs.credentials)
        mock_build.assert_called_once_with(
            'storage', self.test_class.api_version,
            credentials=mock.sentinel.new_credentials)

    @mock.patch.object(common.discovery, 'build')
    def test_set_same_credentials(self, mock_build):
        """Setting same credentials doesn't do discovery."""
        gcs = self.test_class(mock.sentinel.credentials)
        mock_build.reset_mock()
        gcs.credentials = mock.sentinel.credentials
        self.assertEqual(mock.sentinel.credentials, gcs.credentials)
        self.assertFalse(mock_build.called)

    @mock.patch.object(common.discovery, 'build')
    def test_clear_credentials(self, mock_build):
        """Clearing credentials removes service."""
        gcs = self.test_class(mock.sentinel.credentials)
        mock_build.reset_mock()
        gcs.credentials = None
        self.assertIsNone(gcs.credentials)
        mock_build.assert_called_once_with('storage',
                                           self.test_class.api_version,
                                           credentials=None)


class TestFillable(TestGCS):
    """Test Fillable class."""

    def setUp(self):
        self.test_class = common.Fillable

    def test_init_without_credentials(self):
        """Variables are initialized correctly."""
        super(TestFillable, self).test_init_without_credentials()
        self.assertFalse(self.gcs._data_retrieved)
        self.assertIsNone(self.gcs._exists)

    @mock.patch.object(common.discovery, 'build')
    def test_get_data(self, mock_build):
        """Class doesn't implement _get_data method."""
        fill = self.test_class(None)
        self.assertRaises(NotImplementedError, fill._get_data)

    @mock.patch('gcs_client.common.Fillable._get_data')
    @mock.patch.object(common.discovery, 'build')
    def test_auto_fill_get_existing_attr(self, mock_build, mock_get_data):
        """Getting an attribute that exists on the model.

        When requesting a non exiting attribute the Fillable class will first
        get data (calling _get_data method) and create attributes in the object
        with that data, then try to return requested attribute.

        This test confirms that for an valid attribute we can retrieve it and
        return it.
        """
        mock_get_data.return_value = {'name': mock.sentinel.name}
        fill = self.test_class(None)
        self.assertEquals(mock.sentinel.name, fill.name)
        self.assertTrue(fill._exists)
        self.assertTrue(fill._data_retrieved)
        mock_get_data.assert_called_once_with()

        # Calling non existing attribute will not trigger another _get_data
        # call
        mock_get_data.reset_mock()
        self.assertRaises(AttributeError, getattr, fill, 'wrong_name')
        self.assertFalse(mock_get_data.called)

    @mock.patch('gcs_client.common.Fillable._get_data')
    @mock.patch.object(common.discovery, 'build')
    def test_auto_fill_get_nonexistent_attr(self, mock_build, mock_get_data):
        """Getting an attribute that exists on the model.

        When requesting a non exiting attribute the Fillable class will first
        get data (calling _get_data method) and create attributes in the object
        with that data, then try to return requested attribute.

        This test confirms that for an invalid attribute we can retrieve the
        data but we'll still return an AttributeError exception.
        """
        mock_get_data.return_value = {'name': mock.sentinel.name}
        fill = self.test_class(None)
        self.assertRaises(AttributeError, getattr, fill, 'wrong_name')
        self.assertTrue(fill._exists)
        self.assertTrue(fill._data_retrieved)
        mock_get_data.assert_called_once_with()

        # Calling another non existing attribute will not trigger another
        # _get_data call
        mock_get_data.reset_mock()
        self.assertRaises(AttributeError, getattr, fill, 'another_wrong_name')
        self.assertFalse(mock_get_data.called)

    @mock.patch('gcs_client.common.Fillable._get_data')
    @mock.patch.object(common.discovery, 'build')
    def test_auto_fill_doesnt_exist(self, mock_build, mock_get_data):
        """Raises Attribute error for non existing resource."""
        resp = mock.Mock()
        resp.status = 404
        mock_get_data.side_effect = errors.HttpError(resp, b'')
        fill = self.test_class(None)
        self.assertRaises(AttributeError, getattr, fill, 'name')
        self.assertFalse(fill._exists)
        self.assertFalse(fill._data_retrieved)
        mock_get_data.assert_called_once_with()

    @mock.patch('gcs_client.common.Fillable._get_data')
    @mock.patch.object(common.discovery, 'build')
    def test_obj_from_data(self, mock_build, mock_get_data):
        """Test obj_from_data class method."""
        data = {'name': 'my_name', 'one_entry_dict': {'value': '1dict'},
                'multi_entry_dict': {1: 1, 2: 2}}
        fill = self.test_class.obj_from_data(data, mock.sentinel.credentials)
        self.assertFalse(fill._exists)
        self.assertTrue(fill._data_retrieved)
        self.assertEqual('my_name', fill.name)
        self.assertEqual('1dict', fill.one_entry_dict)
        self.assertDictEqual({1: 1, 2: 2}, fill.multi_entry_dict)

        # Check that it will not try to retrieve data for non existing
        # attributes
        self.assertRaises(AttributeError, getattr, fill, 'wrong_name')
        self.assertFalse(mock_get_data.called)


class TestIsCompleteDecorator(unittest.TestCase):
    """Test is_complete decorator."""

    def test_missing_required_attributes_attribute(self):
        """Test decorator with missing attribute _required_attributes."""
        function = mock.Mock(__name__='fake')
        slf = mock.Mock(spec=[])
        wrapper = common.is_complete(function)
        self.assertRaises(Exception, wrapper, slf, 1, entry=2)
        self.assertFalse(function.called)

    def test_required_attributes_is_none(self):
        """Test decorator with attribute _required_attributes set to None."""
        function = mock.Mock(__name__='fake',
                             return_value=mock.sentinel.return_value)
        slf = mock.Mock(spec=[], _required_attributes=None)
        wrapper = common.is_complete(function)

        self.assertEqual(mock.sentinel.return_value, wrapper(slf, 1, entry=2))
        function.assert_called_once_with(slf, 1, entry=2)

    def test_required_attributes_is_empty(self):
        """Test decorator with empty attribute _required_attributes."""
        function = mock.Mock(__name__='fake',
                             return_value=mock.sentinel.return_value)
        slf = mock.Mock(spec=[], _required_attributes=[])
        wrapper = common.is_complete(function)

        self.assertEqual(mock.sentinel.return_value, wrapper(slf, 1, entry=2))
        function.assert_called_once_with(slf, 1, entry=2)

    def test_missing_attribute(self):
        """Test decorator when missing required attribute.."""
        function = mock.Mock(__name__='fake',
                             return_value=mock.sentinel.return_value)
        slf = mock.Mock(spec=['attr1'],
                        _required_attributes=['attr1', 'attr2'])
        wrapper = common.is_complete(function)

        self.assertRaises(Exception, wrapper, slf, 1, entry=2)
        self.assertFalse(function.called)

    def test_complete(self):
        """Test decorator when we have all required attributes."""
        function = mock.Mock(__name__='fake',
                             return_value=mock.sentinel.return_value)
        slf = mock.Mock(spec=['attr1', 'attr2'],
                        _required_attributes=['attr1', 'attr2'])
        wrapper = common.is_complete(function)

        self.assertEqual(mock.sentinel.return_value, wrapper(slf, 1, entry=2))
        function.assert_called_once_with(slf, 1, entry=2)
