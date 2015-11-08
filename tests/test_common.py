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
test_common
----------------------------------

Tests common classes and decorators.
"""
import unittest

import mock

from apiclient import errors

from gcs_client import common
from gcs_client import errors as gcs_errors


class TestGCS(unittest.TestCase):
    """Test Google Cloud Service base class."""

    def setUp(self):
        self.test_class = common.GCS

    @mock.patch.object(common.discovery, 'build')
    def test_init_without_credentials(self, mock_build):
        """Test init without credentials tries to do discovery."""
        # NOTE(geguileo): We store gcs on the instance so Fillable tests can
        # use it.
        self.gcs = self.test_class(None)
        self.assertIsNone(self.gcs.credentials)
        self.assertEqual(mock_build.return_value, self.gcs._service)
        mock_build.assert_called_once_with('storage',
                                           self.test_class.api_version,
                                           credentials=None)
        self.assertIs(common.RetryParams.get_default(), self.gcs._retry_params)

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

    @mock.patch.object(common.discovery, 'build')
    def test_get_retry_params(self, mock_build):
        """Test retry_params getter method."""
        gcs = self.test_class(mock.sentinel.credentials)
        self.assertIs(common.RetryParams.get_default(), gcs._retry_params)
        self.assertIs(common.RetryParams.get_default(), gcs.retry_params)

    @mock.patch.object(common.discovery, 'build')
    def test_set_retry_params_to_none(self, mock_build):
        """Test retry_params setter method with None value."""
        gcs = self.test_class(mock.sentinel.credentials)
        gcs.retry_params = None
        self.assertIs(None, gcs.retry_params)

    @mock.patch.object(common.discovery, 'build')
    def test_set_retry_params(self, mock_build):
        """Test retry_params setter method with RetryParams instance."""
        gcs = self.test_class(mock.sentinel.credentials)
        new_params = common.RetryParams()
        gcs.retry_params = new_params
        self.assertIsNot(common.RetryParams.get_default(), gcs.retry_params)
        self.assertIs(new_params, gcs.retry_params)

    @mock.patch.object(common.discovery, 'build')
    def test_set_retry_params_incorrect_value(self, mock_build):
        """Test retry_params setter method with incorrect value."""
        gcs = self.test_class(mock.sentinel.credentials)
        self.assertRaises(AssertionError, setattr, gcs, 'retry_params', 1)
        self.assertIs(common.RetryParams.get_default(), gcs.retry_params)


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
    def test_auto_fill_skip_assignment(self, mock_build, mock_get_data):
        """Getting an attribute skipping existing attribute.

        When requesting a non exiting attribute the Fillable class will first
        get data (calling _get_data method) and create attributes in the object
        with that data, then try to return requested attribute.

        This test confirms that the filling of attributes will skip existing
        attributes.
        """
        mock_get_data.return_value = {'credentials': mock.sentinel.new_attr,
                                      'name': mock.sentinel.name}
        fill = self.test_class(mock.sentinel.credentials)
        self.assertEquals(mock.sentinel.name, fill.name)
        self.assertEquals(mock.sentinel.credentials, fill.credentials)
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
        mock_get_data.side_effect = gcs_errors.NotFound()
        fill = self.test_class(None)
        self.assertRaises(AttributeError, getattr, fill, 'name')
        self.assertFalse(fill._exists)
        self.assertFalse(fill._data_retrieved)
        mock_get_data.assert_called_once_with()

    @mock.patch('gcs_client.common.Fillable._get_data')
    @mock.patch.object(common.discovery, 'build')
    def test_auto_fill_other_http_error(self, mock_build, mock_get_data):
        """Raises HTTP exception on non expected HTTP exceptions."""
        mock_get_data.side_effect = gcs_errors.BadRequest()
        fill = self.test_class(None)
        self.assertRaises(gcs_errors.BadRequest, getattr, fill, 'name')
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


class TestRetryParams(unittest.TestCase):
    """Test RetryParams class."""

    def setUp(self):
        # We don't want to bring default configuration from one test to another
        if hasattr(common.RetryParams, 'default'):
            delattr(common.RetryParams, 'default')

    def test_init_default(self):
        """Test that default values for new instances are as expected."""
        params = common.RetryParams()
        self.assertEqual(5, params.max_retries)
        self.assertEqual(1, params.initial_delay)
        self.assertEqual(32, params.max_backoff)
        self.assertEqual(2, params.backoff_factor)
        self.assertTrue(params.randomize)

    def test_init_values(self):
        """Test that we can initialize values for new instances."""
        params = common.RetryParams(mock.sentinel.max_retries, 2, 3, 4, False)
        self.assertEqual(mock.sentinel.max_retries, params.max_retries)
        self.assertEqual(2, params.initial_delay)
        self.assertEqual(3, params.max_backoff)
        self.assertEqual(4, params.backoff_factor)
        self.assertFalse(params.randomize)

    def test_init_values_by_key(self):
        """Test that we can initialize values for new instances by name."""
        params = common.RetryParams(max_retries=1, initial_delay=2,
                                    max_backoff=3, backoff_factor=4,
                                    randomize=False)
        self.assertEqual(1, params.max_retries)
        self.assertEqual(2, params.initial_delay)
        self.assertEqual(3, params.max_backoff)
        self.assertEqual(4, params.backoff_factor)
        self.assertFalse(params.randomize)

    def test_get_default_singleton(self):
        """Test that get_default always returns the same instance."""
        first_params = common.RetryParams.get_default()
        second_params = common.RetryParams.get_default()
        self.assertIs(first_params, second_params)

    def test_set_default_using_instance(self):
        """Test that get_default always returns the same instance."""
        first_params = common.RetryParams.get_default()
        first_params_dict = dict(vars(common.RetryParams.get_default()))
        new_params = common.RetryParams(1, 2, 3, 4, False)
        common.RetryParams.set_default(new_params)
        second_params = common.RetryParams.get_default()
        self.assertIs(first_params, second_params)
        self.assertDictEqual(vars(new_params), vars(second_params))
        self.assertNotEqual(vars(new_params), first_params_dict)

    def test_set_default_using_positional_args(self):
        """Test that get_default always returns the same instance."""
        first_params = common.RetryParams.get_default()
        first_params_values = tuple(
            vars(common.RetryParams.get_default()).values())
        new_params = (1, 2, 3, 4, False)
        common.RetryParams.set_default(*new_params)
        second_params = common.RetryParams.get_default()
        self.assertIs(first_params, second_params)
        self.assertEqual(sorted(new_params),
                         sorted(vars(second_params).values()))
        self.assertNotEqual(sorted(new_params), sorted(first_params_values))


class TestConvertException(unittest.TestCase):
    """Test convert_exception decorator."""

    def test_convert_exception_no_exception(self):
        """Test decorator when the function doesn't raise any exception."""
        function = mock.Mock(__name__='fake',
                             return_value=mock.sentinel.funct_return)
        wrapper = common.convert_exception(function)
        result = wrapper(mock.sentinel.pos_arg, key=mock.sentinel.key_value)
        self.assertEqual(mock.sentinel.funct_return, result)
        function.assert_called_once_with(mock.sentinel.pos_arg,
                                         key=mock.sentinel.key_value)

    @mock.patch('gcs_client.errors.create_http_exception')
    def test_convert_exception_raise_exception(self, create_mock):
        """Test decorator when the function raises an exception."""
        create_mock.side_effect = gcs_errors.Http()
        exc = errors.HttpError({'status': mock.sentinel.status},
                               b'', mock.sentinel.uri)
        function = mock.Mock(__name__='fake', side_effect=exc)
        wrapper = common.convert_exception(function)
        self.assertRaises(gcs_errors.Http, wrapper, mock.sentinel.pos_arg,
                          key=mock.sentinel.key_value)
        function.assert_called_once_with(mock.sentinel.pos_arg,
                                         key=mock.sentinel.key_value)
        create_mock.assert_called_once_with(mock.sentinel.status, exc)


class TestRetry(unittest.TestCase):
    def setUp(self):
        # Set default retries to 2 retries and no delay between retries
        self.retries = 2
        common.RetryParams.set_default(self.retries, 0)

    def test_retry_no_error(self):
        """Test function is only called once if there is no error."""
        function = mock.Mock(__name__='fake',
                             return_value=mock.sentinel.funct_return)
        slf = mock.Mock(spec=[])
        wrapper = common.retry(function)
        result = wrapper(slf, mock.sentinel.pos_arg, key=mock.sentinel.key_arg)
        self.assertEqual(mock.sentinel.funct_return, result)
        function.assert_called_once_with(slf, mock.sentinel.pos_arg,
                                         key=mock.sentinel.key_arg)

    def test_retry_error_default(self):
        """Test that we retry the function and end up raising the error."""
        function = mock.Mock(__name__='fake',
                             side_effect=gcs_errors.RequestTimeout())
        slf = mock.Mock(spec=[])
        wrapper = common.retry(function)
        self.assertRaises(gcs_errors.RequestTimeout, wrapper, slf)
        # Initial call plus all the retries
        self.assertEqual(self.retries + 1, function.call_count)

    @mock.patch('time.sleep')
    def test_retry_error_default_reach_max_backoff(self, time_mock):
        """Test that we don't exceed max backoff time."""
        retries = 4
        common.RetryParams.set_default(retries, 1, 4, 2, False)
        function = mock.Mock(__name__='fake',
                             side_effect=gcs_errors.RequestTimeout())
        slf = mock.Mock(spec=[])
        wrapper = common.retry(function)
        self.assertRaises(gcs_errors.RequestTimeout, wrapper, slf)
        # Initial call plus all the retries
        self.assertEqual(retries + 1, function.call_count)
        self.assertEqual(retries, time_mock.call_count)

        delays = (1, 2, 4, 4)
        for i in range(retries):
            self.assertEqual(delays[i], time_mock.call_args_list[i][0][0])

    def test_retry_excluded_exception(self):
        """Test that we don't retry not included exceptions."""
        function = mock.Mock(__name__='fake',
                             side_effect=gcs_errors.NotFound())
        slf = mock.Mock(spec=[])
        wrapper = common.retry(function)
        self.assertRaises(gcs_errors.NotFound, wrapper, slf)
        # Initial call plus all the retries
        self.assertEqual(1, function.call_count)

    def test_retry_error_default_finally_succeeds(self):
        """Test that after retries we end up returning a result."""
        exc = gcs_errors.RequestTimeout()
        function = mock.Mock(__name__='fake', side_effect=[exc,
                             mock.sentinel.funct_return])
        slf = mock.Mock(spec=[])
        wrapper = common.retry(function)
        self.assertEqual(mock.sentinel.funct_return,
                         wrapper(slf, mock.sentinel.pos_arg,
                                 key=mock.sentinel.key_arg))
        self.assertEqual(self.retries, function.call_count)

    def test_retry_no_retry(self):
        """Test that we can set no retry on decorator call."""
        function = mock.Mock(__name__='fake',
                             side_effect=gcs_errors.RequestTimeout())
        slf = mock.Mock(_retry_params=common.RetryParams.get_default())
        wrapper = common.retry(None)(function)
        self.assertRaises(gcs_errors.RequestTimeout, wrapper, slf)
        # Initial call plus all the retries
        self.assertEqual(1, function.call_count)

    def test_retry_specify_params_decorator(self):
        """Test that we can set retry parameter on decorator call."""
        function = mock.Mock(__name__='fake',
                             side_effect=gcs_errors.RequestTimeout())
        slf = mock.Mock(spec=[])
        retries = self.retries + 1
        wrapper = common.retry(common.RetryParams(retries, 0))(function)
        self.assertRaises(gcs_errors.RequestTimeout, wrapper, slf)
        # Initial call plus all the retries
        self.assertEqual(retries + 1, function.call_count)

    def test_retry_specify_params_self(self):
        """Test that we can set retry parameter on self attribute."""
        function = mock.Mock(__name__='fake',
                             side_effect=gcs_errors.RequestTimeout())
        retries = self.retries + 1
        slf = mock.Mock(_retry_params=common.RetryParams(retries, 0))
        wrapper = common.retry(function)
        self.assertRaises(gcs_errors.RequestTimeout, wrapper, slf)
        # Initial call plus all the retries
        self.assertEqual(retries + 1, function.call_count)

    def test_retry_specify_params_self_custom_attr(self):
        """Test that we can set retry parameter on self in custom attribute."""
        function = mock.Mock(__name__='fake',
                             side_effect=gcs_errors.RequestTimeout())
        retries = self.retries + 1
        slf = mock.Mock(_retry_params=None,
                        _my_retry_params=common.RetryParams(retries, 0))
        wrapper = common.retry('_my_retry_params')(function)
        self.assertRaises(gcs_errors.RequestTimeout, wrapper, slf)
        # Initial call plus all the retries
        self.assertEqual(retries + 1, function.call_count)

    def test_retry_error_default_specify_codes(self):
        """Test that we can change retry status codes with default retries."""
        function = mock.Mock(__name__='fake',
                             side_effect=gcs_errors.NotFound())
        slf = mock.Mock(spec=[])
        error_codes = [gcs_errors.NotFound.code]
        wrapper = common.retry(error_codes=error_codes)(function)
        self.assertRaises(gcs_errors.NotFound, wrapper, slf)
        # Initial call plus all the retries
        self.assertEqual(self.retries + 1, function.call_count)

    def test_retry_error_default_specify_both(self):
        """Test that we can set both arguments in the decorator."""
        function = mock.Mock(__name__='fake',
                             side_effect=gcs_errors.NotFound())
        retries = self.retries + 1
        slf = mock.Mock(spec=[])
        error_codes = [gcs_errors.NotFound.code]
        wrapper = common.retry(common.RetryParams(retries, 0), error_codes)
        wrapper = wrapper(function)
        self.assertRaises(gcs_errors.NotFound, wrapper, slf)
        # Initial call plus all the retries
        self.assertEqual(retries + 1, function.call_count)
