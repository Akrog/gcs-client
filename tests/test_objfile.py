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
test_objfile
----------------------------------

Tests for GCSObjFile class and auxiliary classes.
"""

import unittest

import mock

from gcs_client import gcs_object


class TestBuffer(unittest.TestCase):
    """Tests for _Buffer class."""

    def setUp(self):
        self.buf = gcs_object._Buffer()

    def test_init(self):
        """Test buffer initialization."""
        self.assertEqual(0, len(self.buf))

    def test_write(self):
        """Test basic write method."""
        data = b'0' * 50 + b'1' * 50
        self.buf.write(data)
        self.assertEqual(len(data), len(self.buf))
        self.assertEqual(1, len(self.buf._queue))
        self.assertEqual(data, self.buf._queue[0])

    def test_multiple_writes(self):
        """Test multiple writes."""
        data = b'0' * 50
        self.buf.write(data)
        data2 = data + b'1' * 50
        self.buf.write(data2)
        self.assertEqual(len(data) + len(data2), len(self.buf))
        self.assertEqual(2, len(self.buf._queue))
        self.assertEqual(data, self.buf._queue[0])
        self.assertEqual(data2, self.buf._queue[1])

    def test_read(self):
        """Test basic read all method."""
        data = b'0' * 50
        self.buf.write(data)
        data2 = b'1' * 50
        self.buf.write(data2)
        read = self.buf.read()
        self.assertEqual(0, len(self.buf))
        self.assertEqual(data + data2, read)
        self.assertEqual(0, len(self.buf._queue))

    def test_read_partial(self):
        """Test complex read overlapping reads from different 'chunks'."""
        data = b'0' * 20 + b'1' * 20
        self.buf.write(data)
        data2 = b'2' * 50
        self.buf.write(data2)

        read = self.buf.read(20)
        self.assertEqual(70, len(self.buf))
        self.assertEqual(data[:20], read)

        read = self.buf.read(10)
        self.assertEqual(60, len(self.buf))
        self.assertEqual(data[20:30], read)

        read = self.buf.read(30)
        self.assertEqual(30, len(self.buf))
        self.assertEqual(data[30:] + data2[:20], read)

        read = self.buf.read(40)
        self.assertEqual(0, len(self.buf))
        self.assertEqual(data2[20:], read)

    def test_clear(self):
        """Test clear method."""
        data = b'0' * 50
        self.buf.write(data)
        data2 = b'1' * 50
        self.buf.write(data2)
        self.assertEqual(len(data) + len(data2), len(self.buf))
        self.buf.clear()
        self.assertEqual(0, len(self.buf))
        self.assertEqual(0, len(self.buf._queue))


class TestObjFile(unittest.TestCase):
    """Test Object File class."""

    def test_init_wrong_mode(self):
        """Test 'rw' mode is not supported."""
        self.assertRaises(IOError, gcs_object.GCSObjFile, mock.sentinel.bucket,
                          mock.sentinel.name, mock.sentinel.credentials, 'rw')

    def test_init_wrong_chunk(self):
        """Test chunksize must be 'rw' mode is not supported."""
        self.assertRaises(AssertionError, gcs_object.GCSObjFile,
                          mock.sentinel.bucket, mock.sentinel.name,
                          mock.sentinel.credentials, 'r',
                          gcs_object.BLOCK_MULTIPLE + 1)

    @mock.patch('requests.head', **{'return_value.status_code': 404})
    def test_init_read_not_found(self, head_mock):
        access_token = 'access_token'
        creds = mock.Mock()
        creds.get_access_token.return_value.access_token = access_token
        self.assertRaises(IOError, gcs_object.GCSObjFile, mock.sentinel.bucket,
                          mock.sentinel.name, creds, 'r')

    @mock.patch('requests.head', **{'return_value.status_code': 200})
    def test_init_read(self, head_mock):
        access_token = 'access_token'
        chunk = gcs_object.DEFAULT_BLOCK_SIZE * 2
        creds = mock.Mock()
        creds.authorization = 'Bearer ' + access_token
        f = gcs_object.GCSObjFile(mock.sentinel.bucket, mock.sentinel.name,
                                  creds, 'r', chunk, mock.sentinel.size,
                                  mock.sentinel.retry_params)
        self.assertEqual(mock.sentinel.bucket, f.bucket)
        self.assertEqual(mock.sentinel.name, f.name)
        self.assertEqual(mock.sentinel.size, f.size)
        self.assertEqual(creds, f._credentials)
        self.assertEqual(mock.sentinel.retry_params, f._retry_params)
        self.assertEqual(chunk, f._chunksize)
        self.assertEqual(0, len(f._buffer))
        self.assertTrue(f._is_readable())
        self.assertFalse(f._is_writable())

        self.assertFalse(f.closed)
        self.assertEqual(0, f.tell())

        self.assertEqual(1, head_mock.call_count)
        location = head_mock.call_args[0][0]
        self.assertIn(str(mock.sentinel.bucket), location)
        self.assertIn(str(mock.sentinel.name), location)
        headers = head_mock.call_args[1]['headers']
        self.assertEqual('Bearer ' + access_token, headers['Authorization'])

    def _open(self, mode):
        if mode == 'r':
            method = 'requests.head'
        else:
            method = 'requests.post'

        self.access_token = 'access_token'
        creds = mock.Mock()
        creds.authorization = 'Bearer ' + self.access_token
        with mock.patch(method, **{'return_value.status_code': 200}):
            f = gcs_object.GCSObjFile(mock.sentinel.bucket, mock.sentinel.name,
                                      creds, mode)

        return f

    def test_write_on_read_file(self):
        f = self._open('r')
        self.assertRaises(IOError, f.write, '')

    def test_close_read_file(self):
        f = self._open('r')
        f.close()
        self.assertTrue(f.closed)
        # A second close call will do nothing
        f.close()
        self.assertTrue(f.closed)

    def test_operations_on_closed_read_file(self):
        f = self._open('r')
        f.close()
        self.assertRaises(IOError, f.read, '')
        self.assertRaises(IOError, f.write, '')
        self.assertRaises(IOError, f.tell)
        self.assertRaises(IOError, f.seek, 0)

    def test_context_manager(self):
        with self._open('r') as f:
            self.assertFalse(f.closed)
        self.assertTrue(f.closed)

    @mock.patch('requests.post', **{'return_value.status_code': 404})
    def test_init_write_not_found(self, head_mock):
        access_token = 'access_token'
        creds = mock.Mock()
        creds.get_access_token.return_value.access_token = access_token
        self.assertRaises(IOError, gcs_object.GCSObjFile, mock.sentinel.bucket,
                          mock.sentinel.name, creds, 'w')

    @mock.patch('requests.post', **{'return_value.status_code': 200})
    def test_init_write(self, post_mock):
        access_token = 'access_token'
        creds = mock.Mock()
        creds.authorization = 'Bearer ' + access_token
        f = gcs_object.GCSObjFile(mock.sentinel.bucket, mock.sentinel.name,
                                  creds, 'w',
                                  gcs_object.DEFAULT_BLOCK_SIZE * 2,
                                  mock.sentinel.size,
                                  mock.sentinel.retry_params)
        self.assertEqual(mock.sentinel.bucket, f.bucket)
        self.assertEqual(mock.sentinel.name, f.name)
        self.assertEqual(0, f.size)
        self.assertEqual(creds, f._credentials)
        self.assertEqual(mock.sentinel.retry_params, f._retry_params)
        self.assertEqual(0, len(f._buffer))
        self.assertFalse(f._is_readable())
        self.assertTrue(f._is_writable())

        self.assertFalse(f.closed)
        self.assertEqual(0, f.tell())

        self.assertEqual(1, post_mock.call_count)
        location = post_mock.call_args[0][0]
        self.assertIn(str(mock.sentinel.bucket), location)
        params = post_mock.call_args[1]['params']
        self.assertIn(mock.sentinel.name, params.values())
        headers = post_mock.call_args[1]['headers']
        self.assertEqual('Bearer ' + access_token, headers['Authorization'])

    def test_read_on_write_file(self):
        f = self._open('w')
        self.assertRaises(IOError, f.read)

    @mock.patch('gcs_client.gcs_object.GCSObjFile._send_data')
    def test_close_write_file(self, send_mock):
        f = self._open('w')
        f.close()
        send_mock.assert_called_once_with(b'', 0, finalize=True)
        send_mock.reset_mock()
        self.assertTrue(f.closed)
        # A second close call will do nothing
        f.close()
        self.assertFalse(send_mock.called)
        self.assertTrue(f.closed)

    @mock.patch('gcs_client.gcs_object.GCSObjFile._send_data')
    def test_operations_on_closed_write_file(self, send_mock):
        f = self._open('w')
        f.close()
        self.assertRaises(IOError, f.read, '')
        self.assertRaises(IOError, f.write, '')
        self.assertRaises(IOError, f.tell)
        self.assertRaises(IOError, f.seek, 0)

    def _check_get_call(self, get_mock, index, begin, end):
        call_args = get_mock.call_args_list[index]
        location = call_args[0][0]
        self.assertIn(str(mock.sentinel.bucket), location)
        self.assertIn(str(mock.sentinel.name), location)
        params = call_args[1]['params']
        self.assertEqual('media', params['alt'])
        headers = call_args[1]['headers']
        self.assertEqual('Bearer ' + self.access_token,
                         headers['Authorization'])
        self.assertEqual('bytes=%s-%s' % (begin, end - 1), headers['Range'])

    @mock.patch('requests.get')
    def test_read_all_fits_in_1_chunk(self, get_mock):
        f = self._open('r')
        expected_data = b'0' * (f._chunksize - 1)
        get_mock.side_effect = [mock.Mock(status_code=200, headers={},
                                          content=expected_data)]
        data = f.read()
        self.assertEqual(expected_data, data)
        self.assertEqual(1, get_mock.call_count)

        self._check_get_call(get_mock, 0, 0, f._chunksize)

        # Next call to read will not need to call server
        get_mock.reset_mock()
        data = f.read()
        self.assertEqual('', data)
        self.assertFalse(get_mock.called)

        f.close()

    @mock.patch('requests.get')
    def test_read_all_multiple_chunks(self, get_mock):
        f = self._open('r')
        expected_data = b'0' * ((f._chunksize - 1) * 2)
        get_mock.side_effect = [
            mock.Mock(status_code=206, content=expected_data[:f._chunksize]),
            mock.Mock(status_code=200, content=expected_data[f._chunksize:])]
        data = f.read()
        self.assertEqual(expected_data, data)
        self.assertEqual(2, get_mock.call_count)

        offsets = ((0, f._chunksize), (f._chunksize, 2 * f._chunksize))
        for i in range(2):
            self._check_get_call(get_mock, i, offsets[i][0], offsets[i][1])

        # Next call to read will not need to call server
        get_mock.reset_mock()
        data = f.read()
        self.assertEqual('', data)
        self.assertFalse(get_mock.called)

        f.close()

    @mock.patch('requests.get')
    def test_read_all_multiple_chunks_exact_size_no_header(self, get_mock):
        f = self._open('r')
        expected_data = b'0' * (f._chunksize * 2)
        get_mock.side_effect = [
            mock.Mock(status_code=206, content=expected_data[:f._chunksize]),
            mock.Mock(status_code=206, content=expected_data[f._chunksize:]),
            mock.Mock(status_code=416, content='Error blah, blah')]
        data = f.read()
        self.assertEqual(expected_data, data)
        self.assertEqual(3, get_mock.call_count)

        offsets = ((0, f._chunksize), (f._chunksize, 2 * f._chunksize),
                   (2 * f._chunksize, 3 * f._chunksize))
        for i in range(3):
            self._check_get_call(get_mock, i, offsets[i][0], offsets[i][1])

        # Next call to read will not need to call server
        get_mock.reset_mock()
        data = f.read()
        self.assertEqual('', data)
        self.assertFalse(get_mock.called)

        f.close()

    @mock.patch('requests.get')
    def test_read_all_multiple_chunks_exact_size_with_header(self, get_mock):
        f = self._open('r')
        offsets = ((0, f._chunksize), (f._chunksize, 2 * f._chunksize))
        expected_data = b'0' * (f._chunksize * 2)
        ranges = [{'Content-Range': 'bytes=%s-%s/%s' % (o[0], o[1] - 1,
                                                        offsets[-1][1])}
                  for o in offsets]
        get_mock.side_effect = [
            mock.Mock(status_code=206, content=expected_data[:f._chunksize],
                      headers=ranges[0]),
            mock.Mock(status_code=206, content=expected_data[f._chunksize:],
                      headers=ranges[1])]
        data = f.read()
        self.assertEqual(expected_data, data)
        self.assertEqual(2, get_mock.call_count)

        for i in range(2):
            self._check_get_call(get_mock, i, offsets[i][0], offsets[i][1])

        # Next call to read will not need to call server
        get_mock.reset_mock()
        data = f.read()
        self.assertEqual('', data)
        self.assertFalse(get_mock.called)

        f.close()

    @mock.patch('requests.get')
    def test_read_size_multiple_chunks(self, get_mock):
        f = self._open('r')
        offsets = ((0, f._chunksize), (f._chunksize, 2 * f._chunksize))
        expected_data = b'0' * ((f._chunksize - 1) * 2)
        get_mock.side_effect = [
            mock.Mock(status_code=206, content=expected_data[:f._chunksize]),
            mock.Mock(status_code=200, content=expected_data[f._chunksize:])]
        size = int(f._chunksize / 4)
        data = f.read(size)
        self.assertEqual(expected_data[:size], data)

        self.assertEqual(1, get_mock.call_count)
        self._check_get_call(get_mock, 0, offsets[0][0], offsets[0][1])
        get_mock.reset_mock()

        data = f.read(size)
        self.assertEqual(expected_data[size:2*size], data)
        self.assertFalse(get_mock.called)

        data = f.read(0)
        self.assertEqual('', data)
        self.assertFalse(get_mock.called)

        data = f.read(2 * f._chunksize)
        self.assertEqual(expected_data[2*size:], data)
        self._check_get_call(get_mock, 0, offsets[1][0], offsets[1][1])

        # Next call to read will not need to call server
        get_mock.reset_mock()
        data = f.read()
        self.assertEqual('', data)
        self.assertFalse(get_mock.called)

        f.close()

    @mock.patch('requests.get', **{'return_value.status_code': 404})
    def test_read_error(self, get_mock):
        with self._open('r') as f:
            self.assertRaises(gcs_object.errors.NotFound, f.read)

    @mock.patch('requests.get')
    def test_get_data_size_0(self, get_mock):
        get_mock.return_value = mock.Mock(status_code=200, content='data')
        with self._open('r') as f:
            data = f._get_data(0)
            self.assertEqual('', data)
            self.assertFalse(get_mock.called)
