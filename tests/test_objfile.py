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

import os
import six
import unittest

import mock

from gcs_client import errors
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

    def setUp(self):
        self.bucket = 'sentinel.bucket'
        self.name = 'sentinel.name'

    def test_init_wrong_mode(self):
        """Test 'rw' mode is not supported."""
        self.assertRaises(IOError, gcs_object.GCSObjFile, self.bucket,
                          self.name, mock.sentinel.credentials, 'rw')

    def test_init_wrong_chunk(self):
        """Test chunksize must be 'rw' mode is not supported."""
        self.assertRaises(AssertionError, gcs_object.GCSObjFile, self.bucket,
                          self.name, mock.sentinel.credentials, 'r',
                          gcs_object.BLOCK_MULTIPLE + 1)

    @mock.patch('requests.get', **{'return_value.status_code': 404})
    def test_init_read_not_found(self, get_mock):
        access_token = 'access_token'
        creds = mock.Mock()
        creds.get_access_token.return_value.access_token = access_token
        self.assertRaises(IOError, gcs_object.GCSObjFile, self.bucket,
                          self.name, creds, 'r')

    @mock.patch('requests.get', **{'return_value.status_code': 200})
    def test_init_read_non_json(self, get_mock):
        get_mock.return_value.content = 'non_json'
        access_token = 'access_token'
        creds = mock.Mock()
        creds.get_access_token.return_value.access_token = access_token
        self.assertRaises(errors.Error, gcs_object.GCSObjFile, self.bucket,
                          self.name, creds, 'r')

    @mock.patch('requests.get', **{'return_value.status_code': 404})
    def test_init_read_quote_data(self, get_mock):
        access_token = 'access_token'
        creds = mock.Mock()
        creds.get_access_token.return_value.access_token = access_token
        name = 'var/log/message.log'
        bucket = '?mybucket'
        expected_url = gcs_object.GCSObjFile._URL % ('%3Fmybucket',
                                                     'var%2Flog%2Fmessage.log')

        self.assertRaises(IOError, gcs_object.GCSObjFile, bucket, name, creds,
                          'r')
        get_mock.assert_called_once_with(expected_url, headers=mock.ANY,
                                         params={'fields': 'size',
                                                 'generation': None})

    @mock.patch('requests.get', **{'return_value.status_code': 200})
    def test_init_read(self, get_mock):
        size = 123
        get_mock.return_value.content = '{"size": "%s"}' % size
        access_token = 'access_token'
        chunk = gcs_object.DEFAULT_BLOCK_SIZE * 2
        creds = mock.Mock()
        creds.authorization = 'Bearer ' + access_token
        f = gcs_object.GCSObjFile(self.bucket, self.name, creds, 'r', chunk,
                                  mock.sentinel.retry_params)
        self.assertEqual(self.bucket, f.bucket)
        self.assertEqual(self.name, f.name)
        self.assertEqual(size, f.size)
        self.assertEqual(creds, f._credentials)
        self.assertEqual(mock.sentinel.retry_params, f._retry_params)
        self.assertEqual(chunk, f._chunksize)
        self.assertEqual(0, len(f._buffer))
        self.assertTrue(f._is_readable())
        self.assertFalse(f._is_writable())

        self.assertFalse(f.closed)
        self.assertEqual(0, f.tell())

        self.assertEqual(1, get_mock.call_count)
        location = get_mock.call_args[0][0]
        self.assertIn(self.bucket, location)
        self.assertIn(self.name, location)
        headers = get_mock.call_args[1]['headers']
        self.assertEqual('Bearer ' + access_token, headers['Authorization'])

    def _open(self, mode):
        if mode == 'r':
            method = 'requests.get'
        else:
            method = 'requests.post'

        self.access_token = 'access_token'
        creds = mock.Mock()
        creds.authorization = 'Bearer ' + self.access_token
        ret_val = mock.Mock(status_code=200,  content='{"size": "123"}',
                            headers={'Location': mock.sentinel.location})
        with mock.patch(method, return_value=ret_val):
            f = gcs_object.GCSObjFile(self.bucket, self.name, creds, mode)

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
        self.assertRaises(IOError, gcs_object.GCSObjFile, self.bucket,
                          self.name, creds, 'w')

    @mock.patch('requests.post', **{'return_value.status_code': 200})
    def test_init_write(self, post_mock):
        access_token = 'access_token'
        creds = mock.Mock()
        creds.authorization = 'Bearer ' + access_token
        f = gcs_object.GCSObjFile(self.bucket, self.name, creds, 'w',
                                  gcs_object.DEFAULT_BLOCK_SIZE * 2,
                                  mock.sentinel.retry_params)
        self.assertEqual(self.bucket, f.bucket)
        self.assertEqual(self.name, f.name)
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
        self.assertIn(str(self.bucket), location)
        params = post_mock.call_args[1]['params']
        self.assertIn(self.name, params.values())
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

    @mock.patch('requests.put', **{'return_value.status_code': 200})
    def test_write_all_fits_in_1_chunk(self, put_mock):
        f = self._open('w')
        data = b'*' * (f._chunksize - 1)
        f.write(data)

        # Since we haven't written enough data we shouldn't have sent anything
        self.assertFalse(put_mock.called)

        # Closing the file will trigger sending the data
        f.close()
        headers = {'Authorization': 'Bearer ' + self.access_token,
                   'Content-Range': 'bytes 0-%s/%s' % (len(data) - 1,
                                                       len(data))}
        put_mock.assert_called_once_with(mock.sentinel.location, data=data,
                                         headers=headers)

    @mock.patch('requests.put')
    def test_write_all_multiple_chunks(self, put_mock):
        put_mock.side_effect = [mock.Mock(status_code=308),
                                mock.Mock(status_code=200)]
        f = self._open('w')
        data1 = b'*' * (f._chunksize - 1)
        f.write(data1)

        # Since we haven't written enough data we shouldn't have sent anything
        self.assertFalse(put_mock.called)

        data2 = b'-' * f._chunksize
        f.write(data2)

        # This second write will trigger 1 data send
        headers = {'Authorization': 'Bearer ' + self.access_token,
                   'Content-Range': 'bytes 0-%s/*' % (f._chunksize - 1)}
        put_mock.assert_called_once_with(mock.sentinel.location,
                                         data=data1 + data2[0:1],
                                         headers=headers)
        put_mock.reset_mock()

        # Closing the file will trigger sending the rest of the data
        f.close()
        headers['Content-Range'] = 'bytes %s-%s/%s' % (f._chunksize,
                                                       (f._chunksize * 2) - 2,
                                                       f._chunksize * 2 - 1)
        put_mock.assert_called_once_with(mock.sentinel.location,
                                         data=data2[1:],
                                         headers=headers)

    @mock.patch('requests.put', **{'return_value.status_code': 200})
    def test_write_exactly_1_chunk(self, put_mock):
        put_mock.side_effect = [mock.Mock(status_code=308),
                                mock.Mock(status_code=200)]
        f = self._open('w')
        data = b'*' * f._chunksize

        # This will trigger sending the data
        f.write(data)
        headers = {'Authorization': 'Bearer ' + self.access_token,
                   'Content-Range': 'bytes 0-%s/*' % (len(data) - 1)}
        put_mock.assert_called_once_with(mock.sentinel.location, data=data,
                                         headers=headers)

        # Closing the file will trigger sending the finalization of the file
        put_mock.reset_mock()
        f.close()
        headers['Content-Range'] = 'bytes */%s' % f._chunksize
        put_mock.assert_called_once_with(mock.sentinel.location, data=b'',
                                         headers=headers)

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

    def _check_seek(self, offset, whence, expected_initial=None):
        with mock.patch('requests.get') as get_mock:
            block = gcs_object.DEFAULT_BLOCK_SIZE
            f = self._open('r')
            f.size = 4 * block
            if expected_initial is None:
                expected_initial = f.size
            expected_data = b'0' * block
            get_mock.return_value = mock.Mock(status_code=206,
                                              content=expected_data)
            f.read(2 * block)
            f.seek(offset, whence)
            self.assertEqual(0, len(f._buffer))
            f.read(block)

            offsets = ((0, block), (block, 2 * block),
                       (expected_initial, expected_initial + block))
            for i in range(len(offsets)):
                self._check_get_call(get_mock, i, offsets[i][0], offsets[i][1])

            f.close()

    def test_seek_read_set(self):
        self._check_seek(10, os.SEEK_SET, 10)

    def test_seek_read_set_beyond_bof(self):
        self._check_seek(-10, os.SEEK_SET, 0)

    def test_seek_read_set_beyond_eof(self):
        self._check_seek(six.MAXSIZE, os.SEEK_SET)

    def test_seek_read_cur(self):
        self._check_seek(10, os.SEEK_CUR,
                         10 + (2 * gcs_object.DEFAULT_BLOCK_SIZE))

    def test_seek_read_cur_negative(self):
        self._check_seek(-10, os.SEEK_CUR,
                         -10 + (2 * gcs_object.DEFAULT_BLOCK_SIZE))

    def test_seek_read_cur_beyond_bof(self):
        self._check_seek(-3 * gcs_object.DEFAULT_BLOCK_SIZE, os.SEEK_CUR, 0)

    def test_seek_read_cur_beyond_eof(self):
        self._check_seek(six.MAXSIZE, os.SEEK_CUR,
                         4 * gcs_object.DEFAULT_BLOCK_SIZE)

    def test_seek_read_end_negative(self):
        self._check_seek(-10, os.SEEK_END,
                         -10 + (4 * gcs_object.DEFAULT_BLOCK_SIZE))

    def test_seek_read_end_beyond_bof(self):
        self._check_seek(-six.MAXSIZE, os.SEEK_END, 0)

    def test_seek_read_end_beyond_eof(self):
        self._check_seek(six.MAXSIZE, os.SEEK_END,
                         4 * gcs_object.DEFAULT_BLOCK_SIZE)

    @mock.patch('requests.get')
    def test_seek_read_wrong_whence(self, get_mock):
        with self._open('r') as f:
            self.assertRaises(ValueError, f.seek, 0, -1)
