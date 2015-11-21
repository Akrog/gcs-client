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

from __future__ import absolute_import

__all__ = ('BLOCK_MULTIPLE', 'DEFAULT_BLOCK_SIZE', 'Object', 'GCSObjFile')


import collections
import json
import os
import six

import requests

from gcs_client import base
from gcs_client import common
from gcs_client import errors


BLOCK_MULTIPLE = 256 * 1024
DEFAULT_BLOCK_SIZE = 4 * BLOCK_MULTIPLE


class Object(base.Fillable):
    """GCS Stored Object Object representation."""

    kind = 'storage#objects'
    _required_attributes = base.GCS._required_attributes + ['bucket', 'name']
    URL = base.Fillable.URL + '/%s/o/%s'

    def __init__(self, bucket=None, name=None, generation=None,
                 credentials=None, retry_params=None, chunksize=None):
        """Initialize an Object object.

        :param name: Name of the bucket to use.
        :param credentials: A credentials object to authorize the connection.
        """
        super(Object, self).__init__(credentials, retry_params)
        self.name = name
        self.bucket = bucket
        self.generation = generation
        self._chunksize = chunksize

    @common.retry
    def _get_data(self):
        r = self._request(parse=True, generation=self.generation)
        return r.json()

    @common.is_complete
    @common.retry
    def delete(self, generation=None, if_generation_match=None,
               if_generation_not_match=None, if_metageneration_match=None,
               if_metageneration_not_match=None):

        self._request(op='DELETE', ok=(requests.codes.no_content,),
                      generation=generation or self.generation,
                      ifGenerationMatch=if_generation_match,
                      ifGenerationNotMatch=if_generation_not_match,
                      ifMetagenerationMatch=if_metageneration_match,
                      ifMetagenerationNotMatch=if_metageneration_not_match)

    @common.is_complete
    def open(self, mode='r'):
        return GCSObjFile(self.bucket, self.name, self._credentials, mode,
                          self._chunksize, self.retry_params, self.generation)

    def __str__(self):
        return '%s/%s' % (self.bucket, self.name)

    def __repr__(self):
        return ("%s.%s('%s', '%s', '%s') #etag: %s" % (self.__module__,
                self.__class__.__name__, self.bucket, self.name,
                self.generation, getattr(self, 'etag', '?')))


class GCSObjFile(object):
    URL = 'https://www.googleapis.com/storage/v1/b/%s/o/%s'
    URL_UPLOAD = 'https://www.googleapis.com/upload/storage/v1/b/%s/o'

    def __init__(self, bucket, name, credentials, mode='r', chunksize=None,
                 retry_params=None, generation=None):
        if mode not in ('r', 'w'):
            raise IOError('Only r or w modes supported')
        self.mode = mode

        self._chunksize = chunksize or DEFAULT_BLOCK_SIZE
        assert self._chunksize % BLOCK_MULTIPLE == 0, \
            'chunksize must be multiple of %s' % BLOCK_MULTIPLE
        self.name = name
        self.bucket = bucket
        self._offset = 0
        self._eof = False
        self._gcs_offset = 0
        self._credentials = credentials
        self._buffer = _Buffer()
        self._retry_params = retry_params
        self._generation = generation
        try:
            self._open()
        except errors.NotFound:
            raise IOError('Object %s does not exist in bucket %s' %
                          (name, bucket))

    def _is_readable(self):
        return self.mode == 'r'

    def _is_writable(self):
        return self.mode == 'w'

    def _check_is_writable(self, action='write'):
        if self._is_readable():
            raise IOError('File open for reading, cannot %s' % action)

    def _check_is_readable(self, action='read'):
        if self._is_writable():
            raise IOError('File open for writing, cannot %s' % action)

    def _check_is_open(self):
        if self.closed:
            raise IOError('File is closed')

    @common.retry
    def _open(self):
        safe_bucket = requests.utils.quote(self.bucket, safe='')
        safe_name = requests.utils.quote(self.name, safe='')
        if self._is_readable():
            self._location = self.URL % (safe_bucket, safe_name)
            params = {'fields': 'size', 'generation': self._generation}
            headers = {'Authorization': self._credentials.authorization}
            r = requests.get(self._location, params=params, headers=headers)
            if r.status_code == requests.codes.ok:
                try:
                    self.size = int(json.loads(r.content)['size'])
                except Exception as exc:
                    raise errors.Error('Bad data returned by GCS %s' % exc)

        else:
            self.size = 0
            initial_url = self.URL_UPLOAD % safe_bucket
            params = {'uploadType': 'resumable', 'name': self.name}
            headers = {'x-goog-resumable': 'start',
                       'Authorization': self._credentials.authorization,
                       'Content-type': 'application/octet-stream'}
            r = requests.post(initial_url, params=params, headers=headers)
            if r.status_code == requests.codes.ok:
                self._location = r.headers['Location']

        if r.status_code != requests.codes.ok:
            raise errors.create_http_exception(
                r.status_code,
                'Error opening object %s in bucket %s: %s-%s' %
                (self.name, self.bucket, r.status_code, r.content))
        self.closed = False

    def tell(self):
        self._check_is_open()
        return self._offset

    def seek(self, offset, whence=os.SEEK_SET):
        self._check_is_open()
        self._check_is_readable('seek')

        if whence == os.SEEK_SET:
            position = offset
        elif whence == os.SEEK_CUR:
            position = self._offset + offset
        elif whence == os.SEEK_END:
            position = self.size + offset
        else:
            raise ValueError('whence value %s is invalid.' % whence)

        position = min(position, self.size)
        position = max(position, 0)
        # TODO: This could be optimized to not discard all buffer for small
        # movements.
        self._offset = self._gcs_offset = position
        self._buffer.clear()

    def write(self, data):
        self._check_is_open()
        self._check_is_writable()

        self.size += len(data)

        self._buffer.write(data)
        while len(self._buffer) >= self._chunksize:
            data = self._buffer.read(self._chunksize)
            self._send_data(data, self._gcs_offset)
            self._gcs_offset += len(data)

    @common.retry
    def _send_data(self, data, begin=0, finalize=False):
        if not (data or finalize):
            return

        if not data:
            size = self.size
            data_range = 'bytes */%s' % size
        else:
            end = begin + len(data) - 1
            size = self.size if finalize else '*'
            data_range = 'bytes %s-%s/%s' % (begin, end, size)

        headers = {'Authorization': self._credentials.authorization,
                   'Content-Range': data_range}
        r = requests.put(self._location, data=data, headers=headers)

        if size == '*':
            expected = requests.codes.resume_incomplete
        else:
            expected = requests.codes.ok

        if r.status_code != expected:
            raise errors.create_http_exception(
                r.status_code,
                'Error writting to object %s in bucket %s: %s-%s' %
                (self.name, self.bucket, r.status_code, r.content))

    def close(self):
        if not self.closed:
            if self._is_writable():
                self._send_data(self._buffer.read(), self._gcs_offset,
                                finalize=True)
            self.closed = True

    def read(self, size=None):
        self._check_is_open()
        self._check_is_readable()

        if size is 0 or self._eof:
            return ''

        while not self._eof and (not size or len(self._buffer) < size):
            data, self._eof = self._get_data(self._chunksize, self._gcs_offset)
            self._gcs_offset += len(data)
            self._buffer.write(data)

        data = self._buffer.read(size)
        self._offset += len(data)
        return bytes(data)

    @common.retry
    def _get_data(self, size, begin=0):
        if not size:
            return ''

        end = begin + size - 1
        headers = {'Authorization': self._credentials.authorization,
                   'Range': 'bytes=%d-%d' % (begin, end)}
        params = {'alt': 'media'}
        r = requests.get(self._location, params=params, headers=headers)
        expected = (requests.codes.ok, requests.codes.partial_content,
                    requests.codes.requested_range_not_satisfiable)

        if r.status_code not in expected:
            raise errors.create_http_exception(
                r.status_code,
                'Error reading object %s in bucket %s: %s-%s' %
                (self.name, self.bucket, r.status_code, r.content))

        if r.status_code == requests.codes.requested_range_not_satisfiable:
            return ('', True)

        content_range = r.headers.get('Content-Range')
        total_size = None
        if content_range:
            try:
                total_size = int(content_range.split('/')[-1])
                eof = total_size <= begin + len(r.content)
                self.size = total_size
            except Exception:
                eof = len(r.content) < size
        if total_size is None:
            eof = len(r.content) < size

        return (r.content, eof)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class _Buffer(object):
    def __init__(self):
        self._queue = collections.deque()
        self._size = 0

    def __len__(self):
        return self._size

    def clear(self):
        self._queue.clear()
        self._size = 0

    def write(self, data):
        if data:
            if six.PY3 and isinstance(data, six.string_types):
                data = data.encode()
            self._queue.append(memoryview(data))
            self._size += len(data)

    def read(self, size=None):
        if size is None or size > self._size:
            size = self._size

        result = bytearray(size)
        written = 0
        remaining = size
        while remaining:
            data = self._queue.popleft()
            if len(data) > remaining:
                data_view = memoryview(data)
                self._queue.appendleft(data_view[remaining:])
                data = data_view[:remaining]
            result[written: written + len(data)] = data
            written += len(data)
            remaining -= len(data)

        self._size -= size
        return result
