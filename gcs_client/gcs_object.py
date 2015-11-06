# -*- coding: utf-8 -*-
from __future__ import absolute_import

import collections
import os

import requests

from gcs_client import common
from gcs_client import errors


BLOCK_MULTIPLE = 256 * 1024
DEFAULT_BLOCK_SIZE = 4 * BLOCK_MULTIPLE


class Object(common.Fillable):
    """GCS Stored Object Object representation."""

    _required_attributes = common.GCS._required_attributes + ['name', 'bucket']

    def __init__(self, bucket=None, name=None, generation=None,
                 credentials=None, retry_params=None):
        """Initialize an Object object.

        :param name: Name of the bucket to use.
        :param credentials: A credentials object to authorize the connection.
        """
        super(Object, self).__init__(credentials, retry_params)
        self.name = name
        self.bucket = bucket
        self.generation = generation

    @common.retry
    @common.convert_exception
    def _get_data(self):
        req = self._service.objects().get(bucket=self.bucket, object=self.name,
                                          generation=self.generation)
        return req.execute()

    def exists(self):
        try:
            self.size
        except AttributeError:
            return False
        return True

    @common.is_complete
    @common.retry
    @common.convert_exception
    def delete(self, generation=None, if_generation_match=None,
               if_generation_not_match=None, if_metageneration_match=None,
               if_metageneration_not_match=None):

        req = self._service.objects().delete(
            bucket=self.bucket,
            object=self.name,
            generation=generation or self.generation,
            ifGenerationMatch=if_generation_match,
            ifGenerationNotMatch=if_generation_not_match,
            ifMetagenerationMatch=if_metageneration_match,
            ifMetagenerationNotMatch=if_metageneration_not_match)
        req.execute()

    @common.is_complete
    def open(self, mode='r', generation=None):
        if mode not in ('r', 'w'):
            raise IOError('Only r or w modes supported')
        if mode == 'w':
            return GCSObjResumableUpload(self.bucket, self.name,
                                         self._credentials,
                                         retry_params=self.retry_params)

        if not self.exists():
            raise IOError('Object %s does not exist in bucket %s' %
                          (self.name, self.bucket))
        return GCSObjResumableDownload(self.bucket, self.name,
                                       self._credentials, None, self.size,
                                       self.retry_params)


class GCSObjResumableDownload(object):

    URL = 'https://www.googleapis.com/storage/v1/b/%s/o/%s'

    def __init__(self, bucket, name, credentials, chunksize=None, size=None,
                 retry_params=None):
        self._chunksize = chunksize or DEFAULT_BLOCK_SIZE
        assert self._chunksize % BLOCK_MULTIPLE == 0, \
            'chunksize must be multiple of %s' % BLOCK_MULTIPLE
        self.name = name
        self.bucket = bucket
        self.size = size
        self._offset = 0
        self._eof = False
        self._read_bytes = 0
        self._credentials = credentials
        self._buffer = _Buffer()
        self._retry_params = retry_params
        self._location = self.URL % (bucket, name)
        self._open()

    @common.retry
    def _open(self):
        auth_token = self._credentials.get_access_token().access_token
        headers = {'Authorization': 'Bearer ' + auth_token}
        r = requests.head(self._location, headers=headers)
        if r.status_code != requests.codes.ok:
            raise errors.create_http_exception(
                r.status_code,
                'Error opening object %s in bucket %s: %s' %
                (self.name, self.bucket, r.status_code, r.content))
        self.closed = False

    def tell(self):
        return self._offset

    def seek(self, offset, whence=os.SEEK_SET):
        self._check_is_open()

        if whence == os.SEEK_SET:
            position = offset
        elif whence == os.SEEK_CUR:
            position += offset
        elif whence == os.SEEK_END:
            position = self.size + offset
        else:
            raise ValueError('whence value %s is invalid.' % whence)

        position = min(position, self.size)
        position = max(position, 0)
        # TODO: This could be optimized to not discard all buffer for small
        # movements.
        self._offset = self._read_bytes = position
        self._buffer.clear()

    def _check_is_open(self):
        if self.closed:
            raise IOError('File is closed')

    def write(self, data):
        self._check_is_open()
        raise IOError('File open for write, cannot read')

    def close(self):
        if not self.closed:
            self.closed = True

    def read(self, size=None):
        self._check_is_open()

        if size is 0 or self._eof:
            return ''

        while not self._eof and (not size or len(self._buffer) < size):
            data, self._eof = self._get_data(self._chunksize, self._read_bytes)
            self._read_bytes += len(data)
            self._buffer.write(data)

        data = self._buffer.read(size)
        self._offset += len(data)
        return data

    @common.retry
    def _get_data(self, size, begin=0):
        if not size:
            return ''

        auth_token = self._credentials.get_access_token().access_token
        end = begin + size - 1
        headers = {'Authorization': 'Bearer ' + auth_token,
                   'Range': 'bytes=%d-%d' % (begin, end)}
        params = {'alt': 'media'}
        r = requests.get(self._location, params=params, headers=headers)
        expected = (requests.codes.ok, requests.codes.partial_content,
                    requests.codes.requested_range_not_satisfiable)

        if r.status_code not in expected:
            raise errors.create_http_exception(
                r.status_code,
                'Error reading object %s in bucket %s: %s' %
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


class GCSObjResumableUpload(object):
    URL = 'https://www.googleapis.com/upload/storage/v1/b/%s/o'

    def __init__(self, bucket, name, credentials, chunksize=None, size=None,
                 retry_params=None):
        self._chunksize = chunksize or DEFAULT_BLOCK_SIZE
        assert self._chunksize % BLOCK_MULTIPLE == 0, \
            'chunksize must be multiple of %s' % BLOCK_MULTIPLE
        self.name = name
        self.bucket = bucket
        self.size = size or 0
        self._written = 0
        self._credentials = credentials
        self._buffer = _Buffer()
        self._retry_params = retry_params
        self._open()

    @common.retry
    def _open(self):
        initial_url = self.URL % self.bucket
        params = {'uploadType': 'resumable', 'name': self.name}
        auth_token = self._credentials.get_access_token().access_token
        headers = {'x-goog-resumable': 'start',
                   'Authorization': 'Bearer ' + auth_token,
                   'Content-type': 'application/octet-stream'}
        r = requests.post(initial_url, params=params, headers=headers)
        self.closed = r.status_code != requests.codes.ok
        if self.closed:
            raise errors.create_http_exception(
                r.status_code, 'Could not open object %s in bucket %s: %s-%s' %
                (self.name, self.bucket, r.status_code, r.content))
        self._location = r.headers['Location']

    def tell(self):
        return self.size

    def seek(self, offset, whence=None):
        raise IOError("Object doesn't support seek operation")

    def _check_is_open(self):
        if self.closed:
            raise IOError('File is closed')

    def read(self, size=None):
        self._check_is_open()
        raise IOError('File open for write, cannot read')

    def write(self, data):
        self._check_is_open()
        self.size += len(data)

        self._buffer.write(data)
        while len(self._buffer) >= self._chunksize:
            self._send_data(self._buffer.read(self._chunksize))

    def close(self):
        if not self.closed:
            self._send_data(self._buffer.read(), finalize=True)
            self.closed = True

    @common.retry
    def _send_data(self, data, finalize=False):
        if not (data or finalize):
            return

        if not data:
            size = self.size
            data_range = 'bytes */%s' % size
        else:
            start = self._written
            end = self._written + len(data) - 1
            size = self.size if finalize else '*'
            data_range = 'bytes %s-%s/%s' % (start, end, size)

        auth_token = self._credentials.get_access_token().access_token
        headers = {'Authorization': 'Bearer ' + auth_token,
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
        self._written += len(data)

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
