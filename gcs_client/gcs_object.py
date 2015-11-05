# -*- coding: utf-8 -*-
from __future__ import absolute_import

import collections
import io

from apiclient import http as gcs_http

from gcs_client import common
import requests


BLOCK_MULTIPLE = 256 * 1024
DEFAULT_BLOCK_SIZE = 4 * BLOCK_MULTIPLE


class Object(common.Fillable):
    """GCS Stored Object Object representation."""

    _required_attributes = common.GCS._required_attributes + ['name', 'bucket']

    def __init__(self, bucket=None, name=None, generation=None,
                 credentials=None):
        """Initialize an Object object.

        :param name: Name of the bucket to use.
        :param credentials: A credentials object to authorize the connection.
        """
        super(Object, self).__init__(credentials)
        self.name = name
        self.bucket = bucket
        self.generation = generation

    @common.retry
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
    def open(self, mode, generation=None):
        if mode not in ('r', 'w'):
            raise IOError('Only r or w modes supported')
        if mode == 'w':
            return GCSObjResumableUpload(self.bucket, self.name,
                                         self._credentials)

        if not self.exists():
            raise IOError('Object %s does not exist in bucket %s' %
                          (self.name, self.bucket))
        return GCSObjFile(self.bucket, self.name, self._service, self.size,
                          generation or self.generation)


class GCSObjFile(object):
    def __init__(self, bucket, name, service, size, generation=None,
                 chunksize=None):
        self.bucket = bucket
        self.name = name
        self.generation = generation
        self._transferer = None
        self._service = service
        self.size = size
        self._chunksize = chunksize or DEFAULT_BLOCK_SIZE
        assert self._chunksize % BLOCK_MULTIPLE == 0, \
            'chunksize must be multiple of %s' % BLOCK_MULTIPLE

        req = self._service.objects().get_media(bucket=self.bucket,
                                                object=self.name,
                                                generation=self.generation)
        self._fh = io.BytesIO()
        self._transferer = gcs_http.MediaIoBaseDownload(
            self._fh, req, chunksize=self._chunksize)
        self._status = gcs_http.MediaDownloadProgress(0, int(self.size))
        self._done = False
        self._cache = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._mode = None
        self._transferer = None
        self._done = True

    def write(self, size=None):
        if self._transferer is None:
            raise IOError('File is closed')
        raise IOError('File open for read, cannot read')

    def read(self, size=None):
        if self._transferer is None:
            raise IOError('File is closed')
        if self._done:
            return b''

        remaining_in_file = int(self.size) - self._status.resumable_progress
        if size is None or size < 0:
            size = remaining_in_file
        else:
            size = min(remaining_in_file, size)
        remaining = size

        while not self._done and remaining:
            self._transferer._chunksize = min(self._chunksize, remaining)
            self._status, self._done = self._transferer.next_chunk()
            remaining = size - self._status.resumable_progress

        data = self._fh.getvalue()
        self._fh.truncate(0)
        return data


class GCSObjResumableUpload(object):
    URL = 'https://www.googleapis.com/upload/storage/v1/b/%s/o'

    def __init__(self, bucket, name, credentials, chunksize=None, size=None):
        self._chunksize = chunksize or DEFAULT_BLOCK_SIZE
        assert self._chunksize % BLOCK_MULTIPLE == 0, \
            'chunksize must be multiple of %s' % BLOCK_MULTIPLE
        self.name = name
        self.bucket = bucket
        initial_url = self.URL % bucket
        params = {'uploadType': 'resumable', 'name': name}
        auth_token = credentials.get_access_token().access_token
        headers = {'x-goog-resumable': 'start',
                   'Authorization': 'Bearer ' + auth_token,
                   'Content-type': 'application/octet-stream'}
        r = requests.post(initial_url, params=params, headers=headers)
        self.size = size or 0
        self._written = 0
        self.closed = r.status_code != 200
        if self.closed:
            raise IOError('Could not open object %s in buckets %s: '
                          '(status=%s): %s' %
                          (name, bucket, r.status_code, r.content))
        self._location = r.headers['Location']
        self._credentials = credentials
        self._buffer = _Buffer()

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
            self._closed = True

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
            expected = 308
        else:
            expected = 200

        if r.status_code != expected:
            raise IOError('Error writting to object %s in buckets %s: '
                          '(status=%s): %s' %
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
