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
    """GCS Stored Object Object representation.

    :ivar bucket: The name of the bucket containing this object.
    :vartype bucket: string

    :ivar contentType: Content-Type of the object data.
    :vartype contentType: string

    :ivar crc32c: CRC32c checksum, as described in RFC 4960, Appendix B;
                  encoded using base64 in big-endian byte order.
    :vartype crc32c: string

    :ivar etag: HTTP 1.1 Entity tag for the object.
    :vartype etag: string

    :ivar generation: The content generation of this object. Used for object
                      versioning.
    :vartype generation: long

    :ivar id: The ID of the object.
    :vartype id: string

    :ivar kind: The kind of item this is. For objects, this is always
                storage#object.
    :vartype kind: string

    :ivar md5Hash: MD5 hash of the data; encoded using base64.
    :vartype md5Hash: string

    :ivar mediaLink: Media download link.
    :vartype mediaLink: string

    :ivar metadata: User-provided metadata, in key/value pairs.
    :vartype metadata: dict

    :ivar metageneration: The version of the metadata for this object at this
                          generation.  Used for preconditions and for
                          detecting changes in metadata.  A metageneration
                          number is only meaningful in the context of a
                          particular generation of a particular object.
    :vartype metageneration: long

    :ivar name: The name of this object.
    :vartype name: string

    :ivar owner: The owner of the object.  This will always be the uploader of
                 the object.  Contains entity and entityId keys.
    :vartype owner: dict

    :ivar selfLink: The link to this object.
    :vartype selfLink: string

    :ivar size: Content-Length of the data in bytes.
    :vartype size: unsigned long

    :ivar storageClass: Storage class of the object.
    :vartype storageClass: string

    :ivar timeCreated: The creation time of the object in RFC 3339 format.
    :vartype timeCreated: string

    :ivar timeDeleted: The deletion time of the object in RFC 3339 format. Will
                       be None if this version of the object has not been
                       deleted.
    :vartype timeDeleted: string

    :ivar updated: The modification time of the object metadata in RFC 3339
                   format.
    :vartype updated: string
    """

    kind = 'storage#objects'
    timeDeleted = None
    metadata = {}
    _required_attributes = base.GCS._required_attributes + ['bucket', 'name']
    _URL = base.Fillable._URL + '/{bucket}/o/{name}'

    def __init__(self, bucket=None, name=None, generation=None,
                 credentials=None, retry_params=None, chunksize=None):
        """Initialize an Object object.

        :param bucket: Name of the bucket to use.
        :type bucket: String
        :param name: Name of the object.
        :type name: String
        :param generation: If present, selects a specific revision of this
                           object (as opposed to the latest version, the
                           default).
        :type generation: long
        :param credentials: A credentials object to authorize the connection.
        :type credentials: Credentials
        :param retry_params: Retry configuration used for communications with
                             GCS.  If None is passed default retries will be
                             used.
        :type retry_params: RetryParams or NoneType
        :param chunksize: Size in bytes of the payload to send/receive to/from
                          GCS.  Default is gcs_client.DEFAULT_BLOCK_SIZE
        :type chunksize: int
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
        """Deletes an object and its metadata.

        Deletions are permanent if versioning is not enabled for the bucket, or
        if the generation parameter is used.

        The authenticated user in the credentials must have WRITER permissions
        on the bucket.

        :param generation: If present, permanently deletes a specific revision
                           of this object (as opposed to the latest version,
                           the default).
        :type generation: long
        :param if_generation_match: Makes the operation conditional on whether
                                    the object's current generation matches the
                                    given value.
        :type if_generation_match: long
        :param if_generation_not_match: Makes the operation conditional on
                                        whether the object's current generation
                                        does not match the given value.
        :type if_generation_not_match: long
        :param if_metageneration_match: Makes the operation conditional on
                                        whether the object's current
                                        metageneration matches the given value.
        :type if_metageneration_match: long
        :param if_metageneration_not_match: Makes the operation conditional on
                                            whether the object's current
                                            metageneration does not match the
                                            given value.
        :type if_metageneration_not_match: long
        :returns: None
        """
        self._request(op='DELETE', ok=(requests.codes.no_content,),
                      generation=generation or self.generation,
                      ifGenerationMatch=if_generation_match,
                      ifGenerationNotMatch=if_generation_not_match,
                      ifMetagenerationMatch=if_metageneration_match,
                      ifMetagenerationNotMatch=if_metageneration_not_match)

    @common.is_complete
    def open(self, mode='r', chunksize=None):
        """Open this object.

        :param mode: Mode to open the file with, 'r' for read and 'w' for
                     writing are only supported formats.  Default is 'r' if
                     this argument is not provided.
        :type mode: String
        :param chunksize: Size in bytes of the payload to send/receive to/from
                          GCS.  Default chunksize is the one defined on
                          object's initialization.
        :type chunksize: int
        """
        return GCSObjFile(self.bucket, self.name, self._credentials, mode,
                          chunksize or self._chunksize, self.retry_params,
                          self.generation)

    def __str__(self):
        return '%s/%s' % (self.bucket, self.name)

    def __repr__(self):
        return ("%s.%s('%s', '%s', '%s') #etag: %s" % (self.__module__,
                self.__class__.__name__, self.bucket, self.name,
                self.generation, getattr(self, 'etag', '?')))


class GCSObjFile(object):
    """Reader/Writer for GCS Objects.

    Supports basic functionality:
        - Read
        - Write
        - Close
        - Seek
        - Tell

    Instances support context manager behavior.
    """
    _URL = base.Fillable._URL + '/%s/o/%s'
    _URL_UPLOAD = base.Fillable._URL + '/%s/o'

    def __init__(self, bucket, name, credentials, mode='r', chunksize=None,
                 retry_params=None, generation=None):
        """Initialize reader/writer of GCS object.

        On initialization connection to GCS will be tested.  For reading it'll
        confirm the existence of the object in the bucket and for writing it'll
        create the object (it won't send any content).

        :param bucket: Name of the bucket to use.
        :type bucket: String
        :param name: Name of the object.
        :type name: String
        :param credentials: A credentials object to authorize the connection.
        :type credentials: gcs_client.Credentials
        :param mode: Mode to open the file with, 'r' for read and 'w' for
                     writing are only supported formats.  Default is 'r' if
                     this argument is not provided.
        :type mode: String
        :param chunksize: Size in bytes of the payload to send/receive to/from
                          GCS.  Default is gcs_client.DEFAULT_BLOCK_SIZE
        :type chunksize: int
        :param retry_params: Retry configuration used for communications with
                             GCS.  If None is passed default retries will be
                             used.
        :type retry_params: RetryParams or NoneType
        :param generation: If present, selects a specific revision of this
                           object (as opposed to the latest version, the
                           default).
        :type generation: long
        """
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
        self.closed = True
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
            self._location = self._URL % (safe_bucket, safe_name)
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
            initial_url = self._URL_UPLOAD % safe_bucket
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
        """Return file's current position from the beginning of the file."""
        self._check_is_open()
        return self._offset

    def seek(self, offset, whence=os.SEEK_SET):
        """Set the file's current position, like stdio's fseek().

        Note that only files open for reading are seekable.

        :param offset: Offset to move the file cursor.
        :type offset: int
        :param whence: How to interpret the offset, defaults to os.SEEK_SET (0)
                       -absolute file positioning- other values are os.SEEK_CUR
                       (1) -seek relative to the current position- and
                       os.SEEK_END (2) -seek relative to the file's end-.
        :type whence: int
        :returns: None
        """
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
        """Write a string to the file.

        Due to buffering, the string may not actually show up in the file until
        we close the file or enough data to send another chunk has been
        buffered.

        :param data: Data to write to the object.
        :type data: String
        :returns: None
        """
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
        """Close the file.

        A closed file cannot be read or written any more. Any operation which
        requires that the file be open will raise an error after the file has
        been closed. Calling close() more than once is allowed.
        """
        if not self.closed:
            if self._is_writable():
                self._send_data(self._buffer.read(), self._gcs_offset,
                                finalize=True)
            self.closed = True

    def read(self, size=None):
        """Read data from the file.

        Read at most size bytes from the file (less if the read hits EOF before
        obtaining size bytes).  If the size argument is None, read all data
        until EOF is reached.

        The bytes are returned as a bytes object. An empty string is returned
        when EOF is encountered immediately.

        Note that this method may make multiple requests to GCS service in an
        effort to acquire as close to size bytes as possible.

        :param size: Number of bytes to read.
        :type size: int
        :returns: Bytes with read data from GCS.
        :rtype: bytes
        """
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
