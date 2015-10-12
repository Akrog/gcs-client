# -*- coding: utf-8 -*-
from __future__ import absolute_import

import io

from apiclient import http as gcs_http

from gcs_client import common


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

    def _get_data(self):
        req = self._service.objects().get(bucket=self.bucket, object=self.name,
                                          generation=self.generation)
        return req.execute()

    @common.is_complete
    def delete(self, generation=None, if_generation_match=None,
               if_generation_not_match=None, if_metageneration_match=None,
               if_metageneration_not_match=None):

        req = self._service.objects().delete(
            bucket=self.bucket,
            object=self.name,
            ifGenerationMatch=if_generation_match,
            ifGenerationNotMatch=if_generation_not_match,
            ifMetagenerationMatch=if_metageneration_match,
            ifMetagenerationNotMatch=if_metageneration_not_match)
        req.execute()

    @common.is_complete
    def open(self, mode, generation=None):
        return GCSObjFile(mode, self.bucket, self.name, self._service,
                          self.size, generation or self.generation)


class GCSObjFile(object):
    def __init__(self, mode, bucket, name, service, size, generation=None,
                 chunksize=None):
        if mode not in ('r', 'w'):
            raise Exception('Only r or w modes supported')
        self._mode = mode
        self.bucket = bucket
        self.name = name
        self.generation = generation
        self._transferer = None
        self._service = service
        self.size = size
        self._chunksize = chunksize or (1024 * 1024)

        if self._mode == 'r':
            req = self._service.objects().get_media(bucket=self.bucket,
                                                    object=self.name,
                                                    generation=self.generation)
            self._fh = io.BytesIO()
            self._transferer = gcs_http.MediaIoBaseDownload(
                self._fh, req, chunksize=self._chunksize)
            self._status = gcs_http.MediaDownloadProgress(0, int(self.size))
            self._done = False
            self._cache = []
        else:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._mode = None
        self._transferer = None
        self._done = True

    def read(self, size=None):
        if not self._mode:
            raise Exception('File is closed')
        if self._mode == 'w':
            raise Exception('File open for write, cannot read')
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
