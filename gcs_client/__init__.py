# -*- coding: utf-8 -*-

"""Client Library for Google Cloud Storage."""
from __future__ import absolute_import

from gcs_client.bucket import Bucket  # noqa
from gcs_client.project import Project  # noqa
from gcs_client.credentials import GCSCredential  # noqa
from gcs_client.gcs_object import Object  # noqa
from gcs_client.common import RetryParams  # noqa


__author__ = 'Gorka Eguileor'
__email__ = 'gorka@eguileor.com'
__version__ = '0.0.1'
