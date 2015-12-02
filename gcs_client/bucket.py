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

import requests

from gcs_client import base
from gcs_client import common
from gcs_client import gcs_object


class Bucket(base.Fillable, base.Listable):
    """GCS Bucket Object representation.

    :ivar kind: The kind of item this is. For buckets, this is always
                storage#bucket.
    :vartype kind: string

    :ivar name: The name of the bucket.
    :vartype name: string

    :ivar timeCreated: The creation time of the bucket in RFC 3339 format.
    :vartype timeCreated: string

    :ivar updated: The modification time of the bucket in RFC 3339 format.
    :vartype updated: string

    :ivar id: The ID of the bucket.
    :vartype id: string

    :ivar metageneration: The metadata generation of this bucket.
    :vartype metageneration: long

    :ivar location: The location of the bucket. Object data for objects in the
                    bucket resides in physical storage within this region.
                    Defaults to US. See the developer's guide for the
                    authoritative list:
                    https://cloud.google.com/storage/docs/bucket-locations
    :vartype location: string

    :ivar owner: The owner of the object.  This will always be the uploader of
                 the object.  Contains entity and entityId keys.
    :vartype owner: dict

    :ivar etag: HTTP 1.1 Entity tag for the object.
    :vartype etag: string

    :ivar projectNumber: The project number of the project the bucket belongs
                         to.
    :vartype projectNumber: long

    :ivar selfLink: The link to this object.
    :vartype selfLink: string

    :ivar storageClass: Storage class of the object.
    :vartype storageClass: string
    """

    kind = 'storage#buckets'
    _required_attributes = base.GCS._required_attributes + ['name']
    _URL = base.Fillable._URL + '/{name}'

    def __init__(self, name=None, credentials=None, retry_params=None):
        """Initialize a Bucket object.

        :param name: Name of the bucket to use.
        :type name: String
        :param credentials: A credentials object to authorize the connection.
        :type credentials: gcs_client.Credentials
        :param retry_params: Retry configuration used for communications with
                             GCS.  If None is passed default retries will be
                             used.
        :type retry_params: RetryParams or NoneType
        """
        super(Bucket, self).__init__(credentials, retry_params)
        self.name = name

    @common.retry
    def _get_data(self):
        r = self._request(parse=True)
        return r.json()

    def list(self, prefix=None, maxResults=None, versions=None, delimiter=None,
             projection=None, pageToken=None):
        """List Objects matching the criteria contained in the Bucket.

        In conjunction with the prefix filter, the use of the delimiter
        parameter allows the list method to operate like a directory listing,
        despite the object namespace being flat.  For example, if delimiter
        were set to "/", then listing objects from a bucket that contains the
        objects "a/b", "a/c", "d", "e", "e/f" would return objects "d" and "e",
        and prefixes "a/" and "e/".

        The authenticated user must have READER permissions on the bucket.

        Object list operations are eventually consistent. This means that if
        you upload an object to a bucket and then immediately perform a list
        operation on the bucket in which the object is stored, the uploaded
        object might not immediately appear in the returned list of objects.
        However, you can always immediately download a newly-created object
        and get its ACLs because object uploads are strongly consistent.

        :param prefix: Filter results to objects whose names begin with this
                       prefix.
        :type prefix: String
        :param maxResults: Maximum number of items plus prefixes to return.
                           As duplicate prefixes are omitted, fewer total
                           results may be returned than requested.  The default
                           value of this parameter is 1,000 items.
        :type maxResults: Unsigned integer
        :param versions: If True, lists all versions of an object as distinct
                         results.  The default is False.
        :type versions: bool
        :param delimiter: Returns results in a directory-like mode.  Objects
                          whose names, aside from the prefix, do not contain
                          delimiter with be returned as Object instances.
                          Objects whose names, aside from the prefix, contain
                          delimiter will be returned as Prefix instances.
                          Duplicate prefixes are omitted.
        :type delimiter: String
        :param projection: Set of properties to return. Defaults to noAcl.
                           Acceptable values are:
                               "full": Include all properties.
                               "noAcl": Omit the acl property.
        :type projection: String
        :param pageToken:  A previously-returned page token representing part
                           of the larger set of results to view.  The pageToken
                           is an encoded field representing the name and
                           generation of the last object in the returned list.
                           In a subsequent request using the pageToken, items
                           that come after the pageToken are shown (up to
                           maxResults).  Object list operations are eventually
                           consistent. In addition, if you start a listing and
                           then create an object in the bucket before using a
                           pageToken to continue listing, you will not see the
                           new object in subsequent listing results if it is in
                           part of the object namespace already listed.
        :type pageToken: String
        :returns: List of objects and prefixes that match the criteria.
        :rtype: List of gcs_client.Object and gcs_client.Prefix.
        """
        return self._list(_list_url=self._URL.format(name=self.name) + '/o',
                          prefix=prefix,
                          maxResults=maxResults, versions=versions,
                          delimiter=delimiter, projection=projection,
                          pageToken=pageToken)

    @common.retry
    def delete(self, if_metageneration_match=None,
               if_metageneration_not_match=None):
        """Permanently deletes an empty bucket from a Project.

        The authenticated user in credentials must be a member of the project's
        team as an editor or owner.

        :param if_metageneration_match: If set, only deletes the bucket if its
                                        metageneration matches this value.
        :type if_metageneration_match: long
        :param if_metageneration_not_match: If set, only deletes the bucket if
                                            its metageneration does not match
                                            this value.
        :type if_metageneration_not_match: long
        :returns: None
        """
        self._request(op='DELETE', ok=(requests.codes.no_content,),
                      ifMetagenerationMatch=if_metageneration_match,
                      ifMetagenerationNotMatch=if_metageneration_not_match)

    def open(self, name, mode='r', generation=None, chunksize=None):
        """Open an object from the Bucket.

        :param name: Name of the file to open.
        :type name: String
        :param mode: Mode to open the file with, 'r' for read and 'w' for
                     writing are only supported formats.  Default is 'r' if
                     this argument is not provided.
        :type mode: String
        :param generation: If present, selects a specific revision of this
                           object (as opposed to the latest version, the
                           default).
        :type generation: long
        :param chunksize: Size in bytes of the payload to send/receive to/from
                          GCS.  Default is gcs_client.DEFAULT_BLOCK_SIZE
        :type chunksize: int
        """
        obj = gcs_object.Object(self.name, name, generation, self.credentials,
                                self.retry_params, chunksize)
        return obj.open(mode)

    def __str__(self):
        return self.name

    def __repr__(self):
        return ("%s.%s('%s') #etag: %s" %
                (self.__module__, self.__class__.__name__, self.name,
                 getattr(self, 'etag', '?')))
