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

from gcs_client import base


class Prefix(base.Listable):
    """GCS Prefix Object representation.

    Represents prefixes returned from listing objects inside a bucket using a
    delimiter.

    A Prefix object can be thought of as a directory in a filesystem.

    :ivar kind: The kind of item this is. For buckets, this is always
                storage#prefix.
    :vartype kind: string

    :ivar name: Bucket name that this prefix belongs to.
    :vartype name: string

    :ivar prefix: Prefix name (like the full path of a directory).
    :vartype prefix: string

    :ivar delimiter: Delimiter used to generate this prefix.
    :vartype delimiter: string
    """

    kind = 'storage#prefix'
    _URL = base.GCS._URL + '/{name}/o'
    _required_attributes = base.GCS._required_attributes + ['name',
                                                            'prefix']

    def __init__(self, name, prefix, delimiter=None, credentials=None,
                 retry_params=None):
        """Initialize a prefix representation.

        :param name: Name of the bucket this prefix belongs to.
        :type name: String
        :param prefix: Prefix name (like the full path of a directory).
        :type prefix: String
        :param delimiter: Delimiter used on the listing
        :type delimiter: String
        :param credentials: A credentials object to authorize the connection.
        :type credentials: gcs_client.Credentials
        :param retry_params: Retry configuration used for communications with
                             GCS.  If None is passed default retries will be
                             used.
        :type retry_params: RetryParams or NoneType
        """
        super(Prefix, self).__init__(credentials, retry_params)
        self.name = name
        self.prefix = prefix
        self.delimiter = delimiter

    def list(self, prefix='', maxResults=None, versions=None, delimiter=None,
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

        This differs slightly from bucket listing, because if no delimiter is
        provided it will use the delimiter that was used on the listing that
        generated the instance.  Likewise for the prefix.

        :param prefix: Filter results in this 'directory' to objects whose
                       names begin with this prefix.  Default is to list all
                       objects in the directory.
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
                          Duplicate prefixes are omitted.  Default use
                          delimiter that was used to create this prefix.
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
        if delimiter is None:
            delimiter = self.delimiter
        return self._list(prefix=self.prefix + prefix,
                          maxResults=maxResults, versions=versions,
                          delimiter=delimiter,
                          projection=projection,
                          pageToken=pageToken)

    def __str__(self):
        return self.prefix

    def __repr__(self):
        return ("%s.%s('%s', '%s')" % (self.__module__,
                self.__class__.__name__, self.name, self.prefix))
