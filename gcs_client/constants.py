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

# ACLs

#: Project team/Object owners get OWNER access, and allAuthenticatedUsers get
#: READER access.
ACL_AUTH_READ = 'authenticatedRead'

#: Object owner gets OWNER access, and project team owners get OWNER access.
ACL_OWNER_FULL = 'bucketOwnerFullControl'

#: Object owner gets OWNER access, and project team owners get READER access.
ACL_OWNER_READ = 'bucketOwnerRead'

#: Project team/Object owners get OWNER access.
ACL_PRIVATE = 'private'

#: Object owner gets OWNER access, and project team members get access
#: according to their roles.
ACL_PROJECT_PRIVATE = 'projectPrivate'

#: Project team owners get OWNER access, and allUsers get READER access.
ACL_PUBLIC_R = 'publicRead'

#: Project team/Object owners get OWNER access, and allUsers get WRITER access.
ACL_PUBLIC_RW = 'publicRead'


# PROJECTIONS

#: Full projection: Include all properties....
PROJECTION_FULL = 'full'

#: Simple projection: Exclude the ACL property.
PROJECTION_SIMPLE = 'noAcl'


# CREDENTIAL SCOPES

#: Reader permissions lets a user download an object's data and list a bucket's
#: contents.
SCOPE_READER = 'READER'

#: Writer permissions: For buckets lets a user list, create, overwrite, and
#: delete objects in a bucket.  You cannot apply this permission to objects.
SCOPE_WRITER = 'WRITER'

#: Owner permissions: For buckets gives a user READER and WRITER permissions on
#: the bucket.  It also lets a user read and write bucket metadata, including
#: ACLs.  For objects, gives a user READER access.  It also lets a
#: user read and write object metadata, including ACLs.
SCOPE_OWNER = 'OWNER'

#: Cloud permissions: For buckets, they have the predefined project-private ACL
#: applied when they are created. Buckets are always owned by the
#: project-owners group.  For objects, they have the predefined project-private
#: ACL applied when they are uploaded. Objects are always owned by the original
#: requester who uploaded the object.
SCOPE_CLOUD = 'CLOUD'


# STORAGE CLASSES

#: Standard Storage class:  High availability, low latency (time to first byte
#: is typically tens of milliseconds).  Use cases: Storing data that requires
#: low latency access or data that is frequently accessed ("hot" objects), such
#: as serving website content, interactive workloads, or gaming and mobile
#: applications.
STORAGE_STANDARD = 'STANDARD'

#: Cloud Storage Nearline class: Slightly lower availability and slightly
#: higher latency (time to first byte is typically 2 - 5 seconds) than Standard
#: Storage but with a lower cost.  Use cases: Data you do not expect to access
#: frequently (i.e., no more than once per month). Typically this is backup
#: data for disaster recovery, or so called "cold" storage that is archived and
#: may or may not be needed at some future time.
STORAGE_NEARLINE = 'NEARLINE'

#: Durable Reduced Availability (DRA) class: Lower availability than Standard
#: Storage and lower cost per GB stored.  Use cases: Applications that are
#: particularly cost-sensitive, or for which some unavailability is acceptable
#: such as batch jobs and some types of data backup.
STORAGE_DURABLE = 'DURABLE_REDUCED_AVAILABILITY'
