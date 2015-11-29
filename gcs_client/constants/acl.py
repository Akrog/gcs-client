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

#: Project team/Object owners get OWNER access, and allAuthenticatedUsers get
#: READER access.
AUTH_READ = 'authenticatedRead'


#: Object owner gets OWNER access, and project team owners get OWNER access.
OWNER_FULL = 'bucketOwnerFullControl'

#: Object owner gets OWNER access, and project team owners get READER access.
OWNER_READ = 'bucketOwnerRead'

#: Project team/Object owners get OWNER access.
PRIVATE = 'private'

#: Object owner gets OWNER access, and project team members get access
#: according to their roles.
PROJECT_PRIVATE = 'projectPrivate'

#: Project team owners get OWNER access, and allUsers get READER access.
PUBLIC_R = 'publicRead'

#: Project team/Object owners get OWNER access, and allUsers get WRITER access.
PUBLIC_RW = 'publicRead'
