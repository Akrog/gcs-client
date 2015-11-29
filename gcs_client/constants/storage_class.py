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


#: Standard Storage:  High availability, low latency (time to first byte is
#: typically tens of milliseconds).  Use cases: Storing data that requires low
#: latency access or data that is frequently accessed ("hot" objects), such as
#: serving website content, interactive workloads, or gaming and mobile
#: applications.
STANDARD = 'STANDARD'

#: Cloud Storage Nearline: Slightly lower availability and slightly higher
#: latency (time to first byte is typically 2 - 5 seconds) than Standard
#: Storage but with a lower cost.  Use cases: Data you do not expect to access
#: frequently (i.e., no more than once per month). Typically this is backup
#: data for disaster recovery, or so called "cold" storage that is archived and
#: may or may not be needed at some future time.
NEARLINE = 'NEARLINE'

#: Durable Reduced Availability (DRA): Lower availability than Standard Storage
#: and lower cost per GB stored.  Use cases: Applications that are particularly
#: cost-sensitive, or for which some unavailability is acceptable such as batch
#: jobs and some types of data backup.
DURABLE = 'DURABLE_REDUCED_AVAILABILITY'
