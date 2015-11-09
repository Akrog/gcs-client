==========
GCS-Client
==========

.. image:: https://img.shields.io/travis/Akrog/gcs-client.svg
        :target: https://travis-ci.org/Akrog/gcs-client

.. image:: https://img.shields.io/pypi/v/gcs-client.svg
        :target: https://pypi.python.org/pypi/gcs-client

.. image:: https://coveralls.io/repos/Akrog/gcs-client/badge.svg
         :target: https://coveralls.io/github/Akrog/gcs-client

.. image:: https://readthedocs.org/projects/gcs-client/badge/?version=latest
         :target: http://gcs-client.readthedocs.org/en/latest/?badge=latest
         :alt: Documentation Status


Google Cloud Storage Python Client

* Apache 2.0 License
* Documentation: https://gcs-client.readthedocs.org.

The idea is to create a client with similar functionality to `Google's
appengine-gcs-client`_ but intended for applications running from outside
Google's AppEngine.

Cloud Storage documentation can be found at Google_

Features
--------

For now only basic functionality is available:

* Creating buckets
* Deleting buckets
* Listing buckets in a project
* Getting default bucket for a project
* Getting bucket attributes
* Listing objects in a bucket
* Getting objects attributes
* Deleting objects
* Reading object contents
* Writing an object
* Configurable retries with Truncated Exponential Backoff


Usage Example
-------------

To use gcs-client in a project you will need to have Credentials to access intended Google Cloud Storage.

Credentials are generated in `Google Developers Console`_ in the `Credentials section`_ of the API Manager of the project. Recommended credentials file is JSON.

Once you have the credentials you can start using gcs_client to access your project:

.. code-block:: python

    import gcs_client

    credentials = gcs_client.Credentials('private_key.json')
    project = gcs_client.Project('project_name', credentials)

    # Print buckets in the project
    buckets = project.list()
    print 'Buckets:\n\t- ','\n\t- '.join(map(str, buckets))

    # Print some information from first bucket
    bucket = buckets[0]
    print 'Bucket %s is located in %s with storage class %s' % (bucket, bucket.location, bucket.storageClass)

    # List the objects in the bucket
    objects = bucket.list()
    if objects:
        print '\t','\n\t'.join(map(lambda o: o.name + ' has %s bytes' % o.size, objects))
        # Read the contents from the first file
        with objects[0].open() as obj:
            print 'Contents of file %s are:\n' % obj.name, obj.read()
    else:
        print 'There are no objects, nothing to do'

More examples can be found in the documentation, in the Usage section.


.. _Google's appengine-gcs-client: https://github.com/GoogleCloudPlatform/appengine-gcs-client
.. _Google: https://cloud.google.com/storage/docs/overview
.. _Google Developers Console: https://console.developers.google.com
.. _Credentials section: https://console.developers.google.com/apis/credentials
