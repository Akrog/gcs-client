========
Usage
========

To use gcs-client in a project you will need to have Credentials to access intended Google Cloud Storage.

Credentials are generated in `Google Developers Console`_ in the `Credentials section`_ of the API Manager of the project. Recommended credentials file is JSON.

Once you have the credentials you can start using gcs_client to access your project.


Listing buckets
---------------

.. code-block:: python

    import gcs_client

    credentials_file = 'private_key.json'
    project_name = 'project_name'

    credentials = gcs_client.Credentials(credentials_file)
    project = gcs_client.Project(project_name, credentials)

    buckets = project.list()
    print 'Buckets:\n\t- ','\n\t- '.join(map(str, buckets))


Creating a bucket
-----------------

.. code-block:: python

    import gcs_client

    credentials_file = 'private_key.json'
    project_name = 'project_name'

    credentials = gcs_client.Credentials(credentials_file)
    project = gcs_client.Project(project_name, credentials)

    bucket = project.create_bucket('my_new_bucket', location='EU')
    print 'Bucket %s is located in %s with storage class %s' % (bucket, bucket.location,
                                                                bucket.storageClass)


Deleting a bucket
-----------------

.. code-block:: python

    import gcs_client

    credentials_file = 'private_key.json'
    project_name = 'project_name'

    credentials = gcs_client.Credentials(credentials_file)
    project = gcs_client.Project(project_name, credentials)

    buckets = project.list()
    # Delete one bucket but never the default bucket
    default_bucket = project.default_bucket_name
    filtered = filter(lambda b: b.name != default_bucket, buckets)
    if filtered:
        buckets[0].delete()


Listing all objects
-------------------

.. code-block:: python

    import gcs_client

    credentials_file = 'private_key.json'
    project_name = 'project_name'

    credentials = gcs_client.Credentials(credentials_file)
    project = gcs_client.Project(project_name, credentials)

    buckets = project.list()
    objects = buckets[0].list()

    print 'Contents of bucket %s:' % bucket
    if objects:
        print '\t','\n\t'.join(map(lambda o: o.name + ' has %s bytes' % o.size, objects))
    else:
        print '\tThere are no objects'


Listing objects with a prefix
-----------------------------

.. code-block:: python

    import gcs_client

    credentials = gcs_client.Credentials('private_key.json')
    bucket = gcs_client.Bucket('bucket_name', credentials)
    directory = 'var/log'
    objects = bucket.list(directory)

    print 'Contents of %s/%s:' % (bucket.name, directory)
    if objects:
        print '\t','\n\t'.join(map(lambda o: o.name + ' has %s bytes' % o.size, objects))
    else:
        print '\tThere are no objects'


List contents of a bucket as a directory
----------------------------------------

We can list a bucket as if it were a directory by passing ``delimiter``
optional argument on the ``list`` call.

Following example implements ``tree`` command for a bucket.

This is just for demonstration purposes, since it is not efficient because for
each "directory" we find we make another request to the server to list its
contents.  It would be more efficient to request all the objects in one go
and then rebuild the tree locally.

.. code-block:: python

    import gcs_client

    def print_obj(obj, i, last):
        if isinstance(obj, gcs_client.Prefix):
            name = obj.prefix.split('/')[-2]
        else:
            name = obj.name.split('/')[-1]
        print ('   ' * i) + ('└──' if last else '├──'), name

    def tree(objs, indent=0):
        if indent == 0:
            print '.'
        for i in range(len(objs)):
            obj = objs[i]
            print_obj(obj, indent, i == len(objs) - 1)
            if isinstance(obj, gcs_client.Prefix):
                tree(obj.list(), indent+1)

    credentials = gcs_client.Credentials('private_key.json')
    bucket = gcs_client.Bucket('bucket_name', credentials)

    print '$ tree', bucket.name
    tree(bucket.list(delimiter='/'))


Deleting objects
----------------

.. code-block:: python

    import gcs_client

    credentials_file = 'private_key.json'
    project_name = 'project_name'

    credentials = gcs_client.Credentials(credentials_file)
    project = gcs_client.Project(project_name, credentials)

    bucket = project.list()[0]
    objects = bucket.list()

    if objects:
        obj = objects[0]
        print 'Deleting object %s' % obj
        obj.delete()


Reading objects
---------------

.. code-block:: python

    import gcs_client

    credentials_file = 'private_key.json'
    project_name = 'project_name'

    credentials = gcs_client.Credentials(credentials_file)
    project = gcs_client.Project(project_name, credentials)

    buckets = project.list()
    objects = buckets[0].list()

    if objects:
        with objects[0].open() as obj:
            print 'Contents of file %s are:\n' % obj.name, obj.read()


Reading objects in big chunks
-----------------------------

.. code-block:: python

    import gcs_client

    credentials = gcs_client.Credentials('private_key.json')
    bucket = gcs_client.Bucket('bucket_name', credentials)

    chunksize = 4 * 1024 * 1024

    with bucket.open('my_file', 'r', chunksize=chunksize) as obj:
        print 'Contents of file %s are:\n' % obj.name, obj.read()

Writing objects
---------------

.. code-block:: python

    import gcs_client

    credentials_file = 'private_key.json'
    project_name = 'project_name'

    credentials = gcs_client.Credentials(credentials_file)
    project = gcs_client.Project(project_name, credentials)

    bucket = project.list()[0]

    with bucket.open('new_file.txt', 'w') as obj:
        obj.write('Hello world\n')

    with bucket.open('new_file.txt') as obj:
        print obj.read()


Changing default retry configuration
------------------------------------

All operations use retries with Truncated Exponential Backoff by default, but we can change default configuration.

.. code-block:: python

    import gcs_client

    # Set default retry configuration using a RetryParams instance
    new_retry_cfg = gcs_client.RetryParams(max_retries=10, initial_delay=0.5, max_backoff=8,
                                           randomize=False)
    gcs_client.RetryParams.set_default(new_retry_cfg)

    # Set default retry configuration via params
    gcs_client.RetryParams.set_default(max_retries=10, initial_delay=0.5, max_backoff=8,
                                       randomize=False)


Disabling default retries
-------------------------

We may want to disable all retries for all instances that are using default retry configuration.  Those that are using specific instance configurations will continue doing so.

.. code-block:: python

    import gcs_client

    # Disable retry configuration
    gcs_client.RetryParams.set_default(0)


Per instance retry configuration
--------------------------------

We can set specific retry configuration for an instance.  Important to notice that listed objects will inherit retry configuration from the object that did the listing.

.. code-block:: python

    import gcs_client

    credentials_file = 'private_key.json'
    project_name = 'project_name'

    credentials = gcs_client.Credentials(credentials_file)
    project = gcs_client.Project(project_name, credentials)

    bucket = project.list()[0]
    # Set bucket retry configuration
    bucket.retry_params = gcs_client.RetryParams(max_retries=10, initial_delay=0.5, max_backoff=8,
                                                 randomize=False)

    # Disable retries on the bucket
    bucket.retry_params = None


.. _Google Developers Console: https://console.developers.google.com
.. _Credentials section: https://console.developers.google.com/apis/credentials
