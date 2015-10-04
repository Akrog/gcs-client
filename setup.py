#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

requirements = [
    # TODO: put package requirements here
]

test_requirements = [
    # TODO: put package test requirements here
]

import gcs_client

setup(
    name='gcs-client',
    version=gcs_client.__version__,
    description="Google Cloud Storage Python client",
    long_description=readme + '\n\n' + history,
    author="Gorka Eguileor",
    author_email='gorka@eguileor.com',
    url='https://github.com/Akrog/gcs-client',
    packages=[
        'gcs_client',
    ],
    package_dir={'gcs_client': 'gcs_client', },
    include_package_data=True,
    install_requires=requirements,
    license="Apache License 2.0",
    zip_safe=False,
    keywords='gcs-client',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
