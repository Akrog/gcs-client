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
    'oauth2client',
    'requests[security]'
]

test_requirements = [
    'bumpversion==0.5.3',
    'wheel==0.23.0',
    'watchdog==0.8.3',
    'flake8==2.4.1',
    'tox==2.1.1',
    'coverage==4.0',
    'Sphinx==1.3.1',
    'mock==1.3.0'
]


setup(
    name='gcs-client',
    version='0.2.0',
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
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
