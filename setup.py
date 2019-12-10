#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

# To update the package version number, edit dask-cassandra-loader/__version__.py

with open('README.rst') as readme_file:
    readme = readme_file.read()

setup(
    name='dask_cassandra_loader',
    version='1.0.0',
    description="""A data loader which loads data from a Cassandra table into a Dask dataframe. It allows partition elimination,
        selection and projections pushdown.""",
    long_description=readme + '\n\n',
    author="Romulo Goncalves",
    author_email='r.goncalves@esciencecenter.nl',
    url='https://github.com/NLeSC/dask-cassandra-loader',
    packages=[
        'dask_cassandra_loader',
    ],
    include_package_data=True,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='dask_cassandra_loader',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    test_suite='tests',
    install_requires=[
        'cassandra-driver',
        'dask',
        'distributed',
        'fsspec',
        'pandas',
        'sqlalchemy',
    ],
    setup_requires=[
        # dependency for `python setup.py test`
        'pytest-runner',
        # dependencies for `python setup.py build_sphinx`
        'sphinx<2',
        'sphinx-rtd-theme',
        'recommonmark'
    ],
    tests_require=[
        'coverage',
        'pytest',
        'pytest-cov',
        'pycodestyle',
    ],
    extras_require={
        'dev':  ['prospector[with_pyroma]', 'isort'],
    }
)
