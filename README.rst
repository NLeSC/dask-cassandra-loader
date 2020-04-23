.. image:: https://readthedocs.org/projects/dask-cassandra-loader/badge/?version=latest
    :target: https://dask-cassandra-loader.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Build Status

.. image:: https://travis-ci.org/NLeSC/dask-cassandra-loader.svg?branch=master
    :target: https://travis-ci.org/NLeSC/dask-cassandra-loader
    :alt: Build Status

.. image:: https://api.codacy.com/project/badge/Grade/e0685caa122140f582c64c479a5a1da5
    :target: https://www.codacy.com/manual/r.goncalves/dask-cassandra-loader
    :alt: Codacy Grade
    
.. image:: https://api.codacy.com/project/badge/Coverage/e0685caa122140f582c64c479a5a1da5
    :target: https://www.codacy.com/manual/r.goncalves/dask-cassandra-loader
    :alt: Code Coverage
    
.. image:: https://zenodo.org/badge/DOI/10.5281/zenodo.3482935.svg
   :target: https://doi.org/10.5281/zenodo.3482935

################################################################################
dask-cassandra-loader
################################################################################

A data loader which loads data from a Cassandra table into a Dask dataframe. It allows partition elimination, selection and projections pushdown.


The project setup is documented in `a separate document <project_setup.rst>`_. Feel free to remove this document (and/or the link to this document) if you don't need it.

Installation
------------

To install dask-cassandra-loader, do:

.. code-block:: console

  git clone https://github.com/NLeSC/dask-cassandra-loader.git
  cd dask-cassandra-loader
  pip install . -r requirements.txt


Run tests (including coverage) with:

.. code-block:: console

  python setup.py test


Documentation
*************

.. _README:

If you want more information about dask_cassandra_loader API, have a look at the `dask_cassandra_loader documentation <https://dask-cassandra-loader.readthedocs.io/en/latest/?badge=latest>`_.

Contributing
************

If you want to contribute to the development of dask-cassandra-loader,
have a look at the `contribution guidelines <CONTRIBUTING.rst>`_.

License
*******

Copyright (c) 2019, Netherlands eScience Center

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.



Credits
*******

This package was created with `Cookiecutter <https://github.com/audreyr/cookiecutter>`_ and the `NLeSC/python-template <https://github.com/NLeSC/python-template>`_.
