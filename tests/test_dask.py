#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the dask_cassandra_loader module.
"""
import pytest
from dask.distributed import Client

@pytest.fixture
def test_dask_connection():
    client = Client(processes=False)

    def square(x):
        return x ** 2

    def neg(x):
        return -x

    # Run a computation on Dask
    a = client.map(square, range(10))
    b = client.map(neg, a)
    total = client.submit(sum, b)
    result = client.gather(total)

    if result != -285:
        raise AssertionError()

    return True
