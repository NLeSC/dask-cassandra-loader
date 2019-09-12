#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the dask-cassandra-loader module.
"""
import pytest

from dask-cassandra-loader import dask-cassandra-loader


def test_something():
    assert True


def test_with_error():
    with pytest.raises(ValueError):
        # Do something that raises a ValueError
        raise(ValueError)


# Fixture example
@pytest.fixture
def an_object():
    return {}


def test_dask-cassandra-loader(an_object):
    assert an_object == {}
