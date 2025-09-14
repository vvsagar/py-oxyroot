import pytest
import py_oxyroot


def test_sum_as_string():
    assert py_oxyroot.sum_as_string(1, 1) == "2"
