import time
import unittest

# python -m unittest tests/test_kerground.py


class TestKerground(unittest.TestCase):
    def test_kerground(self):

        ker = Kerground()

        @ker.task
        def some_procesing_func():
            return "something"

        assert some_procesing_func() == "something"
        assert "some_procesing_func" in ker.tasks
        assert ker.delay("some_procesing_func") == "something"
