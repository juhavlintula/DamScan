from unittest import TestCase
import unittest
from Daminion.SessionParams import FilterTags
import sys
import io
import os

def create_test_ini_file(name):
    fd = open(name, "w", encoding="utf-8")
    fd.write("[Keywords]\n"
             "domestic|cat\n"
             "domestic|dog:schäfer\n")
    fd.close()

class TestFilterTags(TestCase):

    def test__has_option(self):
        create_test_ini_file("test/test_filter.ini")
        tags = FilterTags("test/test_filter.ini")
        self.assertTrue(tags._has_option("Keywords", "domestic|cat"))
        self.assertTrue(tags._has_option("Keywords", "domestic|cat|siamese"))
        self.assertTrue(tags._has_option("Keywords", "domestic|dog:schäfer"))
        self.assertFalse(tags._has_option("Keywords", "domestic|cow"))
        self.assertFalse(tags._has_option("Keywords", "wild|lion"))
        self.assertFalse(tags._has_option("Keywords", ""))
        self.assertFalse(tags._has_option("People", "Lintula"))

    def test__has_no_option(self):
        create_test_ini_file("test/test_filter.ini")
        tags = FilterTags("test/test_filter.ini")
        self.assertEqual(tags._has_no_option("Keywords", "domestic|cat"),
                         not tags._has_option("Keywords", "domestic|cat"))

    def test__init(self):
        tags = FilterTags(None, True)
        self.assertEqual(tags.has_option, tags._has_option)

        fd = io.StringIO()
        pos = 0
        tmp = sys.stderr
        sys.stderr = fd

        if os.path.exists("test/non-existent"):
            os.remove("test/non-existent")
        tags = FilterTags("test/non-existent", True)
        fd.seek(pos)
        str = fd.readline()
        pos += len(str)
        self.assertEqual(tags.has_option, tags._has_option)
        self.assertEqual(str, "File test/non-existent specified with -x|-y doesn't exist. Option ignored.\n")

        tags = FilterTags("test/test.ini", False)
        self.assertEqual(tags.has_option, tags._has_option)

        tags = FilterTags("test/test.ini", True)
        self.assertEqual(tags.has_option, tags._has_no_option)

        fd.close()
        sys.stderr = tmp



