from unittest import TestCase
from Daminion.SessionParams import SessionParams
import io
import sys


def create_test_ini_file(name):
    fd = open(name, "w", encoding="utf-8")
    fd.write("n1 (1)\t<>\tn2 (2)\tPlace\tFinland\n"
             "n1 (1)\t<>\tn2 (2)\tPlace\tSweden\n"
             "n3 (3)\t<>\tn4 (4)\tPlace\tSweden\n"
             "n3 (3)\t<>\tn4 (4)\tPeople\t'A', 'B'\n"
             "n3 (3)\t<>\tn4 (4)\tPeople\t'C', 'D'\n")
    fd.close()

class TestSessionParams(TestCase):

    def test__get_item_id(self):
        self.assertEqual(SessionParams._get_item_id("name (123)"), 123)
        self.assertEqual(SessionParams._get_item_id("(123)"), None)
        self.assertEqual(SessionParams._get_item_id("name"), None)
        self.assertEqual(SessionParams._get_item_id(""), None)

    def test_parse_line(self):

        fd = io.StringIO()
        pos = 0
        tmp = sys.stderr
        sys.stderr = fd

        self.assertEqual(SessionParams.parse_line(""), [])
        self.assertCountEqual(SessionParams.parse_line("n1 (1)\t<>\tn2 (2)\tPlace\tTampere"),
                              ["Place", 1, 2, []])
        self.assertCountEqual(SessionParams.parse_line("n1 (1)\t<\tn2 (2)\tPeople\t'A', 'B', 'C'"),
                              ["People", 1, 2, ["A", "B", "C"]])

        line = "n1\t<>\tn2\tPlace\tTampere"
        p = SessionParams.parse_line(line)
        self.assertEqual(p, [])
        fd.seek(pos)
        str = fd.readline()
        pos += len(str)
        self.assertEqual(str, "*Warning: No item IDs – ignored: " + line + "\n")

        line = "n1 (1)\t<>\tn2 (2)"
        p = SessionParams.parse_line(line)
        self.assertEqual(p, [])
        fd.seek(pos)
        str = fd.readline()
        pos += len(str)
        self.assertEqual(str, "*Warning: Invalid line – ignored: " + line + "\n")

        line = "n1 (1)\t<\tn2 (2)\tPeople"
        p = SessionParams.parse_line(line)
        self.assertEqual(p, [])
        fd.seek(pos)
        str = fd.readline()
        pos += len(str)
        self.assertEqual(str, "*Ignored:" + line + "\n")

        fd.close()
        sys.stderr = tmp

    def test_read_pairs(self):
        pairs = { "Place": { 1: { 2: [] } , 3: { 4: [] } },
                  "People": { 3: { 4: [ 'A', 'B', 'C', 'D'] } } }

        create_test_ini_file("test/test_pairs.ini")
        p = SessionParams.read_pairs("test/test_pairs.ini")
        self.assertCountEqual(p, pairs)

        fd = io.StringIO()
        pos = 0
        tmp = sys.stderr
        sys.stderr = fd
        p = SessionParams.read_pairs("test/non-existent")
        self.assertEqual(p, {})
        fd.seek(pos)
        str = fd.readline()
        pos += len(str)
        self.assertEqual(str, "test/non-existent" + " doesn't exist. Option -a ignored.\n")

        fd.close()
        sys.stderr = tmp

