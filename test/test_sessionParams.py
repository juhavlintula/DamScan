from unittest import TestCase
from Daminion.SessionParams import SessionParams, FilterTags
import io
import sys
from unittest.mock import patch


def create_test_ini_file(name):
    fd = open(name, "w", encoding="utf-8")
    fd.write("n1 (1)\t<>\tn2 (2)\tPlace\tFinland\n"
             "n1 (1)\t<>\tn2 (2)\tPlace\tSweden\n"
             "n3 (3)\t<>\tn4 (4)\tPlace\tSweden\n"
             "n3 (3)\t<>\tn4 (4)\tPeople\t'A', 'B'\n")
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
        self.assertCountEqual(SessionParams.parse_line(("n1 (1)\t<>\tn2 (2)\tPlace\tTampere")),
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
        p = SessionParams.read_pairs(None)
        self.assertEqual(p, {})

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


    @patch('Daminion.SessionParams.FilterTags')
    @patch('Daminion.SessionParams.SessionParams.read_pairs')
    def test__init(self, m_pairs, m_list):
        tag_cat_list = ['tagcat_a', 'tagcat_b']
        ex_dir = ['2011', '2012']
        only_dir = ['2013, 2014']

        s = SessionParams(tag_cat_list, True, True, True, ['_'], True, "tagvaluefile", "pairsfile",
                          1.0, 1.0, ex_dir, only_dir, sys.stderr)
        self.assertCountEqual(s.tag_cat_list, tag_cat_list)
        self.assertTrue(s.fullpath)
        self.assertTrue(s.print_id)
        self.assertTrue(s.group)
        self.assertEqual(s.comp_name, ['_'])
        m_pairs.assert_called_once_with("pairsfile")
        m_list.assert_called_once_with("tagvaluefile", True)
        self.assertEqual(s.dist_tolerance, 1.0)
        self.assertEqual(s.alt_tolerance, 1.0)
        self.assertCountEqual(s.exdir, ex_dir)
        self.assertCountEqual(s.onlydir, only_dir)
        self.assertEqual(s.outfile, sys.stderr)

        s = SessionParams(exdir=None, onlydir=None)
        self.assertEqual(s.exdir, [])
        self.assertEqual(s.onlydir, [])