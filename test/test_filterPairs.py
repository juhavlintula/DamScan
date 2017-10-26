from unittest import TestCase
from Daminion.SessionParams import FilterPairs


class TestFilterPairs(TestCase):
    def test__nested_set(self):
        pairs = FilterPairs()
        pairs._nested_set(["a", "b", "c"], [4])
        self.assertEqual(pairs["a"]["b"]["c"], [4])
        with self.assertRaises(KeyError):
            i = pairs["d"]["e"]["f"]

    def test_nested_set(self):
        pairs = FilterPairs()
        pairs.nested_set(["a", "b", "c"], [4], False)
        self.assertEqual(pairs["a"]["b"]["c"], [4])
        with self.assertRaises(KeyError):
            i = pairs["d"]["e"]["f"]
        pairs.nested_set(["a", "b", "c"], [5, 6], False)
        self.assertCountEqual(pairs["a"]["b"]["c"], [5, 6])
        pairs.nested_set(["d", "e", "f"], [6, 7], True)
        self.assertCountEqual(pairs["d"]["e"]["f"],[6, 7])
        pairs.nested_set(["d", "e", "f"], [8, 9], True)
        self.assertEqual(pairs["d"]["e"]["f"],[6, 7, 8, 9])

    def test___contains__(self):
        pairs = FilterPairs()
        pairs.nested_set(["Name", 1, 2], [], True)
        pairs.nested_set(["People", 1, 2], ["a", "b"], True)
        self.assertTrue(("Name", 1, 2) in pairs)
        self.assertFalse(("Name", 1, 3) in pairs)
        self.assertTrue(("People", 1, 2, "a") in pairs)
        self.assertFalse(("People", 1, 2, "c") in pairs)
