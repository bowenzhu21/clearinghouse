import unittest

from clearinghouse.models import ValidationError
from clearinghouse.reconcile import FIELDS, reconcile


class ReconciliationTest(unittest.TestCase):
    def test_all_categories_and_signed_refunds(self):
        expected = {key: {"event_id": key, "amount_cents": amount, "currency": "USD"} for key, amount in [("exact", -100), ("duplicate", 200), ("missing", 300), ("amount", 400), ("currency", 500)]}
        csv = ",".join(FIELDS) + "\ns1,exact,-100,USD\ns2,duplicate,200,USD\ns3,duplicate,200,USD\ns4,amount,401,USD\ns5,currency,500,EUR\ns6,extra,1,USD\n"
        result = reconcile(csv, expected)
        self.assertEqual({"exact": 1, "duplicate": 1, "missing": 1, "amount_mismatch": 1, "currency_mismatch": 1, "unexpected": 1}, result["counts"])
        self.assertEqual(6, result["provider_row_count"])

    def test_duplicate_settlement_id_marks_both_events(self):
        csv = ",".join(FIELDS) + "\nsame,a,1,USD\nsame,b,2,USD\n"
        result = reconcile(csv, {})
        self.assertEqual(2, result["counts"]["duplicate"])

    def test_strict_csv_contract(self):
        for csv in ("x,y\n", ",".join(FIELDS) + "\ns1,a,1.0,USD\n", ",".join(FIELDS) + "\ns1,a,100\n", ",".join(FIELDS) + "\ns1,a,1,USD,extra\n", ",".join(FIELDS) + "\ns1,a,1,JPY\n"):
            with self.subTest(csv=csv), self.assertRaises(ValidationError):
                reconcile(csv, {})

    def test_malformed_quotes_are_not_silently_accepted(self):
        for row in ('s1,a,1,"USD', '"s1"junk,a,1,USD'):
            with self.subTest(row=row), self.assertRaises(ValidationError):
                reconcile(",".join(FIELDS) + "\n" + row, {})

    def test_malformed_header_and_oversized_integer_are_validation_errors(self):
        for text in ('"settlement_id,event_id', ",".join(FIELDS) + "\ns1,a," + "9" * 5000 + ",USD\n"):
            with self.subTest(text=text[:30]), self.assertRaises(ValidationError):
                reconcile(text, {})


if __name__ == "__main__":
    unittest.main()
