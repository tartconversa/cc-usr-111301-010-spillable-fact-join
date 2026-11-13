"""金额多对多扩张演示测试：报告扩张，不平摊。"""

from __future__ import annotations

import unittest

from src.amount_demo import amount_expansion_report
from tests.helpers import make_hot_key_rows


class AmountExpansionTests(unittest.TestCase):
    def test_many_to_many_amount_is_replicated_not_prorated(self):
        left = make_hot_key_rows("l", "x", 2, value_base=10)
        right = make_hot_key_rows("r", "x", 3, value_base=0)
        # 右行 value 为 1,2,3，原始金额合计 6；左重复 2 => 联接后朴素求和 12。
        report = amount_expansion_report(left, right)
        key_report = report["per_key"][0]
        self.assertEqual(key_report["cardinality"], "many_to_many")
        self.assertEqual(key_report["right_amount_raw"], 6)
        self.assertEqual(key_report["right_amount_after_join"], 12)
        self.assertEqual(key_report["expansion_factor"], 2)
        self.assertEqual(report["total_join_rows"], 6)
        self.assertEqual(report["naive_sum_after_join"], 12)
        self.assertEqual(report["global_expansion_factor"], 2.0)
        self.assertIn("禁止", key_report["warning"])
        self.assertIn("不自动平摊", report["proration_policy"])

    def test_null_and_unmatched_keys_excluded_from_amounts(self):
        left = [{"row_id": "l1", "key": None, "value": 10}]
        right = [
            {"row_id": "r1", "key": None, "value": 5},
            {"row_id": "r2", "key": "x", "value": 7},
        ]
        report = amount_expansion_report(left, right)
        self.assertEqual(report["total_join_rows"], 0)
        # 右 NULL 行金额同样不计入原始金额口径。
        self.assertEqual(report["raw_right_amount"], 7)
        self.assertEqual(report["naive_sum_after_join"], 0)

    def test_one_to_one_key_has_no_warning(self):
        left = [{"row_id": "l1", "key": "x", "value": 1}]
        right = [{"row_id": "r1", "key": "x", "value": 9}]
        report = amount_expansion_report(left, right)
        self.assertEqual(report["per_key"][0]["cardinality"], "one_to_one")
        self.assertIsNone(report["per_key"][0]["warning"])
        self.assertEqual(report["per_key"][0]["right_amount_after_join"], 9)


if __name__ == "__main__":
    unittest.main()
