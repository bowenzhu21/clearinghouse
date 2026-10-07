from pathlib import Path
import json
import tempfile
import unittest

from clearinghouse.benchmark import benchmark
from clearinghouse.demo import run_demo
from clearinghouse.models import ValidationError
from clearinghouse.report import render_report


class DemoTest(unittest.TestCase):
    def test_executable_scenarios_generate_report_and_interactive_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            report = run_demo(temp, benchmark_operations=3)
            self.assertEqual(8, len(report["checks"]))
            self.assertTrue(all(check["passed"] for check in report["checks"]))
            self.assertEqual(9, len(report["ledger"]["journals"]))
            self.assertEqual(18, len(report["ledger"]["journal_lines"]))
            self.assertEqual(10, report["consumer_receipts"])
            html = (Path(temp) / "report.html").read_text()
            self.assertIn('id="journal-drawer"', html)
            self.assertIn('id="event-search"', html)
            self.assertIn('id="settlements"', html)
            self.assertIn("static evidence report", html)
            self.assertEqual(report, json.loads((Path(temp) / "report.json").read_text()))
            # Even audit reasons must not be able to close the embedded data script.
            report["ledger"]["journals"][0]["reason"] = "</script><script>alert(1)</script>"
            safe = render_report(report)
            self.assertNotIn("</script><script>alert(1)</script>", safe)
            self.assertIn("\\u003c/script>", safe)

    def test_benchmark_has_environment_and_validates_counts(self):
        result = benchmark(2)
        self.assertEqual((2, 2, True), (result["operations"], result["journal_count"], result["balanced"]))
        self.assertGreater(result["elapsed_seconds"], 0)
        self.assertEqual({"python", "sqlite", "os", "os_release", "architecture"}, set(result["environment"]))
        for count in (True, 0, -1, 1.5, 100001):
            with self.subTest(count=count), self.assertRaises(ValidationError):
                benchmark(count)


if __name__ == "__main__":
    unittest.main()
