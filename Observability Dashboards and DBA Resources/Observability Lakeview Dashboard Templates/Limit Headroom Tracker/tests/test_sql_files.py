"""Checks on the SQL files and runner:  python3 -m unittest discover -s tests"""
import os
import re
import sys
import unittest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
from lt_sql import load_sql  # noqa: E402

SQL_DIR = os.path.join(HERE, "..", "sql")
SQL_FILES = sorted(f for f in os.listdir(SQL_DIR) if f.endswith(".sql"))


def code_lines(text):
    return [l for l in text.splitlines() if l.strip() and not l.strip().startswith("--")]


class SqlFileTest(unittest.TestCase):
    def test_each_file_is_one_scripting_block(self):
        for name in SQL_FILES:
            lines = code_lines(load_sql(os.path.join(SQL_DIR, name)))
            self.assertEqual(lines[0].strip(), "BEGIN", name)
            self.assertEqual(lines[-1].strip(), "END", name)

    def test_names_go_through_quoted_prefix(self):
        # :catalog / :schema may only appear inside the backtick-escaping replace() calls.
        for name in SQL_FILES:
            text = "\n".join(code_lines(load_sql(os.path.join(SQL_DIR, name))))
            stripped = text.replace("replace(:catalog, '`', '``')", "").replace("replace(:schema, '`', '``')", "")
            self.assertNotRegex(stripped, r":(catalog|schema)\b", name)
            self.assertIn("IDENTIFIER(v_prefix ||", text, name)

    def test_seed_rows_have_public_sources_only(self):
        text = open(os.path.join(SQL_DIR, "02_seed_limits_reference.sql")).read()
        urls = set(re.findall(r"https?://[^\s']+", text))
        for u in urls:
            self.assertRegex(u, r"^https://(docs\.databricks\.com|learn\.microsoft\.com)/", u)

    def test_scanned_ids_match_scan_and_seed(self):
        scan = open(os.path.join(SQL_DIR, "03_scan_uc.sql")).read()
        seed = open(os.path.join(SQL_DIR, "02_seed_limits_reference.sql")).read()
        declared = re.search(r"v_scanned_ids ARRAY<STRING> DEFAULT array\((.*?)\);", scan, re.S).group(1)
        declared = set(re.findall(r"'([a-z_]+)'", declared))
        emitted = set(re.findall(r"SELECT '([a-z_]+)'", scan))
        seeded = set(re.findall(r"^\s*\('([a-z_]+)',", seed, re.M))
        self.assertEqual(declared, emitted, "v_scanned_ids out of sync with the usage CTE")
        self.assertLessEqual(emitted, seeded, f"scan emits ids with no seed row: {emitted - seeded}")
        info = set(re.findall(r"^\s*\('([a-z_]+)',[^\n]*'information_schema'", seed, re.M))
        self.assertEqual(info, emitted, "seed rows marked information_schema should be exactly what the scan measures")

    def test_rest_scan_ids_match_seed(self):
        scan = open(os.path.join(HERE, "..", "scripts", "scan_rest.py")).read()
        seed = open(os.path.join(SQL_DIR, "02_seed_limits_reference.sql")).read()
        emitted = set(re.findall(r'(?:one|top)\("([a-z_]+)"', scan))
        rest = set(re.findall(r"^\s*\('([a-z_]+)',[^\n]*'rest_api'", seed, re.M))
        self.assertEqual(emitted, rest, "scan_rest.py ids should be exactly the seed rows marked rest_api")

    def test_every_seed_row_has_a_valid_level(self):
        seed = open(os.path.join(SQL_DIR, "02_seed_limits_reference.sql")).read()
        rows = re.findall(r"^\s*\('([a-z_]+)',\s*'([a-z]+)',", seed, re.M)
        self.assertEqual(len(rows), len(re.findall(r"^\s*\('[a-z_]+',", seed, re.M)))
        for lid, level in rows:
            self.assertIn(level, {"account", "region", "metastore", "workspace"}, lid)


if __name__ == "__main__":
    unittest.main()
