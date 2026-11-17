"""Dashboard JSON stays in sync with dashboards/*.sql and uses bare (portable) table names."""
import json
import os
import re
import unittest

D = os.path.join(os.path.dirname(__file__), "..", "dashboards")


class DashboardTest(unittest.TestCase):
    def setUp(self):
        self.dash = json.load(open(os.path.join(D, "limits_headroom.lvdash.json")))

    def test_dataset_sql_matches_files(self):
        for ds in self.dash["datasets"]:
            sql = "".join(ds["queryLines"]).strip()
            self.assertEqual(sql, open(os.path.join(D, ds["name"] + ".sql")).read().strip(), ds["name"])

    def test_bare_table_names(self):
        for ds in self.dash["datasets"]:
            sql = "".join(ds["queryLines"])
            self.assertNotRegex(sql, r"(?i)\b(FROM|JOIN)\s+[\w`]+\.", ds["name"])

    def layout(self):
        return [item for page in self.dash["pages"] for item in page["layout"]]

    def test_widget_fields_bound(self):
        for item in self.layout():
            w = item["widget"]
            for q in w.get("queries", []):
                names = {f["name"] for f in q["query"]["fields"]}
                enc = w["spec"]["encodings"]
                refs = [c["fieldName"] for c in enc.get("columns", [])]
                refs += [enc[k]["fieldName"] for k in ("x", "y", "color", "value") if k in enc]
                refs += [f["fieldName"] for f in enc.get("fields", []) if f.get("queryName") == q["name"]]
                for r in refs:
                    self.assertIn(r, names, f"{w['name']}: {r}")

    def test_grid_has_no_overlaps_or_gaps(self):
        for page in self.dash["pages"]:
            cells = {}
            for item in page["layout"]:
                p = item["position"]
                for y in range(p["y"], p["y"] + p["height"]):
                    for x in range(p["x"], p["x"] + p["width"]):
                        self.assertNotIn((x, y), cells, f"{item['widget']['name']} overlaps {cells.get((x, y))}")
                        cells[(x, y)] = item["widget"]["name"]
            height = max(y for _, y in cells) + 1
            self.assertEqual(len(cells), 12 * height, f"page {page['name']} has gaps")

if __name__ == "__main__":
    unittest.main()
