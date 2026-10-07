import json

from ingest.config import REPO_ROOT

DASHBOARD = json.loads(
    (
        REPO_ROOT / "grafana" / "provisioning" / "dashboards" / "patient_pipeline_health.json"
    ).read_text()
)
PANELS = DASHBOARD["panels"]


def test_panel_ids_are_unique():
    ids = [p["id"] for p in PANELS]
    assert len(ids) == len(set(ids))


def test_panels_do_not_overlap():
    cells = set()
    for p in PANELS:
        g = p["gridPos"]
        for x in range(g["x"], g["x"] + g["w"]):
            for y in range(g["y"], g["y"] + g["h"]):
                assert (x, y) not in cells, f"panel {p['id']} overlaps at {(x, y)}"
                cells.add((x, y))


def test_contract_results_have_a_status_and_a_detail_panel():
    contract_panels = {
        p["type"]
        for p in PANELS
        for t in p.get("targets", [])
        if "observability.contract_results" in t.get("rawSql", "")
    }
    assert contract_panels == {"stat", "table"}
