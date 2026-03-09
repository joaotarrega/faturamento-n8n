import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_workflow(pattern: str) -> dict:
    matches = sorted(ROOT.glob(pattern))
    if not matches:
        raise AssertionError(f"Workflow not found for pattern: {pattern}")
    return json.loads(matches[0].read_text())


def get_node(workflow: dict, name: str) -> dict:
    for node in workflow["nodes"]:
        if node["name"] == name:
            return node
    raise AssertionError(f"Node not found: {name}")


def node_targets(workflow: dict, node_name: str, output_index: int = 0) -> list[str]:
    main = workflow["connections"].get(node_name, {}).get("main", [])
    if output_index >= len(main):
        return []
    return [item["node"] for item in main[output_index]]


class BillingDayFilterArtifactTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.parent = load_workflow("FIN - Gerar faturas mensais*.json")
        cls.prep = load_workflow("FIN - Prepar*elegi*mensal*.json")

    def test_parent_keeps_same_entrypoint_for_cron_and_manual(self) -> None:
        self.assertEqual(
            node_targets(self.parent, "Cron (dias 1-5 07:00)"),
            ["Init Run"],
        )
        self.assertEqual(
            node_targets(self.parent, "Manual Trigger"),
            ["Init Run"],
        )

    def test_prep_build_normalizes_billing_day_before_comparing(self) -> None:
        code = get_node(self.prep, "Build regras elegíveis")["parameters"]["jsCode"]

        self.assertIn("normalizeBillingDay", code)
        self.assertIn("rawDiaGeracao", code)
        self.assertIn("normalizedDiaGeracao", code)
        self.assertNotIn("if(dia!==today){", code)


if __name__ == "__main__":
    unittest.main()
