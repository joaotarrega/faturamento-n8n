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


class ProcessRuleRequiredFieldsArtifactTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.parent = load_workflow("FIN - Gerar faturas mensais*.json")
        cls.child = load_workflow("FIN - Processar regra faturamento mensal*.json")

    def test_child_defaults_required_usd_base_fields_to_zero(self) -> None:
        code = get_node(
            self.child, "02.5 | Consolidar templates e elegibilidade"
        )["parameters"]["jsCode"]

        for field_id in [
            "valor_base_saas_categoria_a_b_usd",
            "valor_base_saas_categoria_e_s_usd",
            "valor_base_saas_categoria_espa_os_usd",
            "valor_base_saas_categoria_hospedagem_usd",
            "valor_base_saas_categoria_outros_usd",
        ]:
            self.assertIn(field_id, code)

        self.assertIn("const requiredUsdBaseFields = [", code)
        self.assertIn("upsertFieldValue(fa, fid, 0);", code)

    def test_child_throws_required_field_sentinel(self) -> None:
        code = get_node(
            self.child, "03.5 | Consolidar criação de itens"
        )["parameters"]["jsCode"]

        self.assertIn("create_item_required_field_error", code)
        self.assertIn("FIN_CREATE_ITEM_REQUIRED_FIELD_ERROR::", code)
        self.assertIn(
            "throw new Error(REQUIRED_FIELD_ERROR_SENTINEL + String(requiredFieldErrorMessages[0]",
            code,
        )

    def test_child_exits_after_create_stage_when_terminal_is_done(self) -> None:
        get_node(self.child, "03.6 | Encerrar após criação de itens?")

        self.assertEqual(
            node_targets(self.child, "03.5 | Consolidar criação de itens"),
            ["03.6 | Encerrar após criação de itens?"],
        )
        self.assertEqual(
            node_targets(self.child, "03.6 | Encerrar após criação de itens?", 0),
            ["08.1 | Finalizar rule_result"],
        )
        self.assertEqual(
            node_targets(self.child, "03.6 | Encerrar após criação de itens?", 1),
            ["04.1 | Há parcelas para atualizar?"],
        )

    def test_parent_routes_child_success_and_error_outputs_through_merge(self) -> None:
        process_node = get_node(self.parent, "Processar regra")
        merge_name = "Merge retorno processar regra"

        self.assertEqual(process_node.get("onError"), "continueErrorOutput")
        self.assertEqual(
            node_targets(self.parent, "Loop: É rule_input?", 0),
            ["Processar regra", merge_name],
        )
        self.assertEqual(node_targets(self.parent, "Processar regra", 0), [merge_name])
        self.assertEqual(node_targets(self.parent, "Processar regra", 1), [merge_name])
        self.assertEqual(node_targets(self.parent, merge_name, 0), ["Accumulate results"])

    def test_parent_accumulator_maps_child_runtime_errors_to_failed_rules(self) -> None:
        code = get_node(self.parent, "Accumulate results")["parameters"]["jsCode"]

        self.assertIn("childWorkflowErrorSentinel", code)
        self.assertIn("create_item_required_field_error", code)
        self.assertIn("child_workflow_runtime_error", code)
        self.assertIn("itemType==='rule_input' && childErrorText", code)
        self.assertIn("node:'Processar regra'", code)


if __name__ == "__main__":
    unittest.main()
