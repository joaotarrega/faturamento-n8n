#!/usr/bin/env python3
import json
import pathlib
import subprocess
import tempfile
from typing import Optional


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = ROOT / "data" / "raw" / "n8n" / "workflows"

EXPECTED = [
    "[FIN] 1.0 Orquestrar faturamento mensal",
    "[FIN] 1.1 Preparar regras elegiveis",
    "[FIN] 1.2 Preparar itens elegiveis da regra",
    "[FIN] 1.3 Materializar itens FIN-04",
    "[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02",
]

TRIGGER_INPUT_EXAMPLE_TOKENS = {
    "[FIN] 1.1 Preparar regras elegiveis": ["runContext", "trace"],
    "[FIN] 1.2 Preparar itens elegiveis da regra": ["contractVersion", "itemType", "trace", "run", "rule"],
    "[FIN] 1.3 Materializar itens FIN-04": ["contractVersion", "itemType", "trace", "run", "rule", "itemPlan", "failures"],
    "[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02": [
        "contractVersion",
        "itemType",
        "trace",
        "run",
        "rule",
        "itemPlan",
        "createdItemIds",
        "parcelUpdates",
        "failures",
    ],
}

GRAPHQL_EXPECTATIONS = {
    "[FIN] 1.1 Preparar regras elegiveis": {
        "FIN-01: Listar regras Ativo (page)": ["341313813"],
        "FIN-03: Prefetch faturas (competencia)": ["data_de_compet_ncia"],
    },
    "[FIN] 1.2 Preparar itens elegiveis da regra": {
        "FIN-02 Buscar item template": ["card(id:"],
        "FIN-04 Verificar conflitos por competencia": ["id_do_item_usado_como_template_fin_02"],
    },
    "[FIN] 1.3 Materializar itens FIN-04": {
        "FIN-04 Recheck conflito antes de criar": ["id_do_item_usado_como_template_fin_02"],
        "FIN-04 Criar item": ["createCard"],
    },
    "[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02": {
        "FIN-03 Recheck fatura existente": ["id_da_regra_de_faturamento"],
        "FIN-03 Criar fatura": ["createCard"],
        "FIN-04 Vincular id_da_fatura": ["id_da_fatura"],
        "FIN-02 Atualizar parcelas_pagas": ["parcelas_pagas"],
        "FIN-02 Atualizar status": ["status"],
    },
}

PAGINATION_CONTEXT_EXPECTATIONS = {
    "[FIN] 1.1 Preparar regras elegiveis": {
        "Parse rules page": {
            "must_contain": [
                "$(nodeName).item.json",
                "linkedJson('Ha proxima pagina de regras?').ctx",
                "linkedJson('Init Run').ctx",
            ],
            "must_not_contain": [
                "$node['Init Run'].json.ctx",
            ],
        },
        "Parse prefetch": {
            "must_contain": [
                "$(nodeName).item.json",
                "linkedJson('Ha proxima pagina de prefetch?').ctx",
                "linkedJson('Ha proxima pagina de regras?').ctx",
            ],
            "must_not_contain": [
                "$node['Parse rules page'].json.ctx",
            ],
        },
    },
}


def fail(message: str) -> None:
    raise SystemExit(message)


def load_workflow(workflow_name: str) -> dict:
    path = WORKFLOWS_DIR / f"{workflow_name}.json"
    return json.loads(path.read_text())


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def assert_contains(text: str, token: str, message: str) -> None:
    assert_true(token in text, message)


def compile_js(js_code: str, label: str) -> None:
    wrapped = "new Function(" + json.dumps(js_code) + ");\n"
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as tmp:
        tmp.write(wrapped)
        tmp_path = pathlib.Path(tmp.name)
    try:
        proc = subprocess.run(["node", str(tmp_path)], capture_output=True, text=True)
    finally:
        tmp_path.unlink(missing_ok=True)
    if proc.returncode != 0:
        fail(f"js syntax invalid for {label}: {proc.stderr.strip()}")


def compile_expression(expression: str, label: str) -> None:
    wrapped = "new Function(" + json.dumps(f"return ({expression});") + ");\n"
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as tmp:
        tmp.write(wrapped)
        tmp_path = pathlib.Path(tmp.name)
    try:
        proc = subprocess.run(["node", str(tmp_path)], capture_output=True, text=True)
    finally:
        tmp_path.unlink(missing_ok=True)
    if proc.returncode != 0:
        fail(f"expression syntax invalid for {label}: {proc.stderr.strip()}")


def iter_expressions(value, path: str = ""):
    if isinstance(value, dict):
        for key, nested in value.items():
            next_path = f"{path}.{key}" if path else str(key)
            yield from iter_expressions(nested, next_path)
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            next_path = f"{path}[{index}]" if path else f"[{index}]"
            yield from iter_expressions(nested, next_path)
    elif isinstance(value, str) and value.startswith("={{") and value.endswith("}}"):
        yield path or "<root>", value[3:-2]


def validate_expected_exports() -> None:
    for workflow_name in EXPECTED:
        path = WORKFLOWS_DIR / f"{workflow_name}.json"
        assert_true(path.exists(), f"missing workflow export {path.name}")


def validate_orchestrator(workflow: dict) -> None:
    node_names = {node["name"] for node in workflow["nodes"]}
    for required in [
        "Cron (dias 1-5 07:00)",
        "Manual Trigger",
        "Preparar regras elegiveis",
        "Preparar itens elegiveis da regra",
        "Normalizar retorno preparar itens",
        "Materializar itens FIN-04",
        "Normalizar retorno materializacao",
        "Criar fatura FIN-03 e concluir regra",
        "Normalizar retorno finalizacao",
        "Emit rule logs",
        "Ha rule logs?",
        "Split rule logs",
        "Persistir fin_billing_rule_log",
    ]:
        assert_true(required in node_names, f"orchestrator missing node {required}")
    for removed in [
        "Emit workflow run logs",
        "Persistir fin_billing_run_log",
        "Emit entity logs",
        "Ha entity logs?",
        "Persistir fin_billing_entity_log",
        "Emit graphql logs",
        "Ha graphql logs?",
        "Persistir fin_billing_graphql_log",
    ]:
        assert_true(removed not in node_names, f"orchestrator still contains stale node {removed}")
    text = json.dumps(workflow, ensure_ascii=False)
    assert_contains(text, "competence_date", "orchestrator missing competence_date tracking")
    connections = workflow.get("connections", {})

    for node_name in [
        "Preparar itens elegiveis da regra",
        "Materializar itens FIN-04",
        "Criar fatura FIN-03 e concluir regra",
    ]:
        node = get_node(workflow, node_name)
        assert_true(node.get("alwaysOutputData") is True, f"orchestrator {node_name} must use alwaysOutputData")

    def branch_targets(node_name: str, branch_index: int = 0) -> list[str]:
        branches = (connections.get(node_name) or {}).get("main") or []
        branch = branches[branch_index] if len(branches) > branch_index else []
        return [item.get("node") for item in branch]

    if_branches = (connections.get("Loop: item e rule_input?") or {}).get("main") or []
    true_targets = [item.get("node") for item in (if_branches[0] if len(if_branches) > 0 else [])]
    false_targets = [item.get("node") for item in (if_branches[1] if len(if_branches) > 1 else [])]
    assert_true(
        true_targets == ["Preparar itens elegiveis da regra"],
        f"orchestrator Loop: item e rule_input? true branch expected ['Preparar itens elegiveis da regra'] but found {true_targets}",
    )
    assert_true(
        false_targets == ["Accumulate results"],
        f"orchestrator Loop: item e rule_input? false branch expected ['Accumulate results'] but found {false_targets}",
    )
    assert_true(
        branch_targets("Preparar itens elegiveis da regra") == ["Normalizar retorno preparar itens"],
        "orchestrator Preparar itens elegiveis da regra must flow into Normalizar retorno preparar itens",
    )
    assert_true(
        branch_targets("Normalizar retorno preparar itens") == ["Loop: item e item_plan?"],
        "orchestrator Normalizar retorno preparar itens must flow into Loop: item e item_plan?",
    )
    assert_true(
        branch_targets("Loop: item e item_plan?", 0) == ["Materializar itens FIN-04"],
        "orchestrator Loop: item e item_plan? true branch must flow into Materializar itens FIN-04",
    )
    assert_true(
        branch_targets("Loop: item e item_plan?", 1) == ["Accumulate results"],
        "orchestrator Loop: item e item_plan? false branch must flow into Accumulate results",
    )
    assert_true(
        branch_targets("Materializar itens FIN-04") == ["Normalizar retorno materializacao"],
        "orchestrator Materializar itens FIN-04 must flow into Normalizar retorno materializacao",
    )
    assert_true(
        branch_targets("Normalizar retorno materializacao") == ["Loop: item e fin04_items_ready?"],
        "orchestrator Normalizar retorno materializacao must flow into Loop: item e fin04_items_ready?",
    )
    assert_true(
        branch_targets("Loop: item e fin04_items_ready?", 0) == ["Criar fatura FIN-03 e concluir regra"],
        "orchestrator Loop: item e fin04_items_ready? true branch must flow into Criar fatura FIN-03 e concluir regra",
    )
    assert_true(
        branch_targets("Loop: item e fin04_items_ready?", 1) == ["Accumulate results"],
        "orchestrator Loop: item e fin04_items_ready? false branch must flow into Accumulate results",
    )
    assert_true(
        branch_targets("Criar fatura FIN-03 e concluir regra") == ["Normalizar retorno finalizacao"],
        "orchestrator Criar fatura FIN-03 e concluir regra must flow into Normalizar retorno finalizacao",
    )
    assert_true(
        branch_targets("Normalizar retorno finalizacao") == ["Accumulate results"],
        "orchestrator Normalizar retorno finalizacao must flow into Accumulate results",
    )
    assert_true(
        branch_targets("Finalize summary") == ["Emit rule logs"],
        "orchestrator Finalize summary must flow into Emit rule logs",
    )
    assert_true(
        branch_targets("Emit rule logs") == ["Ha rule logs?"],
        "orchestrator Emit rule logs must flow into Ha rule logs?",
    )
    assert_true(
        branch_targets("Ha rule logs?", 0) == ["Split rule logs"],
        "orchestrator Ha rule logs? true branch must flow into Split rule logs",
    )
    assert_true(
        branch_targets("Ha rule logs?", 1) == ["Restaurar summary final"],
        "orchestrator Ha rule logs? false branch must flow into Restaurar summary final",
    )
    assert_true(
        branch_targets("Split rule logs") == ["Persistir fin_billing_rule_log"],
        "orchestrator Split rule logs must flow into Persistir fin_billing_rule_log",
    )
    assert_true(
        branch_targets("Persistir fin_billing_rule_log") == ["Restaurar summary final"],
        "orchestrator Persistir fin_billing_rule_log must flow into Restaurar summary final",
    )
    for node_name in ["Materializar itens FIN-04", "Criar fatura FIN-03 e concluir regra"]:
        params = get_node(workflow, node_name).get("parameters", {}).get("workflowInputs", {})
        schema_ids = {col.get("id") for col in params.get("schema", [])}
        value_keys = set(params.get("value", {}).keys())
        assert_true("logs" not in schema_ids, f"orchestrator {node_name} still exposes logs in workflowInputs schema")
        assert_true("logs" not in value_keys, f"orchestrator {node_name} still maps logs into child workflow inputs")


def get_node(workflow: dict, node_name: str) -> dict:
    for node in workflow["nodes"]:
        if node["name"] == node_name:
            return node
    fail(f"{workflow['name']} missing node {node_name}")


def validate_graphql_nodes(workflow_name: str, workflow: dict) -> None:
    expectations = GRAPHQL_EXPECTATIONS.get(workflow_name, {})
    for node in workflow["nodes"]:
        params = node.get("parameters", {})
        js_code = params.get("jsCode")
        if js_code:
            compile_js(js_code, f"{workflow_name}:{node['name']}")
        if not str(node.get("type") or "").endswith(".graphql"):
            continue
        assert_true(node.get("onError") == "continueErrorOutput", f"{workflow_name}:{node['name']} must use continueErrorOutput")
        assert_true(node.get("retryOnFail") is True, f"{workflow_name}:{node['name']} must retry on fail")
        assert_true(int(node.get("maxTries") or 0) == 5, f"{workflow_name}:{node['name']} must use maxTries=5")
        query = json.dumps(params, ensure_ascii=False)
        for token in expectations.get(node["name"], []):
            assert_contains(query, token, f"{workflow_name}:{node['name']} missing token {token}")


def validate_pagination_context(workflow_name: str, workflow: dict) -> None:
    for node_name, expectations in PAGINATION_CONTEXT_EXPECTATIONS.get(workflow_name, {}).items():
        js_code = get_node(workflow, node_name).get("parameters", {}).get("jsCode", "")
        for token in expectations.get("must_contain", []):
            assert_contains(js_code, token, f"{workflow_name}:{node_name} missing token {token}")
        for token in expectations.get("must_not_contain", []):
            assert_true(token not in js_code, f"{workflow_name}:{node_name} contains stale token {token}")


def assert_merge_append(parameters: dict, workflow_name: str, node_name: str) -> None:
    # n8n 3.2 merge nodes may omit `mode: append` when append is the serialized default.
    mode = parameters.get("mode")
    assert_true(
        mode in (None, "append"),
        f"{workflow_name}:{node_name} must use append mode",
    )


def validate_code_node_return_contracts(workflow_name: str, workflow: dict) -> None:
    if workflow_name != "[FIN] 1.1 Preparar regras elegiveis":
        return
    js_code = get_node(workflow, "Build regras elegiveis").get("parameters", {}).get("jsCode", "")
    assert_contains(
        js_code,
        "...eligibleRules.map((x) => ({ json: x }))",
        f"{workflow_name}:Build regras elegiveis must wrap eligibleRules in json envelopes",
    )
    assert_true(
        "...eligibleRules," not in js_code,
        f"{workflow_name}:Build regras elegiveis contains raw eligibleRules return spread",
    )


def validate_execute_workflow_trigger_examples(workflow_name: str, workflow: dict) -> None:
    tokens = TRIGGER_INPUT_EXAMPLE_TOKENS.get(workflow_name)
    if not tokens:
        return
    json_example = get_node(workflow, "Execute Workflow Trigger").get("parameters", {}).get("jsonExample", "")
    for token in tokens:
        assert_contains(
            json_example,
            f'"{token}"',
            f"{workflow_name}:Execute Workflow Trigger jsonExample missing key {token}",
        )


def validate_expression_syntax(workflow_name: str, workflow: dict) -> None:
    for path, expression in iter_expressions(workflow, workflow_name):
        compile_expression(expression, path)


def validate_contract_tokens(workflow_name: str, workflow: dict) -> None:
    text = json.dumps(workflow, ensure_ascii=False)
    for forbidden in ["parent_ids", "Faturas associadas", "Itens associados"]:
        assert_true(forbidden not in text, f"{workflow_name} contains forbidden token {forbidden}")
    if workflow_name == "[FIN] 1.0 Orquestrar faturamento mensal":
        for token in [
            "Normalizar retorno preparar itens",
            "Normalizar retorno materializacao",
            "Normalizar retorno finalizacao",
            "child_workflow_returned_no_items",
            "failed_prepare_items_stage",
            "failed_materialize_items_stage",
            "ruleLogRows",
            "Persistir fin_billing_rule_log",
            "created_item_ids_json",
            "failures_json",
        ]:
            assert_contains(text, token, f"{workflow_name} missing token {token}")
    if workflow_name == "[FIN] 1.1 Preparar regras elegiveis":
        for token in ["341313813", "itens_da_fatura", "invoice_already_exists"]:
            assert_contains(text, token, f"{workflow_name} missing token {token}")
    if workflow_name == "[FIN] 1.2 Preparar itens elegiveis da regra":
        for token in [
            "341306826",
            "341351580",
            "missing_required_nonzero_field",
            "existing_fin04_conflict",
            "orphan_fin04_exists_for_rule_competence",
            "specific_date_not_ready",
            "specific_date_finished",
            "blocked_missing_external_signal_onboarding_training",
            "blocked_missing_external_signal_setup_payment",
            "Normalizar retorno template",
            "Normalizar retorno conflitos",
            "Template retornou?",
            "Tipo de conflito FIN-04",
            "Motivo de bloqueio de início",
            "Consolidar campos obrigatórios visíveis",
            "Merge requisitos modelo + tipo",
            "Merge requisitos + desconto",
            "Append decisões item_plan",
            "templateFetchError",
            "fin04ConflictError",
        ]:
            assert_contains(text, token, f"{workflow_name} missing token {token}")
    if workflow_name == "[FIN] 1.3 Materializar itens FIN-04":
        for token in ["341351580", "existing_fin04_conflict", "no_items_ready_for_invoice", "Pode criar item FIN-04?"]:
            assert_contains(text, token, f"{workflow_name} missing token {token}")
    if workflow_name == "[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02":
        for token in [
            "itens_da_fatura",
            "id_da_fatura",
            "parcelas_pagas",
            "Inativo",
            "invoice_already_exists",
            "failed_invoice_stage",
            "Pode criar invoice?",
            "Ha parcelas para atualizar?",
            "Ha status para atualizar?",
        ]:
            assert_contains(text, token, f"{workflow_name} missing token {token}")


def validate_item_prep_pipeline(workflow_name: str, workflow: dict) -> None:
    if workflow_name != "[FIN] 1.2 Preparar itens elegiveis da regra":
        return
    node_names = {node["name"] for node in workflow["nodes"]}
    expected_type_versions = {
        "n8n-nodes-base.executeWorkflowTrigger": 1.1,
        "n8n-nodes-base.code": 2,
        "n8n-nodes-base.graphql": 1.1,
        "n8n-nodes-base.if": 2.2,
        "n8n-nodes-base.set": 3.4,
        "n8n-nodes-base.switch": 3.3,
        "n8n-nodes-base.merge": 3.2,
        "n8n-nodes-base.splitOut": 1,
    }
    for node in workflow["nodes"]:
        expected_type_version = expected_type_versions.get(node.get("type"))
        if expected_type_version is None:
            continue
        assert_true(
            node.get("typeVersion") == expected_type_version,
            f"{workflow_name}:{node['name']} must use typeVersion {expected_type_version} for {node['type']}",
        )
    required_types = {
        "Emitir rule_result direto": "n8n-nodes-base.set",
        "Normalizar retorno template": "n8n-nodes-base.set",
        "Normalizar retorno conflitos": "n8n-nodes-base.set",
        "Init contexto item_plan": "n8n-nodes-base.code",
        "Consolidar elegibilidade e planos": "n8n-nodes-base.code",
        "Template retornou?": "n8n-nodes-base.if",
        "Consulta de conflitos retornou?": "n8n-nodes-base.if",
        "Consulta de conflitos paginou?": "n8n-nodes-base.if",
        "Template está Ativo?": "n8n-nodes-base.if",
        "Há FIN-04 na competência?": "n8n-nodes-base.if",
        "Há bloqueio de início?": "n8n-nodes-base.if",
        "Base percentual é SaaS?": "n8n-nodes-base.if",
        "Unidade é Afiliados?": "n8n-nodes-base.if",
        "Há desconto?": "n8n-nodes-base.if",
        "Desconto nominal?": "n8n-nodes-base.if",
        "Desconto percentual?": "n8n-nodes-base.if",
        "Desconto por categorias?": "n8n-nodes-base.if",
        "Tipo de conflito FIN-04": "n8n-nodes-base.switch",
        "Motivo de bloqueio de início": "n8n-nodes-base.switch",
        "Modelo de cobrança": "n8n-nodes-base.switch",
        "Tipo de item": "n8n-nodes-base.switch",
        "Merge requisitos modelo + tipo": "n8n-nodes-base.merge",
        "Merge requisitos + desconto": "n8n-nodes-base.merge",
        "Append guardas iniciais item_plan": "n8n-nodes-base.merge",
        "Append conflitos FIN-04 item_plan": "n8n-nodes-base.merge",
        "Append bloqueios de inicio item_plan": "n8n-nodes-base.merge",
        "Append decisões item_plan": "n8n-nodes-base.merge",
        "Condições de início": "n8n-nodes-base.set",
        "Dimensões comerciais do item": "n8n-nodes-base.set",
        "Consolidar campos obrigatórios visíveis": "n8n-nodes-base.set",
        "Definir fetch_template_item_error": "n8n-nodes-base.set",
        "Definir conflict_fin04_query_error": "n8n-nodes-base.set",
        "Definir conflict_fin04_query_requires_pagination": "n8n-nodes-base.set",
        "Definir inactive_template_phase": "n8n-nodes-base.set",
        "Definir existing_fin04_conflict": "n8n-nodes-base.set",
        "Definir orphan_fin04_exists_for_rule_competence": "n8n-nodes-base.set",
        "Definir specific_date_not_ready": "n8n-nodes-base.set",
        "Definir specific_date_finished": "n8n-nodes-base.set",
        "Definir blocked_missing_external_signal_onboarding_training": "n8n-nodes-base.set",
        "Definir blocked_missing_external_signal_setup_payment": "n8n-nodes-base.set",
    }
    for node_name, expected_type in required_types.items():
        assert_true(node_name in node_names, f"{workflow_name} missing node {node_name}")
        assert_true(
            get_node(workflow, node_name).get("type") == expected_type,
            f"{workflow_name}:{node_name} must be {expected_type}",
        )
    for removed in [
        "Embalar retorno template",
        "Merge retorno template",
        "Embalar retorno conflitos",
        "Merge retorno conflitos",
        "Tipo de desconto",
        "Campos obrigatórios visíveis",
    ]:
        assert_true(removed not in node_names, f"{workflow_name} still contains stale node {removed}")

    merge_model = get_node(workflow, "Merge requisitos modelo + tipo").get("parameters", {})
    assert_true(merge_model.get("mode") == "combine", f"{workflow_name}:Merge requisitos modelo + tipo must use combine mode")
    assert_true(
        merge_model.get("combineBy") == "combineByPosition",
        f"{workflow_name}:Merge requisitos modelo + tipo must combine by position",
    )
    merge_discount = get_node(workflow, "Merge requisitos + desconto").get("parameters", {})
    assert_true(merge_discount.get("mode") == "combine", f"{workflow_name}:Merge requisitos + desconto must use combine mode")
    assert_true(
        merge_discount.get("combineBy") == "combineByPosition",
        f"{workflow_name}:Merge requisitos + desconto must combine by position",
    )
    merge_guardrails = get_node(workflow, "Append guardas iniciais item_plan").get("parameters", {})
    assert_merge_append(merge_guardrails, workflow_name, "Append guardas iniciais item_plan")
    assert_true(
        merge_guardrails.get("numberInputs") == 4,
        f"{workflow_name}:Append guardas iniciais item_plan must keep 4 inputs",
    )
    merge_conflicts = get_node(workflow, "Append conflitos FIN-04 item_plan").get("parameters", {})
    assert_merge_append(merge_conflicts, workflow_name, "Append conflitos FIN-04 item_plan")
    assert_true(
        merge_conflicts.get("numberInputs", 2) == 2,
        f"{workflow_name}:Append conflitos FIN-04 item_plan must keep 2 inputs",
    )
    merge_blocks = get_node(workflow, "Append bloqueios de inicio item_plan").get("parameters", {})
    assert_merge_append(merge_blocks, workflow_name, "Append bloqueios de inicio item_plan")
    assert_true(
        merge_blocks.get("numberInputs") == 4,
        f"{workflow_name}:Append bloqueios de inicio item_plan must keep 4 inputs",
    )
    merge_append = get_node(workflow, "Append decisões item_plan").get("parameters", {})
    assert_merge_append(merge_append, workflow_name, "Append decisões item_plan")
    assert_true(
        merge_append.get("numberInputs") == 4,
        f"{workflow_name}:Append decisões item_plan must keep 4 inputs",
    )

    connections = workflow.get("connections", {})

    def branch_targets(node_name: str, branch_index: int = 0) -> list[str]:
        branches = (connections.get(node_name) or {}).get("main") or []
        branch = branches[branch_index] if len(branches) > branch_index else []
        return [item.get("node") for item in branch]

    def incoming_sources(target_name: str) -> list[tuple[str, int, int]]:
        found = []
        for source_name, source_connections in connections.items():
            for branch_index, branch in enumerate((source_connections.get("main") or [])):
                for item in branch:
                    if item.get("node") == target_name:
                        found.append((source_name, branch_index, int(item.get("index", 0))))
        return sorted(found)

    def assert_branch(node_name: str, branch_index: int, expected: list[str]) -> None:
        actual = branch_targets(node_name, branch_index)
        assert_true(
            actual == expected,
            f"{workflow_name} {node_name} branch {branch_index} expected {expected} but found {actual}",
        )

    def validate_switch_node(node_name: str, expected_output_keys: list[str], fallback_output_name: Optional[str]) -> None:
        node = get_node(workflow, node_name)
        params = node.get("parameters", {})
        rules = ((params.get("rules") or {}).get("values")) or []
        options = params.get("options") or {}
        branches = (connections.get(node_name) or {}).get("main") or []
        assert_true(
            params.get("mode") in (None, "rules"),
            f"{workflow_name}:{node_name} must use Switch rules mode",
        )
        assert_true("output" not in params, f"{workflow_name}:{node_name} must not include expression-mode output")
        assert_true("numberOutputs" not in params, f"{workflow_name}:{node_name} must not include expression-mode numberOutputs")
        assert_true(options.get("ignoreCase") is False, f"{workflow_name}:{node_name} must keep ignoreCase=false")
        assert_true(
            len(rules) == len(expected_output_keys),
            f"{workflow_name}:{node_name} expected {len(expected_output_keys)} rules but found {len(rules)}",
        )
        actual_output_keys = []
        for index, rule in enumerate(rules):
            actual_output_keys.append(rule.get("outputKey"))
            assert_true(rule.get("renameOutput") is True, f"{workflow_name}:{node_name} rule {index} must rename output")
            assert_true(
                bool(str(rule.get("outputKey") or "").strip()),
                f"{workflow_name}:{node_name} rule {index} must define outputKey",
            )
            condition_block = rule.get("conditions") or {}
            assert_true(
                condition_block.get("combinator") == "and",
                f"{workflow_name}:{node_name} rule {index} must use combinator=and",
            )
            conditions = condition_block.get("conditions") or []
            assert_true(
                isinstance(conditions, list) and len(conditions) >= 1,
                f"{workflow_name}:{node_name} rule {index} must define at least one condition",
            )
        assert_true(
            actual_output_keys == expected_output_keys,
            f"{workflow_name}:{node_name} expected output keys {expected_output_keys} but found {actual_output_keys}",
        )
        expected_branches = len(expected_output_keys) + (1 if fallback_output_name else 0)
        assert_true(
            len(branches) == expected_branches,
            f"{workflow_name}:{node_name} expected {expected_branches} output branches but found {len(branches)}",
        )
        if fallback_output_name:
            assert_true(
                options.get("fallbackOutput") == "extra",
                f"{workflow_name}:{node_name} must use fallbackOutput='extra'",
            )
            assert_true(
                options.get("renameFallbackOutput") == fallback_output_name,
                f"{workflow_name}:{node_name} fallback must be named {fallback_output_name}",
            )
        else:
            assert_true(
                options.get("fallbackOutput") != "extra",
                f"{workflow_name}:{node_name} must not add fallback output",
            )

    for source, target in [
        ("Split Out template IDs", "FIN-02 Buscar item template"),
        ("FIN-02 Buscar item template", "Normalizar retorno template"),
        ("Normalizar retorno template", "Template retornou?"),
        ("FIN-04 Verificar conflitos por competencia", "Normalizar retorno conflitos"),
        ("Normalizar retorno conflitos", "Consulta de conflitos retornou?"),
        ("Condições de início", "Há bloqueio de início?"),
        ("Campos modelo percentual base", "Base percentual é SaaS?"),
        ("Campos modelo por unidade base", "Unidade é Afiliados?"),
        ("Campos desconto base", "Desconto nominal?"),
        ("Campos desconto nominal", "Desconto percentual?"),
        ("Campos desconto percentual", "Desconto por categorias?"),
        ("Merge requisitos modelo + tipo", "Merge requisitos + desconto"),
        ("Merge requisitos + desconto", "Consolidar campos obrigatórios visíveis"),
        ("Consolidar campos obrigatórios visíveis", "Append decisões item_plan"),
        ("Append guardas iniciais item_plan", "Append decisões item_plan"),
        ("Append conflitos FIN-04 item_plan", "Append decisões item_plan"),
        ("Append bloqueios de inicio item_plan", "Append decisões item_plan"),
        ("Append decisões item_plan", "Consolidar elegibilidade e planos"),
    ]:
        assert_true(branch_targets(source) == [target], f"{workflow_name} {source} must flow into {target}")

    validate_switch_node(
        "Modelo de cobrança",
        ["Percentual sobre base", "Por unidade"],
        "Fixo/Outros",
    )
    validate_switch_node(
        "Tipo de item",
        ["Produto", "Ajustes de fatura"],
        "Default",
    )
    validate_switch_node(
        "Tipo de conflito FIN-04",
        ["existing_fin04_conflict", "orphan_fin04_exists_for_rule_competence"],
        None,
    )
    validate_switch_node(
        "Motivo de bloqueio de início",
        [
            "specific_date_not_ready",
            "specific_date_finished",
            "blocked_missing_external_signal_onboarding_training",
            "blocked_missing_external_signal_setup_payment",
        ],
        None,
    )

    assert_branch("Pode preparar itens?", 0, ["Set pares template IDs"])
    assert_branch("Pode preparar itens?", 1, ["Emitir rule_result direto"])
    assert_branch("Template retornou?", 0, ["FIN-04 Verificar conflitos por competencia"])
    assert_branch("Template retornou?", 1, ["Definir fetch_template_item_error"])
    assert_branch("Consulta de conflitos retornou?", 0, ["Consulta de conflitos paginou?"])
    assert_branch("Consulta de conflitos retornou?", 1, ["Definir conflict_fin04_query_error"])
    assert_branch("Consulta de conflitos paginou?", 0, ["Definir conflict_fin04_query_requires_pagination"])
    assert_branch("Consulta de conflitos paginou?", 1, ["Template está Ativo?"])
    assert_branch("Template está Ativo?", 0, ["Mapear conflito FIN-04"])
    assert_branch("Template está Ativo?", 1, ["Definir inactive_template_phase"])
    assert_true(branch_targets("Mapear conflito FIN-04") == ["Há FIN-04 na competência?"], f"{workflow_name} Mapear conflito FIN-04 must flow into Há FIN-04 na competência?")
    assert_branch("Há FIN-04 na competência?", 0, ["Tipo de conflito FIN-04"])
    assert_branch("Há FIN-04 na competência?", 1, ["Condições de início"])
    assert_branch("Tipo de conflito FIN-04", 0, ["Definir existing_fin04_conflict"])
    assert_branch("Tipo de conflito FIN-04", 1, ["Definir orphan_fin04_exists_for_rule_competence"])
    assert_branch("Há bloqueio de início?", 0, ["Motivo de bloqueio de início"])
    assert_branch("Há bloqueio de início?", 1, ["Dimensões comerciais do item"])
    assert_branch("Motivo de bloqueio de início", 0, ["Definir specific_date_not_ready"])
    assert_branch("Motivo de bloqueio de início", 1, ["Definir specific_date_finished"])
    assert_branch("Motivo de bloqueio de início", 2, ["Definir blocked_missing_external_signal_onboarding_training"])
    assert_branch("Motivo de bloqueio de início", 3, ["Definir blocked_missing_external_signal_setup_payment"])
    assert_branch("Dimensões comerciais do item", 0, ["Modelo de cobrança", "Tipo de item", "Há desconto?"])
    assert_branch("Modelo de cobrança", 0, ["Campos modelo percentual base"])
    assert_branch("Modelo de cobrança", 1, ["Campos modelo por unidade base"])
    assert_branch("Modelo de cobrança", 2, ["Campos modelo fixo/outros"])
    assert_branch("Base percentual é SaaS?", 0, ["Campos modelo percentual SaaS"])
    assert_branch("Base percentual é SaaS?", 1, ["Merge requisitos modelo + tipo"])
    assert_branch("Unidade é Afiliados?", 0, ["Campos modelo por unidade afiliados"])
    assert_branch("Unidade é Afiliados?", 1, ["Merge requisitos modelo + tipo"])
    assert_branch("Tipo de item", 0, ["Campos tipo produto"])
    assert_branch("Tipo de item", 1, ["Campos tipo ajuste"])
    assert_branch("Tipo de item", 2, ["Merge requisitos modelo + tipo"])
    assert_branch("Há desconto?", 0, ["Campos desconto base"])
    assert_branch("Há desconto?", 1, ["Merge requisitos + desconto"])
    assert_branch("Desconto nominal?", 0, ["Campos desconto nominal"])
    assert_branch("Desconto nominal?", 1, ["Desconto percentual?"])
    assert_branch("Desconto percentual?", 0, ["Campos desconto percentual"])
    assert_branch("Desconto percentual?", 1, ["Desconto por categorias?"])
    assert_branch("Desconto por categorias?", 0, ["Campos desconto por categorias SaaS"])
    assert_branch("Desconto por categorias?", 1, ["Merge requisitos + desconto"])
    assert_true(
        incoming_sources("Append guardas iniciais item_plan") == sorted([
            ("Definir conflict_fin04_query_error", 0, 1),
            ("Definir conflict_fin04_query_requires_pagination", 0, 2),
            ("Definir fetch_template_item_error", 0, 0),
            ("Definir inactive_template_phase", 0, 3),
        ]),
        f"{workflow_name} Append guardas iniciais item_plan has unexpected inputs {incoming_sources('Append guardas iniciais item_plan')}",
    )
    assert_true(
        incoming_sources("Append conflitos FIN-04 item_plan") == sorted([
            ("Definir existing_fin04_conflict", 0, 0),
            ("Definir orphan_fin04_exists_for_rule_competence", 0, 1),
        ]),
        f"{workflow_name} Append conflitos FIN-04 item_plan has unexpected inputs {incoming_sources('Append conflitos FIN-04 item_plan')}",
    )
    assert_true(
        incoming_sources("Append bloqueios de inicio item_plan") == sorted([
            ("Definir blocked_missing_external_signal_onboarding_training", 0, 2),
            ("Definir blocked_missing_external_signal_setup_payment", 0, 3),
            ("Definir specific_date_finished", 0, 1),
            ("Definir specific_date_not_ready", 0, 0),
        ]),
        f"{workflow_name} Append bloqueios de inicio item_plan has unexpected inputs {incoming_sources('Append bloqueios de inicio item_plan')}",
    )

    assert_true(
        incoming_sources("Merge requisitos modelo + tipo") == sorted([
            ("Base percentual é SaaS?", 1, 0),
            ("Campos modelo fixo/outros", 0, 0),
            ("Campos modelo percentual SaaS", 0, 0),
            ("Campos modelo por unidade afiliados", 0, 0),
            ("Campos tipo ajuste", 0, 1),
            ("Campos tipo produto", 0, 1),
            ("Tipo de item", 2, 1),
            ("Unidade é Afiliados?", 1, 0),
        ]),
        f"{workflow_name} Merge requisitos modelo + tipo has unexpected inputs {incoming_sources('Merge requisitos modelo + tipo')}",
    )
    assert_true(
        incoming_sources("Merge requisitos + desconto") == sorted([
            ("Campos desconto por categorias SaaS", 0, 1),
            ("Desconto por categorias?", 1, 1),
            ("Há desconto?", 1, 1),
            ("Merge requisitos modelo + tipo", 0, 0),
        ]),
        f"{workflow_name} Merge requisitos + desconto has unexpected inputs {incoming_sources('Merge requisitos + desconto')}",
    )
    assert_true(
        incoming_sources("Append decisões item_plan") == sorted([
            ("Append bloqueios de inicio item_plan", 0, 3),
            ("Append conflitos FIN-04 item_plan", 0, 2),
            ("Append guardas iniciais item_plan", 0, 1),
            ("Consolidar campos obrigatórios visíveis", 0, 0),
        ]),
        f"{workflow_name} Append decisões item_plan has unexpected inputs {incoming_sources('Append decisões item_plan')}",
    )


def validate_rule_log_contract(workflow_name: str, workflow: dict) -> None:
    output_nodes = {
        "[FIN] 1.2 Preparar itens elegiveis da regra": [
            "Emitir rule_result direto",
            "Consolidar elegibilidade e planos",
        ],
        "[FIN] 1.3 Materializar itens FIN-04": [
            "Emitir rule_result direto",
            "Consolidar materializacao",
        ],
        "[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02": [
            "Finalizar rule_result",
        ],
    }
    for node_name in output_nodes.get(workflow_name, []):
        node_text = json.dumps(get_node(workflow, node_name), ensure_ascii=False)
        assert_true("logs: {" not in node_text, f"{workflow_name}:{node_name} still emits logs in fin_v3 payloads")
        assert_contains(node_text, "startedAt", f"{workflow_name}:{node_name} missing rule_result startedAt")
        assert_contains(node_text, "finishedAt", f"{workflow_name}:{node_name} missing rule_result finishedAt")
    if workflow_name == "[FIN] 1.0 Orquestrar faturamento mensal":
        for node_name in [
            "Accumulate results",
            "Finalize summary",
            "Normalizar retorno preparar itens",
            "Normalizar retorno materializacao",
            "Normalizar retorno finalizacao",
        ]:
            js_code = get_node(workflow, node_name).get("parameters", {}).get("jsCode", "")
            assert_true("workflowLogs" not in js_code, f"{workflow_name}:{node_name} still references workflowLogs")
            assert_true("entityLogs" not in js_code, f"{workflow_name}:{node_name} still references entityLogs")
            assert_true("graphqlLogs" not in js_code, f"{workflow_name}:{node_name} still references graphqlLogs")
        finalize_js = get_node(workflow, "Finalize summary").get("parameters", {}).get("jsCode", "")
        assert_contains(finalize_js, "ruleLogRows", f"{workflow_name}:Finalize summary must emit ruleLogRows")
        assert_contains(finalize_js, "failed_nodes", f"{workflow_name}:Finalize summary missing failed_nodes aggregation")
        assert_contains(finalize_js, "failures_json", f"{workflow_name}:Finalize summary missing failures_json aggregation")


def validate_fin04_webhook_auxiliary() -> None:
    workflow_name = "Webhooks FIN-02 e 04"
    workflow = load_workflow(workflow_name)
    assert_true(workflow["name"] == workflow_name, f"{workflow_name} has unexpected name {workflow['name']}")
    validate_expression_syntax(workflow_name, workflow)

    context_text = json.dumps(get_node(workflow, "Contexto cálculo"), ensure_ascii=False)
    assert_contains(context_text, "1260337244", f"{workflow_name}:Contexto cálculo must accept subtype 1260337244")
    assert_contains(
        context_text,
        "Esperado: 1260337139, 1260337244 ou 1260337296.",
        f"{workflow_name}:Contexto cálculo must list all valid category subtype ids",
    )

    switch_node = get_node(workflow, "Tipo de ajuste da categoria")
    switch_text = json.dumps(switch_node, ensure_ascii=False)
    for token in ["1260337139", "1260337244", "1260337296"]:
        assert_contains(switch_text, token, f"{workflow_name}:Tipo de ajuste da categoria missing subtype {token}")
    rules = switch_node.get("parameters", {}).get("rules", {}).get("values", [])
    assert_true(len(rules) == 3, f"{workflow_name}:Tipo de ajuste da categoria must keep exactly 3 category branches")

    no_op_node = get_node(workflow, "Manter base para isenção de backoffice na categoria")
    no_op_text = json.dumps(no_op_node, ensure_ascii=False)
    assert_contains(
        no_op_text,
        "Number($json.calcBaseValue || 0)",
        f"{workflow_name}:backoffice no-op branch must preserve calcBaseValue",
    )
    for forbidden in ["categoryBaseValue", "categoryDiscountPercent", "categoryTaxPercent"]:
        assert_true(
            forbidden not in no_op_text,
            f"{workflow_name}:backoffice no-op branch must not depend on {forbidden}",
        )

    connections = workflow.get("connections", {})

    def branch_targets(node_name: str, branch_index: int = 0) -> list[str]:
        branches = (connections.get(node_name) or {}).get("main") or []
        branch = branches[branch_index] if len(branches) > branch_index else []
        return [item.get("node") for item in branch]

    assert_true(
        branch_targets("Subtipo do desconto por categoria é válido?", 0) == ["Tipo de ajuste da categoria"],
        f"{workflow_name}:Subtipo do desconto por categoria é válido? true branch must flow into Tipo de ajuste da categoria",
    )
    assert_true(
        branch_targets("Subtipo do desconto por categoria é válido?", 1) == ["Erro subtipo do desconto por categoria"],
        f"{workflow_name}:Subtipo do desconto por categoria é válido? false branch must flow into Erro subtipo do desconto por categoria",
    )
    assert_true(
        branch_targets("Tipo de ajuste da categoria", 0) == ["Aplicar desconto percentual na base da categoria"],
        f"{workflow_name}:Tipo de ajuste da categoria first branch must flow into Aplicar desconto percentual na base da categoria",
    )
    assert_true(
        branch_targets("Tipo de ajuste da categoria", 1) == ["Manter base para isenção de backoffice na categoria"],
        f"{workflow_name}:Tipo de ajuste da categoria second branch must flow into Manter base para isenção de backoffice na categoria",
    )
    assert_true(
        branch_targets("Tipo de ajuste da categoria", 2) == ["Aplicar isenção de imposto na base da categoria"],
        f"{workflow_name}:Tipo de ajuste da categoria third branch must flow into Aplicar isenção de imposto na base da categoria",
    )
    for node_name in [
        "Aplicar desconto percentual na base da categoria",
        "Manter base para isenção de backoffice na categoria",
        "Aplicar isenção de imposto na base da categoria",
    ]:
        assert_true(
            branch_targets(node_name) == ["Calcular subtotal percentual SaaS"],
            f"{workflow_name}:{node_name} must flow into Calcular subtotal percentual SaaS",
        )


def validate_fin_seed_scenario_coverage() -> None:
    workflow_name = "Gerar cenarios FIN-01 + FIN-02"
    workflow = load_workflow(workflow_name)
    js_code = next(
        (
            node.get("parameters", {}).get("jsCode", "")
            for node in workflow.get("nodes", [])
            if "FROZEN_MANIFEST" in node.get("parameters", {}).get("jsCode", "")
        ),
        "",
    )
    assert_true(js_code != "", f"{workflow_name} missing FROZEN_MANIFEST scenario source")

    assert_contains(js_code, '"scenario_id":"S194"', f"{workflow_name} missing scenario S194")
    assert_contains(js_code, '"tipo_de_desconto_espa_os":"1260337139"', f"{workflow_name} missing subtype 1260337139 scenario")
    assert_contains(
        js_code,
        '"desconto_no_valor_vari_vel_do_saas_para_espa_os"',
        f"{workflow_name} missing percentual category field coverage",
    )

    assert_contains(js_code, '"scenario_id":"S195"', f"{workflow_name} missing scenario S195")
    assert_contains(js_code, '"tipo_de_desconto_espa_os":"1260337244"', f"{workflow_name} missing subtype 1260337244 scenario")

    assert_contains(js_code, '"scenario_id":"S196"', f"{workflow_name} missing scenario S196")
    assert_contains(js_code, '"tipo_de_desconto_espa_os":"1260337296"', f"{workflow_name} missing subtype 1260337296 scenario")
    assert_contains(
        js_code,
        '"valor_do_imposto_a_ser_isento_espa_os"',
        f"{workflow_name} missing local tax exemption field coverage",
    )

    assert_contains(js_code, '"scenario_id":"S198"', f"{workflow_name} missing scenario S198")
    assert_contains(js_code, '"tipo_de_desconto_outros":"1260337244"', f"{workflow_name} missing subtype 1260337244 coverage for Outros")


def main() -> None:
    validate_expected_exports()
    for workflow_name in EXPECTED:
        workflow = load_workflow(workflow_name)
        assert_true(workflow["name"] == workflow_name, f"{workflow_name} has unexpected name {workflow['name']}")
        validate_graphql_nodes(workflow_name, workflow)
        validate_pagination_context(workflow_name, workflow)
        validate_code_node_return_contracts(workflow_name, workflow)
        validate_execute_workflow_trigger_examples(workflow_name, workflow)
        validate_expression_syntax(workflow_name, workflow)
        validate_contract_tokens(workflow_name, workflow)
        validate_item_prep_pipeline(workflow_name, workflow)
        validate_rule_log_contract(workflow_name, workflow)
        if workflow_name == "[FIN] 1.0 Orquestrar faturamento mensal":
            validate_orchestrator(workflow)
    validate_fin04_webhook_auxiliary()
    validate_fin_seed_scenario_coverage()
    print("fin_monthly_workflows_ok")


if __name__ == "__main__":
    main()
