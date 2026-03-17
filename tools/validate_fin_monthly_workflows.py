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


def assert_almost_equal(actual: float, expected: float, message: str, tolerance: float = 1e-9) -> None:
    if abs(float(actual) - float(expected)) > tolerance:
        fail(f"{message}: expected {expected}, got {actual}")


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


def evaluate_expression(expression: str, payload: dict, label: str):
    wrapped = (
        "const $json = "
        + json.dumps(payload, ensure_ascii=False)
        + ";\nconst result = ("
        + expression
        + ");\nprocess.stdout.write(JSON.stringify(result));\n"
    )
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as tmp:
        tmp.write(wrapped)
        tmp_path = pathlib.Path(tmp.name)
    try:
        proc = subprocess.run(["node", str(tmp_path)], capture_output=True, text=True)
    finally:
        tmp_path.unlink(missing_ok=True)
    if proc.returncode != 0:
        fail(f"expression execution failed for {label}: {proc.stderr.strip()}")
    try:
        return json.loads(proc.stdout or "null")
    except json.JSONDecodeError as exc:
        fail(f"expression execution returned invalid JSON for {label}: {exc}")


def get_assignment_expression(workflow: dict, node_name: str, assignment_name: str) -> str:
    node = get_node(workflow, node_name)
    assignments = node.get("parameters", {}).get("assignments", {}).get("assignments", [])
    for assignment in assignments:
        if assignment.get("name") != assignment_name:
            continue
        value = assignment.get("value")
        assert_true(
            isinstance(value, str) and value.startswith("={{") and value.endswith("}}"),
            f"{workflow['name']}:{node_name}:{assignment_name} must remain an expression assignment",
        )
        return value[3:-2]
    fail(f"{workflow['name']}:{node_name} missing assignment {assignment_name}")


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
    assert_contains(context_text, "categoryEntries", f"{workflow_name}:Contexto cálculo must expose categoryEntries")
    assert_contains(context_text, "1260337244", f"{workflow_name}:Contexto cálculo must accept subtype 1260337244")
    assert_contains(context_text, "selectedCategories", f"{workflow_name}:Contexto cálculo must expose selectedCategories")
    assert_contains(context_text, "selectedCategorySubtypes", f"{workflow_name}:Contexto cálculo must expose selectedCategorySubtypes")
    assert_contains(
        context_text,
        "Valor base SaaS ausente ou inválido",
        f"{workflow_name}:Contexto cálculo must validate required base values",
    )
    assert_contains(
        context_text,
        "Percentual de desconto da categoria ausente ou inválido",
        f"{workflow_name}:Contexto cálculo must validate required category discount values",
    )
    assert_contains(
        context_text,
        "Percentual de isenção de imposto da categoria ausente ou inválido",
        f"{workflow_name}:Contexto cálculo must validate required category tax values",
    )
    assert_true(
        "Seleção múltipla de categorias SaaS não é suportada" not in context_text,
        f"{workflow_name}:Contexto cálculo must no longer fail closed for multiple selected categories",
    )
    assert_true(
        "Seleção múltipla de subtipos de desconto por categoria não é suportada" not in context_text,
        f"{workflow_name}:Contexto cálculo must no longer fail closed for multiple selected category subtypes",
    )
    assert_contains(
        context_text,
        "Esperado: 1260337139, 1260337244 ou 1260337296.",
        f"{workflow_name}:Contexto cálculo must list all valid category subtype ids",
    )

    split_node = get_node(workflow, "Split Out categorias de desconto")
    assert_true(
        split_node.get("type") == "n8n-nodes-base.splitOut",
        f"{workflow_name}:Split Out categorias de desconto must use splitOut",
    )
    assert_true(
        split_node.get("parameters", {}).get("fieldToSplitOut") == "categoryEntries",
        f"{workflow_name}:Split Out categorias de desconto must split categoryEntries",
    )

    aggregate_node = get_node(workflow, "Aggregate descontos por categoria")
    aggregate_text = json.dumps(aggregate_node, ensure_ascii=False)
    assert_true(
        aggregate_node.get("type") == "n8n-nodes-base.aggregate",
        f"{workflow_name}:Aggregate descontos por categoria must use aggregate",
    )
    for token in ["categoryDiscountAmount", "categoryDiscountTrace", "categoryBackofficePlaceholder"]:
        assert_contains(
            aggregate_text,
            token,
            f"{workflow_name}:Aggregate descontos por categoria must aggregate {token}",
        )

    no_op_node = get_node(workflow, "Manter base para isenção de backoffice na categoria")
    no_op_text = json.dumps(no_op_node, ensure_ascii=False)
    assert_contains(
        no_op_text,
        "external_backoffice_pending",
        f"{workflow_name}:backoffice placeholder branch must keep the pending marker",
    )
    assert_contains(no_op_text, "1260337244", f"{workflow_name}:backoffice placeholder branch must tag subtype 1260337244")

    connections = workflow.get("connections", {})

    def branch_targets(node_name: str, branch_index: int = 0) -> list[str]:
        branches = (connections.get(node_name) or {}).get("main") or []
        branch = branches[branch_index] if len(branches) > branch_index else []
        return [item.get("node") for item in branch]

    def branch_targets_with_indexes(node_name: str, branch_index: int = 0) -> list[tuple[str, int]]:
        branches = (connections.get(node_name) or {}).get("main") or []
        branch = branches[branch_index] if len(branches) > branch_index else []
        return [(str(item.get("node")), int(item.get("index", 0))) for item in branch]

    assert_true(
        sorted(branch_targets_with_indexes("Contexto do desconto por categoria", 0))
        == sorted(
            [
                ("Subtipo do desconto por categoria é válido?", 0),
                ("Merge contexto + desconto por categoria", 0),
            ]
        ),
        f"{workflow_name}:Contexto do desconto por categoria must feed validation and the merge context",
    )
    assert_true(
        branch_targets("Subtipo do desconto por categoria é válido?", 0) == ["Split Out categorias de desconto"],
        f"{workflow_name}:Subtipo do desconto por categoria é válido? true branch must flow into Split Out categorias de desconto",
    )
    assert_true(
        branch_targets("Subtipo do desconto por categoria é válido?", 1) == ["Erro subtipo do desconto por categoria"],
        f"{workflow_name}:Subtipo do desconto por categoria é válido? false branch must flow into Erro subtipo do desconto por categoria",
    )
    assert_true(
        sorted(branch_targets("Split Out categorias de desconto", 0))
        == sorted(
            [
                "Categoria inclui valor percentual?",
                "Categoria inclui backoffice?",
                "Categoria inclui impostos locais?",
            ]
        ),
        f"{workflow_name}:Split Out categorias de desconto must fan out into the three subtype checks",
    )
    assert_true(
        branch_targets("Categoria inclui valor percentual?", 0) == ["Aplicar desconto percentual na base da categoria"],
        f"{workflow_name}:Categoria inclui valor percentual? true branch must flow into Aplicar desconto percentual na base da categoria",
    )
    assert_true(
        branch_targets("Categoria inclui backoffice?", 0) == ["Manter base para isenção de backoffice na categoria"],
        f"{workflow_name}:Categoria inclui backoffice? true branch must flow into Manter base para isenção de backoffice na categoria",
    )
    assert_true(
        branch_targets("Categoria inclui impostos locais?", 0) == ["Aplicar isenção de imposto na base da categoria"],
        f"{workflow_name}:Categoria inclui impostos locais? true branch must flow into Aplicar isenção de imposto na base da categoria",
    )
    assert_true(
        branch_targets_with_indexes("Aplicar desconto percentual na base da categoria", 0) == [("Append descontos por categoria", 0)],
        f"{workflow_name}:Aplicar desconto percentual na base da categoria must feed the append merge at input 0",
    )
    assert_true(
        branch_targets_with_indexes("Manter base para isenção de backoffice na categoria", 0)
        == [("Append descontos por categoria", 1)],
        f"{workflow_name}:Manter base para isenção de backoffice na categoria must feed the append merge at input 1",
    )
    assert_true(
        branch_targets_with_indexes("Aplicar isenção de imposto na base da categoria", 0)
        == [("Append descontos por categoria", 2)],
        f"{workflow_name}:Aplicar isenção de imposto na base da categoria must feed the append merge at input 2",
    )
    assert_true(
        branch_targets("Append descontos por categoria", 0) == ["Aggregate descontos por categoria"],
        f"{workflow_name}:Append descontos por categoria must flow into Aggregate descontos por categoria",
    )
    assert_true(
        branch_targets("Aggregate descontos por categoria", 0) == ["Consolidar desconto por categoria"],
        f"{workflow_name}:Aggregate descontos por categoria must flow into Consolidar desconto por categoria",
    )
    assert_true(
        branch_targets_with_indexes("Consolidar desconto por categoria", 0) == [("Merge contexto + desconto por categoria", 1)],
        f"{workflow_name}:Consolidar desconto por categoria must feed Merge contexto + desconto por categoria at input 1",
    )
    assert_true(
        branch_targets("Merge contexto + desconto por categoria", 0) == ["Aplicar descontos agregados das categorias"],
        f"{workflow_name}:Merge contexto + desconto por categoria must flow into Aplicar descontos agregados das categorias",
    )
    assert_true(
        branch_targets("Aplicar descontos agregados das categorias", 0) == ["Calcular subtotal percentual SaaS"],
        f"{workflow_name}:Aplicar descontos agregados das categorias must flow into Calcular subtotal percentual SaaS",
    )

    context_expression = get_assignment_expression(workflow, "Contexto cálculo", "calcContext")
    percentual_category_expression = get_assignment_expression(
        workflow,
        "Aplicar desconto percentual na base da categoria",
        "categoryDiscountAmount",
    )
    local_tax_expression = get_assignment_expression(
        workflow,
        "Aplicar isenção de imposto na base da categoria",
        "categoryDiscountAmount",
    )
    backoffice_amount_expression = get_assignment_expression(
        workflow,
        "Manter base para isenção de backoffice na categoria",
        "categoryDiscountAmount",
    )
    backoffice_marker_expression = get_assignment_expression(
        workflow,
        "Manter base para isenção de backoffice na categoria",
        "categoryBackofficePlaceholder",
    )
    category_total_expression = get_assignment_expression(
        workflow,
        "Consolidar desconto por categoria",
        "categoryDiscountTotal",
    )
    category_base_apply_expression = get_assignment_expression(
        workflow,
        "Aplicar descontos agregados das categorias",
        "calcBaseValue",
    )
    subtotal_expression = get_assignment_expression(workflow, "Calcular subtotal percentual SaaS", "calcSubtotal")
    final_init_expression = get_assignment_expression(
        workflow,
        "Inicializar total final pelo subtotal",
        "calcFinalTotal",
    )
    nominal_expression = get_assignment_expression(
        workflow,
        "Aplicar desconto nominal pós-subtotal",
        "calcFinalTotal",
    )
    percentual_expression = get_assignment_expression(
        workflow,
        "Aplicar desconto percentual pós-subtotal",
        "calcFinalTotal",
    )

    def field(field_id: str, *, report_value=None, value=None, native_value=None, array_value=None, connected_repo_items=None) -> dict:
        return {
            "field": {"id": field_id},
            "report_value": report_value,
            "value": value,
            "native_value": native_value,
            "array_value": array_value if array_value is not None else [],
            "connectedRepoItems": connected_repo_items if connected_repo_items is not None else [],
        }

    category_type_field_map = {
        "A&B": "tipo_de_desconto_a_b_1",
        "E&S": "tipo_de_desconto_e_s",
        "Hospedagem": "tipo_de_desconto_hospedagem",
        "Espaços": "tipo_de_desconto_espa_os",
        "Outros": "tipo_de_desconto_outros",
    }
    category_suffix_map = {
        "A&B": "a_b",
        "E&S": "e_s",
        "Hospedagem": "hospedagem",
        "Espaços": "espa_os",
        "Outros": "outros",
    }

    def build_context_payload(
        category_subtypes_by_category: dict,
        *,
        discount_types: Optional[list[str]] = None,
        category_values: Optional[dict] = None,
        percentual_base: str = "10",
        percentual_desconto: str = "0",
        nominal_discount: str = "0",
    ) -> dict:
        values = {
            category: {"base": "0", "discount": "0", "tax": "0"}
            for category in category_suffix_map
        }
        for category, overrides in (category_values or {}).items():
            values.setdefault(category, {"base": "0", "discount": "0", "tax": "0"})
            values[category].update({key: str(value) for key, value in overrides.items()})

        fields = [
            field("tipo_de_item", report_value="Produto"),
            field("moeda_da_fatura", report_value="BRL - R$"),
            field("modelo_de_cobran_a", report_value="Percentual sobre base"),
            field("base_do_c_lculo_percentual", array_value=["1268890278"]),
            field("percentual_a_ser_aplicado_na_base", report_value=percentual_base),
            field("percentual_de_desconto_a_ser_aplicado", report_value=percentual_desconto),
            field("h_algum_desconto_nesse_item", report_value="Sim"),
            field(
                "tipo_de_desconto_no_item",
                array_value=discount_types if discount_types is not None else ["Categorias de propostas"],
            ),
            field(
                "categorias_do_saas_isentas_ou_com_desconto",
                array_value=list(category_subtypes_by_category.keys()),
            ),
            field("valor_de_base_brl", report_value="0"),
            field("valor_total_do_item_brl", report_value="0"),
            field("valor_total_do_item_com_descontos_brl", report_value="0"),
            field("valor_unit_rio_brl", report_value="0"),
            field("valor_nominal_a_ser_descontado_brl", report_value=nominal_discount),
        ]

        for category, field_id in category_type_field_map.items():
            fields.append(field(field_id, array_value=category_subtypes_by_category.get(category, [])))

        for category, suffix in category_suffix_map.items():
            category_value = values.get(category, {"base": "0", "discount": "0", "tax": "0"})
            fields.extend(
                [
                    field(f"valor_base_saas_categoria_{suffix}_brl", report_value=category_value.get("base", "0")),
                    field(
                        f"desconto_no_valor_vari_vel_do_saas_para_{suffix}",
                        report_value=category_value.get("discount", "0"),
                    ),
                    field(
                        f"valor_do_imposto_a_ser_isento_{suffix}",
                        report_value=category_value.get("tax", "0"),
                    ),
                ]
            )

        return {
            "data": {
                "card": {
                    "id": "123456",
                    "pipe": {"id": "306859915"},
                    "fields": fields,
                }
            }
        }

    def simulate_discount_flow(context_payload: dict, label: str) -> dict:
        category_amounts = []
        backoffice_markers = []

        for entry in context_payload.get("categoryEntries", []):
            selected_subtypes = entry.get("selectedSubtypes", [])
            if "1260337139" in selected_subtypes:
                category_amounts.append(
                    float(
                        evaluate_expression(
                            percentual_category_expression,
                            entry,
                            f"{label}:percentual-category",
                        )
                    )
                )
            if "1260337244" in selected_subtypes:
                category_amounts.append(
                    float(
                        evaluate_expression(
                            backoffice_amount_expression,
                            entry,
                            f"{label}:backoffice-category",
                        )
                    )
                )
                backoffice_markers.append(
                    str(
                        evaluate_expression(
                            backoffice_marker_expression,
                            entry,
                            f"{label}:backoffice-marker",
                        )
                    )
                )
            if "1260337296" in selected_subtypes:
                category_amounts.append(
                    float(
                        evaluate_expression(
                            local_tax_expression,
                            entry,
                            f"{label}:local-tax-category",
                        )
                    )
                )

        category_discount_total = float(
            evaluate_expression(
                category_total_expression,
                {"categoryDiscountAmount": category_amounts},
                f"{label}:category-discount-total",
            )
        )
        base_after_categories = float(
            evaluate_expression(
                category_base_apply_expression,
                {
                    "calcBaseValue": context_payload.get("saasBaseSum", 0),
                    "categoryDiscountTotal": category_discount_total,
                },
                f"{label}:base-after-categories",
            )
        )
        subtotal = float(
            evaluate_expression(
                subtotal_expression,
                {"calcBaseValue": base_after_categories, "calcContext": context_payload},
                f"{label}:subtotal",
            )
        )
        final_total = float(
            evaluate_expression(
                final_init_expression,
                {"calcSubtotal": subtotal},
                f"{label}:final-init",
            )
        )

        if context_payload.get("hasNominalDiscount") is True:
            final_total = float(
                evaluate_expression(
                    nominal_expression,
                    {"calcFinalTotal": final_total, "calcContext": context_payload},
                    f"{label}:nominal",
                )
            )
        if context_payload.get("hasPercentualDiscount") is True:
            final_total = float(
                evaluate_expression(
                    percentual_expression,
                    {"calcFinalTotal": final_total, "calcContext": context_payload},
                    f"{label}:percentual",
                )
            )

        return {
            "category_amounts": category_amounts,
            "category_discount_total": category_discount_total,
            "base_after_categories": base_after_categories,
            "subtotal": subtotal,
            "final_total": final_total,
            "backoffice_markers": backoffice_markers,
        }

    two_categories = evaluate_expression(
        context_expression,
        build_context_payload(
            {
                "Espaços": ["1260337139"],
                "Outros": ["1260337296"],
            },
            category_values={
                "Espaços": {"base": "100", "discount": "15", "tax": "0"},
                "Outros": {"base": "50", "discount": "0", "tax": "8"},
            },
        ),
        f"{workflow_name}:Contexto cálculo two-categories payload",
    )
    assert_true(
        two_categories.get("categoryDiscountValidationErrorMessage") == "",
        f"{workflow_name}:two-categories payload must not set categoryDiscountValidationErrorMessage",
    )
    assert_true(
        two_categories.get("selectedCategories") == ["Espaços", "Outros"],
        f"{workflow_name}:two-categories payload must preserve both selected categories",
    )
    assert_true(
        len(two_categories.get("categoryEntries") or []) == 2,
        f"{workflow_name}:two-categories payload must expose 2 category entries",
    )
    two_categories_result = simulate_discount_flow(two_categories, f"{workflow_name}:two-categories flow")
    assert_almost_equal(
        two_categories_result["category_discount_total"],
        19,
        f"{workflow_name}:two-categories payload must sum category discounts across categories",
    )
    assert_almost_equal(
        two_categories_result["base_after_categories"],
        131,
        f"{workflow_name}:two-categories payload must subtract the aggregated category discount once from calcBaseValue",
    )
    assert_almost_equal(
        two_categories_result["subtotal"],
        13.1,
        f"{workflow_name}:two-categories payload must calculate subtotal after category aggregation",
    )

    multiple_category_subtypes = evaluate_expression(
        context_expression,
        build_context_payload(
            {"Espaços": ["1260337139", "1260337296"]},
            category_values={"Espaços": {"base": "100", "discount": "15", "tax": "7"}},
        ),
        f"{workflow_name}:Contexto cálculo multiple-category-subtypes payload",
    )
    assert_true(
        multiple_category_subtypes.get("categoryDiscountValidationErrorMessage") == "",
        f"{workflow_name}:multiple-category-subtypes payload must now be accepted",
    )
    assert_true(
        multiple_category_subtypes.get("selectedCategorySubtypes") == ["1260337139", "1260337296"],
        f"{workflow_name}:multiple-category-subtypes payload must preserve both selected subtypes",
    )
    multiple_category_subtypes_result = simulate_discount_flow(
        multiple_category_subtypes,
        f"{workflow_name}:multiple-category-subtypes flow",
    )
    assert_almost_equal(
        multiple_category_subtypes_result["category_discount_total"],
        22,
        f"{workflow_name}:multiple-category-subtypes payload must sum both subtype paths for the same category",
    )
    assert_true(
        not multiple_category_subtypes_result["backoffice_markers"],
        f"{workflow_name}:multiple-category-subtypes payload should not produce backoffice markers without subtype 1260337244",
    )

    backoffice_placeholder = evaluate_expression(
        context_expression,
        build_context_payload(
            {"Outros": ["1260337244"]},
            category_values={"Outros": {"base": "50", "discount": "0", "tax": "0"}},
        ),
        f"{workflow_name}:Contexto cálculo backoffice placeholder payload",
    )
    assert_true(
        backoffice_placeholder.get("categoryDiscountValidationErrorMessage") == "",
        f"{workflow_name}:backoffice placeholder payload must not fail validation",
    )
    backoffice_placeholder_result = simulate_discount_flow(
        backoffice_placeholder,
        f"{workflow_name}:backoffice placeholder flow",
    )
    assert_almost_equal(
        backoffice_placeholder_result["category_discount_total"],
        0,
        f"{workflow_name}:backoffice placeholder payload must keep category discount at zero",
    )
    assert_true(
        backoffice_placeholder_result["backoffice_markers"] == ["external_backoffice_pending|Outros|1260337244"],
        f"{workflow_name}:backoffice placeholder payload must keep the placeholder marker",
    )

    combined_discounts = evaluate_expression(
        context_expression,
        build_context_payload(
            {"Espaços": ["1260337139", "1260337296"]},
            discount_types=["Categorias de propostas", "Nominal", "Percentual"],
            category_values={"Espaços": {"base": "100", "discount": "15", "tax": "5"}},
            percentual_base="20",
            nominal_discount="3",
            percentual_desconto="10",
        ),
        f"{workflow_name}:Contexto cálculo combined-discounts payload",
    )
    combined_discounts_result = simulate_discount_flow(
        combined_discounts,
        f"{workflow_name}:combined-discounts flow",
    )
    assert_almost_equal(
        combined_discounts_result["category_discount_total"],
        20,
        f"{workflow_name}:combined-discounts payload must sum all category subtype discounts before subtotal",
    )
    assert_almost_equal(
        combined_discounts_result["subtotal"],
        16,
        f"{workflow_name}:combined-discounts payload must calculate subtotal after category aggregation",
    )
    assert_almost_equal(
        combined_discounts_result["final_total"],
        11.7,
        f"{workflow_name}:combined-discounts payload must preserve the order categoria -> subtotal -> nominal -> percentual",
    )

    missing_required_base = evaluate_expression(
        context_expression,
        build_context_payload(
            {"Espaços": ["1260337139"]},
            category_values={"Espaços": {"base": "", "discount": "15", "tax": "0"}},
        ),
        f"{workflow_name}:Contexto cálculo missing-base payload",
    )
    assert_contains(
        str(missing_required_base.get("categoryDiscountValidationErrorMessage") or ""),
        "Valor base SaaS ausente ou inválido",
        f"{workflow_name}:missing-base payload must fail when a required numeric category base is missing",
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
