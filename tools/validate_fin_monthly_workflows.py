#!/usr/bin/env python3
import json
import pathlib
import subprocess
import tempfile


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = ROOT / "raw" / "n8n" / "workflows"

EXPECTED = [
    "[FIN] 1 Orquestrar faturamento mensal",
    "[FIN] 1.1 Preparar regras elegiveis",
    "[FIN] 1.2 Preparar itens elegiveis da regra",
    "[FIN] 1.3 Materializar itens FIN-04",
    "[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02",
]

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
        "Materializar itens FIN-04",
        "Criar fatura FIN-03 e concluir regra",
        "Emit workflow run logs",
        "Persistir fin_billing_run_log",
        "Ha entity logs?",
        "Persistir fin_billing_entity_log",
        "Ha graphql logs?",
        "Persistir fin_billing_graphql_log",
    ]:
        assert_true(required in node_names, f"orchestrator missing node {required}")
    text = json.dumps(workflow, ensure_ascii=False)
    assert_contains(text, "competence_date", "orchestrator missing competence_date tracking")


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


def validate_contract_tokens(workflow_name: str, workflow: dict) -> None:
    text = json.dumps(workflow, ensure_ascii=False)
    for forbidden in ["parent_ids", "Faturas associadas", "Itens associados"]:
        assert_true(forbidden not in text, f"{workflow_name} contains forbidden token {forbidden}")
    if workflow_name == "[FIN] 1.1 Preparar regras elegiveis":
        for token in ["341313813", "itens_da_fatura", "invoice_already_exists"]:
            assert_contains(text, token, f"{workflow_name} missing token {token}")
    if workflow_name == "[FIN] 1.2 Preparar itens elegiveis da regra":
        for token in [
            "341306826",
            "341351580",
            "missing_required_nonzero_field",
            "existing_fin04_conflict",
            "blocked_missing_external_signal_onboarding_training",
            "blocked_missing_external_signal_setup_payment",
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


def main() -> None:
    validate_expected_exports()
    for workflow_name in EXPECTED:
        workflow = load_workflow(workflow_name)
        assert_true(workflow["name"] == workflow_name, f"{workflow_name} has unexpected name {workflow['name']}")
        validate_graphql_nodes(workflow_name, workflow)
        validate_pagination_context(workflow_name, workflow)
        validate_contract_tokens(workflow_name, workflow)
        if workflow_name == "[FIN] 1 Orquestrar faturamento mensal":
            validate_orchestrator(workflow)
    print("fin_monthly_workflows_ok")


if __name__ == "__main__":
    main()
