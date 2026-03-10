#!/usr/bin/env python3
import json
import pathlib
import subprocess
import sys
import tempfile


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = ROOT / "raw" / "n8n" / "workflows"
INDEX_PATH = ROOT / "raw" / "n8n" / "workflows.index.json"

EXPECTED = {
    "sbjuPdz4ipegrKh2": "[FIN] 1 Orquestrar faturamento mensal",
    "KRYkEmSTkuVD8Nsn": "[FIN] 1.1 Preparar regras elegiveis",
    "0nUKP2OUpRLgMSxq": "[FIN] 1.2 Preparar itens elegiveis da regra",
    "dC7sV4pLm9QxT2aK": "[FIN] 1.3 Materializar itens FIN-04",
    "fN6rJ3wUz8YhL5cP": "[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02",
}

PLACEHOLDERS = {
    "PLACEHOLDER_FIN_BILLING_RUN_LOG",
    "PLACEHOLDER_FIN_BILLING_ENTITY_LOG",
    "PLACEHOLDER_FIN_BILLING_GRAPHQL_LOG",
}

GRAPHQL_EXPECTATIONS = {
    "KRYkEmSTkuVD8Nsn": {
        "FIN-01: Listar regras Ativo (page)": ["341313813"],
        "FIN-03: Prefetch faturas (competencia)": ["data_de_compet_ncia"],
    },
    "0nUKP2OUpRLgMSxq": {
        "FIN-02 Buscar item template": ["card(id:"],
        "FIN-04 Verificar conflitos por competencia": ["id_do_item_usado_como_template_fin_02"],
    },
    "dC7sV4pLm9QxT2aK": {
        "FIN-04 Recheck conflito antes de criar": ["id_do_item_usado_como_template_fin_02"],
        "FIN-04 Criar item": ["createCard"],
    },
    "fN6rJ3wUz8YhL5cP": {
        "FIN-03 Recheck fatura existente": ["id_da_regra_de_faturamento"],
        "FIN-03 Criar fatura": ["createCard"],
        "FIN-04 Vincular id_da_fatura": ["id_da_fatura"],
        "FIN-02 Atualizar parcelas_pagas": ["parcelas_pagas"],
        "FIN-02 Atualizar status": ["status"],
    },
}


def fail(message: str) -> None:
    raise SystemExit(message)


def load_workflow(workflow_id: str) -> dict:
    path = WORKFLOWS_DIR / f"{workflow_id}.json"
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


def validate_index() -> None:
    index_data = json.loads(INDEX_PATH.read_text()).get("data", [])
    names = {item.get("id"): item.get("name") for item in index_data}
    for workflow_id, expected_name in EXPECTED.items():
        assert_true(names.get(workflow_id) == expected_name, f"index missing workflow {workflow_id} -> {expected_name}")


def validate_orchestrator(workflow: dict) -> None:
    node_names = {node["name"] for node in workflow["nodes"]}
    for required in [
        "Cron (dias 1-5 07:00)",
        "Manual Trigger",
        "Preparar regras elegiveis",
        "Preparar itens elegiveis da regra",
        "Materializar itens FIN-04",
        "Criar fatura FIN-03 e concluir regra",
        "Ha entity logs?",
        "Ha graphql logs?",
    ]:
        assert_true(required in node_names, f"orchestrator missing node {required}")
    text = json.dumps(workflow, ensure_ascii=False)
    for placeholder in PLACEHOLDERS:
        assert_contains(text, placeholder, f"orchestrator missing placeholder {placeholder}")
    assert_contains(text, "competence_date", "orchestrator missing competence_date tracking")


def validate_graphql_nodes(workflow_id: str, workflow: dict) -> None:
    expectations = GRAPHQL_EXPECTATIONS.get(workflow_id, {})
    for node in workflow["nodes"]:
        params = node.get("parameters", {})
        js_code = params.get("jsCode")
        if js_code:
            compile_js(js_code, f"{workflow_id}:{node['name']}")
        if not str(node.get("type") or "").endswith(".graphql"):
            continue
        assert_true(node.get("onError") == "continueErrorOutput", f"{workflow_id}:{node['name']} must use continueErrorOutput")
        assert_true(node.get("retryOnFail") is True, f"{workflow_id}:{node['name']} must retry on fail")
        assert_true(int(node.get("maxTries") or 0) == 5, f"{workflow_id}:{node['name']} must use maxTries=5")
        query = json.dumps(params, ensure_ascii=False)
        for token in expectations.get(node["name"], []):
            assert_contains(query, token, f"{workflow_id}:{node['name']} missing token {token}")


def validate_contract_tokens(workflow_id: str, workflow: dict) -> None:
    text = json.dumps(workflow, ensure_ascii=False)
    for forbidden in ["parent_ids", "Faturas associadas", "Itens associados"]:
        assert_true(forbidden not in text, f"{workflow_id} contains forbidden token {forbidden}")
    if workflow_id == "KRYkEmSTkuVD8Nsn":
        for token in ["341313813", "itens_da_fatura", "invoice_already_exists"]:
            assert_contains(text, token, f"{workflow_id} missing token {token}")
    if workflow_id == "0nUKP2OUpRLgMSxq":
        for token in [
            "341306826",
            "341351580",
            "missing_required_nonzero_field",
            "existing_fin04_conflict",
            "blocked_missing_external_signal_onboarding_training",
            "blocked_missing_external_signal_setup_payment",
        ]:
            assert_contains(text, token, f"{workflow_id} missing token {token}")
    if workflow_id == "dC7sV4pLm9QxT2aK":
        for token in ["341351580", "existing_fin04_conflict", "no_items_ready_for_invoice", "Pode criar item FIN-04?"]:
            assert_contains(text, token, f"{workflow_id} missing token {token}")
    if workflow_id == "fN6rJ3wUz8YhL5cP":
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
            assert_contains(text, token, f"{workflow_id} missing token {token}")


def main() -> None:
    validate_index()
    for workflow_id, expected_name in EXPECTED.items():
        workflow = load_workflow(workflow_id)
        assert_true(workflow["name"] == expected_name, f"{workflow_id} has unexpected name {workflow['name']}")
        validate_graphql_nodes(workflow_id, workflow)
        validate_contract_tokens(workflow_id, workflow)
        if workflow_id == "sbjuPdz4ipegrKh2":
            validate_orchestrator(workflow)
    print("fin_monthly_workflows_ok")


if __name__ == "__main__":
    main()
