# AGENTS.md

## Scope

This repository stores the current FIN monthly billing workflow family for n8n plus the Pipefy evidence used to justify those workflows.

The current canonical implementation surface is the named 5-workflow suite under `data/raw/n8n/workflows/`.

## Canonical Files

- `data/raw/n8n/workflows/[FIN] 1.0 Orquestrar faturamento mensal.json`
- `data/raw/n8n/workflows/[FIN] 1.1 Preparar regras elegiveis.json`
- `data/raw/n8n/workflows/[FIN] 1.2 Preparar itens elegiveis da regra.json`
- `data/raw/n8n/workflows/[FIN] 1.3 Materializar itens FIN-04.json`
- `data/raw/n8n/workflows/[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02.json`
- `tools/validate_fin_monthly_workflows.py`
- `data/graph/nodes.jsonl`
- `data/graph/edges.jsonl`
- `data/graph/manifest.json`
- `data/index/pipe_field_to_refs.json`

## Source Of Truth Rules

- Edit the named workflow exports in `data/raw/n8n/workflows/` directly.
- Treat `data/graph/` and `data/index/` as read-only evidence for IDs, dependencies, and field usage.
- Anchor decisions to stable IDs when possible: pipe IDs, phase IDs, field IDs, and connector IDs.
- Prefer current workflow JSON plus validator behavior over older notes if they disagree.

## Workflow Invariants

- `competence_date` must remain the first day of the previous month.
- Eligibility must be based on `phase_id`, not on display labels.
- `itens_da_fatura` is the canonical connector. Do not reintroduce `Faturas associadas` or `Itens associados`.
- The flow is fail-closed. Do not reuse preexisting FIN-03 or FIN-04 cards when a logical conflict is found.
- FIN-03 dedupe key is `rule_id + competence_date`.
- FIN-04 dedupe key is `template_fin02_id + competence_date`.
- `continueErrorOutput`, retries, and explicit guard branches are part of the reliability contract around GraphQL nodes.
- Preserve pagination-context handling in prep nodes. The validator expects `linkedJson(...)`-based context restoration.
- Prefer native n8n nodes and explicit expressions over expanding `Code` usage unless native nodes cannot express the behavior safely.

## Confirmed IDs

- Pipes:
  - `FIN-01 = 306662125`
  - `FIN-02 = 306852209`
  - `FIN-03 = 306859731`
  - `FIN-04 = 306859915`
- Active phases used by the current flow:
  - `FIN-01 Ativo = 341313813`
  - `FIN-02 Ativo = 341306826`
  - `FIN-04 Ativo = 341351580`

## Data Table Contract

- n8n Data Tables here should use only `string`, `number`, and `dateTime`.
- Structured payloads such as summaries, details, variables, and responses should be serialized into string columns.
- The orchestrator now persists only one Data Table log stream:
  - `fin_billing_rule_log`
- The rule-log schema is one row per `rule_result`, with:
  - `run_id`, `workflow_execution_id`, `competence_date`
  - `rule_id`, `rule_title`
  - `status`, `reason`, `failure_count`
  - `failed_nodes`, `failed_reasons`, `pipefy_card_ids`
  - `invoice_id`, `created_item_ids_json`, `failures_json`
  - `started_at`, `finished_at`
- Current workspace mapping uses Data Table ID `5CPaGUez5Ex8tJHi` for `fin_billing_rule_log`; replace it only when importing into another n8n environment.

## Failure And Blocking Semantics

- Preserve these reason families when editing guards or error handling:
  - `invoice_already_exists`
  - `existing_fin04_conflict`
  - `orphan_fin04_exists_for_rule_competence`
  - `no_items_ready_for_invoice`
  - `missing_required_nonzero_field`
  - `failed_invoice_stage`
  - `blocked_missing_external_signal_onboarding_training`
  - `blocked_missing_external_signal_setup_payment`
- If FIN-04 creation fails for a rule, that rule must not create FIN-03.
- If FIN-03 creation fails after FIN-04 creation, future runs must block on the orphaned FIN-04 state instead of reusing those items.

## Temporary Value-Fill Rules

- The current design allows technical zero-fill only for the explicit base-value families:
  - `valor_de_base_brl`
  - `valor_de_base_usd`
  - `valor_base_saas_categoria_*`
- Any other required field missing in the active branch should fail closed with `missing_required_nonzero_field`.
- Treat this zero-fill behavior as temporary until the definitive base formulas exist.

## Known Gaps

- The external signals for `Treinamento de onboarding realizado` and `Finalizacao do pagamento de setup` are not fully described in this repo.
- Do not claim end-to-end automation for those branches unless the source checks are explicit in the workflow.

## Validation

Run this after workflow edits:

```bash
python3 tools/validate_fin_monthly_workflows.py
```

What it currently validates:

- expected workflow exports exist
- workflow names match file names
- GraphQL nodes keep retry and `continueErrorOutput` settings
- `jsCode` blocks compile with Node
- pagination-context recovery tokens are still present
- child-workflow payloads no longer emit `logs` in the `fin_v3` contract
- the orchestrator no longer passes `logs` into child workflow inputs
- the orchestrator persists only `fin_billing_rule_log`
- critical contract tokens remain in the correct workflow

There is currently no `tests/` directory on `main`. Do not assume old Python artifact tests still exist.

## Operational Notes

- The orchestrator keeps both `Cron (dias 1-5 07:00)` and `Manual Trigger`.
- `parcelas_pagas` only increments after a real FIN-03 exists and the FIN-04 item has been backfilled with `id_da_fatura`.
- When `parcelas_pagas == quantidade_de_parcelas_totais`, the FIN-02 parent item should move to `status = Inativo`.

## Historical Traps

- Older discussions and graph/index evidence may still mention workflow IDs such as `sbjuPdz4ipegrKh2` or `0nUKP2OUpRLgMSxq`. Those are historical/internal workflow IDs, not the current file paths to edit.
- The old 3-workflow root-level exports are not part of the current `main` checkout.
- `solicitacao.md` is not present in the current checkout.
- The removed planning/operation markdown docs were intentionally folded into this file and memory. Do not recreate them unless there is a concrete need for maintained operator docs.

## Practical Review Checklist

- Did the edit preserve ID-based routing and not switch back to name-based heuristics?
- Did the edit keep the fail-closed behavior for duplicates, orphans, and missing required inputs?
- Did the edit avoid introducing silent drops like `return []` on failure paths?
- Did the edit preserve the named 5-workflow contract instead of mixing in historical file layouts?
- Did `python3 tools/validate_fin_monthly_workflows.py` pass after the change?
