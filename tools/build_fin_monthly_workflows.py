#!/usr/bin/env python3
import copy
import datetime as dt
import json
import pathlib
import textwrap
import uuid


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = ROOT / "raw" / "n8n" / "workflows"
INDEX_PATH = ROOT / "raw" / "n8n" / "workflows.index.json"

ORCH_ID = "sbjuPdz4ipegrKh2"
PREP_RULES_ID = "KRYkEmSTkuVD8Nsn"
PREP_ITEMS_ID = "0nUKP2OUpRLgMSxq"
MAT_ITEMS_ID = "dC7sV4pLm9QxT2aK"
FINALIZE_ID = "fN6rJ3wUz8YhL5cP"

WF_NAMES = {
    ORCH_ID: "[FIN] 1 Orquestrar faturamento mensal",
    PREP_RULES_ID: "[FIN] 1.1 Preparar regras elegiveis",
    PREP_ITEMS_ID: "[FIN] 1.2 Preparar itens elegiveis da regra",
    MAT_ITEMS_ID: "[FIN] 1.3 Materializar itens FIN-04",
    FINALIZE_ID: "[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02",
}

PLACEHOLDER_TABLES = {
    "run": {
        "id": "PLACEHOLDER_FIN_BILLING_RUN_LOG",
        "name": "fin_billing_run_log",
    },
    "entity": {
        "id": "PLACEHOLDER_FIN_BILLING_ENTITY_LOG",
        "name": "fin_billing_entity_log",
    },
    "graphql": {
        "id": "PLACEHOLDER_FIN_BILLING_GRAPHQL_LOG",
        "name": "fin_billing_graphql_log",
    },
}


def now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


BUILD_TS = now_iso()


def load_workflow(workflow_id: str) -> dict:
    return json.loads((WORKFLOWS_DIR / f"{workflow_id}.json").read_text())


def try_load_workflow(workflow_id: str):
    path = WORKFLOWS_DIR / f"{workflow_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


OLD_ORCH = load_workflow(ORCH_ID)
OLD_PREP = load_workflow(PREP_RULES_ID)
OLD_PROC = load_workflow(PREP_ITEMS_ID)
OLD_MAT = try_load_workflow(MAT_ITEMS_ID)
OLD_FINAL = try_load_workflow(FINALIZE_ID)


def node_by_name(workflow: dict, node_name: str) -> dict:
    for node in workflow["nodes"]:
        if node["name"] == node_name:
            return copy.deepcopy(node)
    raise KeyError(f"Node not found: {node_name}")


def node_by_any_name(workflow: dict, *node_names: str) -> dict:
    for node_name in node_names:
        try:
            return node_by_name(workflow, node_name)
        except KeyError:
            continue
    raise KeyError(f"Node not found. Tried: {', '.join(node_names)}")


def node_by_type_suffix(workflow: dict, type_suffix: str) -> dict:
    for node in workflow["nodes"]:
        if str(node.get("type") or "").endswith(type_suffix):
            return copy.deepcopy(node)
    raise KeyError(f"Node with type suffix not found: {type_suffix}")


def node_from_sources(*node_names: str, type_suffix: str = None) -> dict:
    sources = [wf for wf in [OLD_ORCH, OLD_PREP, OLD_PROC, OLD_MAT, OLD_FINAL] if wf]
    for workflow in sources:
        if node_names:
            try:
                return node_by_any_name(workflow, *node_names)
            except KeyError:
                pass
        if type_suffix:
            try:
                return node_by_type_suffix(workflow, type_suffix)
            except KeyError:
                pass
    raise KeyError(f"Node not found in any source. names={node_names}, type_suffix={type_suffix}")


TEMPLATES = {
    "cron": node_from_sources("Cron (dias 1-5 07:00)"),
    "manual": node_from_sources("Manual Trigger"),
    "exec_wf": node_from_sources("Preparação: regras elegíveis", "Preparar regras elegiveis"),
    "split_batches": node_from_sources("Loop regras"),
    "if": node_from_sources("Run: tem erros?"),
    "set": node_from_sources("Run: Success output"),
    "stop": node_from_sources("Run: Stop and Error"),
    "code": node_from_sources("Init Run"),
    "exec_trigger": node_from_sources("Execute Workflow Trigger"),
    "graphql": node_from_sources("03.3 | FIN-04 Criar item", "FIN-04 Criar item", type_suffix=".graphql"),
    "merge": node_from_sources("02.4 | Merge retorno template", "Merge retorno template", type_suffix=".merge"),
    "split_out": node_from_sources("02.2 | Split Out template IDs", "Split Out template IDs", type_suffix=".splitOut"),
    "data_table": node_from_sources(type_suffix=".dataTable"),
}


def make_uuid() -> str:
    return str(uuid.uuid4())


def clone_node(template_key: str, name: str, position, parameters=None, **updates) -> dict:
    node = copy.deepcopy(TEMPLATES[template_key])
    node["id"] = make_uuid()
    node["name"] = name
    node["position"] = list(position)
    if parameters is not None:
        node["parameters"] = parameters
    for key, value in updates.items():
        if value is None:
            node.pop(key, None)
        else:
            node[key] = value
    return node


def workflow_shared(existing: dict, workflow_id: str) -> list:
    shared = copy.deepcopy(existing["shared"])
    for item in shared:
        if isinstance(item, dict):
            item["workflowId"] = workflow_id
            item["createdAt"] = BUILD_TS
            item["updatedAt"] = BUILD_TS
    return shared


def workflow_base(existing: dict, workflow_id: str, name: str, tags=None) -> dict:
    base = {
        "active": False,
        "connections": {},
        "createdAt": BUILD_TS,
        "id": workflow_id,
        "isArchived": False,
        "meta": None,
        "name": name,
        "nodes": [],
        "pinData": {},
        "settings": copy.deepcopy(existing["settings"]),
        "shared": workflow_shared(existing, workflow_id),
        "staticData": None,
        "tags": copy.deepcopy(tags if tags is not None else existing.get("tags", [])),
        "triggerCount": 0,
        "updatedAt": BUILD_TS,
        "versionId": str(uuid.uuid4()),
    }
    return base


def simplify_shared_for_index(shared: list) -> list:
    out = []
    for item in shared:
        project_id = None
        if isinstance(item.get("project"), dict):
            project_id = item["project"].get("id")
        elif item.get("projectId") is not None:
            project_id = item.get("projectId")
        out.append(
            {
                "createdAt": item.get("createdAt", BUILD_TS),
                "projectId": project_id,
                "role": item.get("role", "workflow:owner"),
                "updatedAt": item.get("updatedAt", BUILD_TS),
                "workflowId": item.get("workflowId"),
            }
        )
    return out


COMMON_JS = textwrap.dedent(
    """
    const nowIso = new Date().toISOString();
    const asObj = (v) => (v && typeof v === 'object' && !Array.isArray(v)) ? v : {};
    const clone = (v) => JSON.parse(JSON.stringify(v));
    const clean = (v) => String(v ?? '').trim();
    const isBlank = (v) => {
      const s = clean(v).toLowerCase();
      return s === '' || s === 'null' || s === 'undefined' || s === 'nan';
    };
    const isIsoDate = (v) => /^\\d{4}-\\d{2}-\\d{2}$/.test(clean(v).slice(0, 10));
    const collectIds = (...values) => {
      const out = [];
      const visit = (value) => {
        if (value === undefined || value === null) return;
        if (Array.isArray(value)) {
          for (const item of value) visit(item);
          return;
        }
        const s = String(value).trim();
        if (s) out.push(s);
      };
      for (const value of values) visit(value);
      return Array.from(new Set(out));
    };
    const pickScalar = (f) => {
      if (!f) return null;
      const rv = f.report_value;
      return (rv !== undefined && rv !== null && String(rv).trim() !== '') ? rv : f.value;
    };
    const fieldMap = (fields) => {
      const out = {};
      for (const f of (fields || [])) {
        const fid = f && f.field ? f.field.id : null;
        if (fid) out[fid] = f;
      }
      return out;
    };
    const parseNum = (v) => {
      if (v === undefined || v === null) return 0;
      if (typeof v === 'number' && Number.isFinite(v)) return v;
      let s = String(v).trim();
      if (!s) return 0;
      s = s.replace(/[^0-9,.-]/g, '');
      if (!s) return 0;
      if (s.includes(',') && s.includes('.')) {
        if (s.lastIndexOf(',') > s.lastIndexOf('.')) {
          s = s.replace(/\\./g, '').replace(',', '.');
        } else {
          s = s.replace(/,/g, '');
        }
      } else if (s.includes(',')) {
        s = s.replace(',', '.');
      }
      const n = Number(s);
      return Number.isFinite(n) ? n : 0;
    };
    const extractIds = (f) => {
      if (!f) return [];
      const out = [];
      const push = (x) => {
        if (x === undefined || x === null) return;
        if (typeof x === 'object') {
          if (x.id != null) out.push(String(x.id));
          else if (x.value != null) out.push(String(x.value));
          else if (x.label != null) out.push(String(x.label));
        } else {
          out.push(String(x));
        }
      };
      if (Array.isArray(f.array_value)) {
        for (const x of f.array_value) push(x);
      } else {
        const v = pickScalar(f);
        if (Array.isArray(v)) {
          for (const x of v) push(x);
        } else if (typeof v === 'string') {
          const matches = v.match(/[0-9]+/g);
          if (matches) {
            for (const x of matches) out.push(String(x));
          } else if (v.trim()) {
            out.push(v.trim());
          }
        } else if (v != null) {
          push(v);
        }
      }
      return Array.from(new Set(out.filter(Boolean)));
    };
    const extractList = (f) => {
      if (!f) return [];
      const out = [];
      const push = (x) => {
        if (x === undefined || x === null) return;
        const s = String(x).trim();
        if (s) out.push(s);
      };
      if (Array.isArray(f.array_value)) {
        for (const x of f.array_value) push(x);
      } else {
        const v = pickScalar(f);
        if (Array.isArray(v)) {
          for (const x of v) push(x);
        } else if (typeof v === 'string') {
          const s = v.trim();
          if (s.startsWith('[')) {
            try {
              const parsed = JSON.parse(s);
              if (Array.isArray(parsed)) {
                for (const x of parsed) push(x);
              } else {
                push(parsed);
              }
            } catch (_err) {
              for (const x of s.split(',')) push(x);
            }
          } else {
            for (const x of s.split(',')) push(x);
          }
        } else if (v != null) {
          push(v);
        }
      }
      return Array.from(new Set(out));
    };
    const toId = (v) => {
      if (v == null) return null;
      const s = String(v).trim();
      const match = s.match(/[0-9]+/);
      return match ? match[0] : (s || null);
    };
    const jsonString = (v) => {
      try { return JSON.stringify(v ?? null); } catch (_err) { return 'null'; }
    };
    const timeWithOffset = (ymd) => clean(ymd) ? `${clean(ymd)}T00:00:00-03:00` : null;
    const ensureLogState = (ctx) => {
      ctx.failures = Array.isArray(ctx.failures) ? ctx.failures : [];
      ctx.entityLogs = Array.isArray(ctx.entityLogs) ? ctx.entityLogs : [];
      ctx.graphqlLogs = Array.isArray(ctx.graphqlLogs) ? ctx.graphqlLogs : [];
      ctx.workflowLogs = Array.isArray(ctx.workflowLogs) ? ctx.workflowLogs : [];
      ctx.metrics = asObj(ctx.metrics);
      ctx.metrics.graphqlCallsTotal = Number(ctx.metrics.graphqlCallsTotal || 0);
      ctx.metrics.graphqlErrorsTotal = Number(ctx.metrics.graphqlErrorsTotal || 0);
      return ctx;
    };
    const addFailure = (ctx, node, reason, ...ids) => {
      ctx.failures.push({
        node: String(node || 'unknown'),
        reason: String(reason || 'unexpected_payload_shape'),
        pipefyCardIds: collectIds(...ids),
      });
    };
    const nextGraphqlIndex = (ctx) => {
      ctx.metrics.graphqlCallsTotal = Number(ctx.metrics.graphqlCallsTotal || 0) + 1;
      return ctx.metrics.graphqlCallsTotal;
    };
    const addGraphqlErrorLog = (ctx, payload) => {
      ctx.metrics.graphqlErrorsTotal = Number(ctx.metrics.graphqlErrorsTotal || 0) + 1;
      ctx.graphqlLogs.push({
        run_id: ctx.run?.runId ?? null,
        workflow_execution_id: ctx.trace?.workflowExecutionId ?? String($execution.id ?? 'manual'),
        parent_execution_id: ctx.trace?.parentExecutionId ?? null,
        workflow_name: ctx.trace?.workflowName ?? null,
        node_name: payload.node_name,
        operation_name: payload.operation_name,
        pipe_id: payload.pipe_id ?? null,
        phase_id: payload.phase_id ?? null,
        card_id: payload.card_id ?? null,
        entity_type: payload.entity_type ?? null,
        entity_id: payload.entity_id ?? null,
        request_fingerprint: payload.request_fingerprint ?? null,
        graphql_call_index: payload.graphql_call_index ?? null,
        requested_at: payload.requested_at ?? nowIso,
        responded_at: nowIso,
        duration_ms: payload.duration_ms ?? null,
        status: payload.status ?? 'error',
        error_code: payload.error_code ?? null,
        error_message: payload.error_message ?? null,
        query_excerpt: payload.query_excerpt ?? null,
        variables_json: payload.variables_json ?? 'null',
        response_json: payload.response_json ?? 'null',
      });
    };
    const addEntityLog = (ctx, payload) => {
      ctx.entityLogs.push({
        run_id: ctx.run?.runId ?? null,
        workflow_execution_id: ctx.trace?.workflowExecutionId ?? String($execution.id ?? 'manual'),
        parent_execution_id: ctx.trace?.parentExecutionId ?? null,
        workflow_name: ctx.trace?.workflowName ?? null,
        node_name: payload.node_name,
        entity_type: payload.entity_type,
        entity_id: payload.entity_id ?? null,
        rule_id: payload.rule_id ?? ctx.rule?.ruleId ?? null,
        template_item_id: payload.template_item_id ?? null,
        fin02_parent_item_id: payload.fin02_parent_item_id ?? null,
        invoice_id: payload.invoice_id ?? null,
        fin04_item_id: payload.fin04_item_id ?? null,
        competence_date: timeWithOffset(ctx.run?.competenceYMD),
        action: payload.action,
        status: payload.status,
        failure_reason: payload.failure_reason ?? null,
        dedupe_key: payload.dedupe_key ?? null,
        graphql_calls_delta: Number(payload.graphql_calls_delta || 0),
        started_at: payload.started_at ?? ctx.startedAt ?? nowIso,
        finished_at: payload.finished_at ?? nowIso,
        details_json: payload.details_json ?? 'null',
      });
    };
    const addWorkflowLog = (ctx, payload) => {
      ctx.workflowLogs.push({
        run_id: ctx.run?.runId ?? null,
        workflow_execution_id: ctx.trace?.workflowExecutionId ?? String($execution.id ?? 'manual'),
        parent_execution_id: ctx.trace?.parentExecutionId ?? null,
        workflow_name: ctx.trace?.workflowName ?? null,
        trigger_type: ctx.run?.triggerType ?? null,
        competence_date: timeWithOffset(ctx.run?.competenceYMD),
        started_at: payload.started_at ?? ctx.startedAt ?? nowIso,
        finished_at: payload.finished_at ?? nowIso,
        status: payload.status,
        failure_reason: payload.failure_reason ?? null,
        graphql_calls_total: Number(ctx.metrics?.graphqlCallsTotal || 0),
        rules_found: Number(payload.rules_found || 0),
        rules_eligible: Number(payload.rules_eligible || 0),
        rules_processed: Number(payload.rules_processed || 0),
        rules_failed: Number(payload.rules_failed || 0),
        items_created: Number(payload.items_created || 0),
        items_updated: Number(payload.items_updated || 0),
        duplicate_items_blocked: Number(payload.duplicate_items_blocked || 0),
        invoices_created: Number(payload.invoices_created || 0),
        duplicate_invoices_blocked: Number(payload.duplicate_invoices_blocked || 0),
        installments_incremented: Number(payload.installments_incremented || 0),
        parent_items_inactivated: Number(payload.parent_items_inactivated || 0),
        summary_json: payload.summary_json ?? 'null',
      });
    };
    """
).strip()


def js(body: str) -> str:
    return textwrap.dedent(COMMON_JS + "\n" + body).strip()


ORCH_INIT_CODE = js(
    """
    const tz = 'America/Sao_Paulo';
    const now = new Date();
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: tz,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).formatToParts(now);
    const dateParts = {};
    for (const part of parts) dateParts[part.type] = part.value;
    const year = Number(dateParts.year);
    const month = Number(dateParts.month);
    const day = Number(dateParts.day);
    let compYear = year;
    let compMonth = month - 1;
    if (compMonth === 0) {
      compMonth = 12;
      compYear -= 1;
    }
    const mm = String(compMonth).padStart(2, '0');
    const competenciaYMD = `${compYear}-${mm}-01`;
    const daysInCompMonth = new Date(Date.UTC(compYear, compMonth, 0)).getUTCDate();
    const compEnd = `${compYear}-${mm}-${String(daysInCompMonth).padStart(2, '0')}`;
    const triggerType = $input.context?.sourceData?.previousNode == null
      ? (($node['Cron (dias 1-5 07:00)']?.isExecuted === true) ? 'cron' : 'manual')
      : 'manual';
    const runId = `fin-billing:${String($execution.id ?? 'manual')}:${competenciaYMD}`;
    const ctx = ensureLogState({
      contractVersion: 'fin_v3',
      startedAt: nowIso,
      trace: {
        parentExecutionId: null,
        workflowExecutionId: String($execution.id ?? 'manual'),
        workflowName: '[FIN] 1 Orquestrar faturamento mensal',
      },
      run: {
        runId,
        executionId: String($execution.id ?? 'manual'),
        triggerType,
        timezone: tz,
        todayDay: day,
        competenceYMD: competenciaYMD,
        competenceStart: competenciaYMD,
        competenceEnd: compEnd,
        competenceDateTime: timeWithOffset(competenciaYMD),
      },
      metrics: {
        rulesFound: 0,
        rulesEligible: 0,
        rulesProcessed: 0,
        rulesFailed: 0,
        itemsCreated: 0,
        itemsUpdated: 0,
        duplicateItemsBlocked: 0,
        invoicesCreated: 0,
        duplicateInvoicesBlocked: 0,
        installmentsIncremented: 0,
        parentItemsInactivated: 0,
      },
      results: [],
    });
    return [{ json: { runContext: ctx.run, trace: ctx.trace, orchestratorContext: ctx } }];
    """
)


ORCH_ACCUMULATE_CODE = js(
    """
    const input = $input.all().map((item) => asObj(item.json));
    const sd = $getWorkflowStaticData('global');
    const executionId = String($execution.id ?? 'manual');
    sd.finBillingAcc = asObj(sd.finBillingAcc);
    if (!sd.finBillingAcc[executionId]) {
      sd.finBillingAcc[executionId] = {
        ctx: ensureLogState({
          startedAt: nowIso,
          trace: {
            parentExecutionId: null,
            workflowExecutionId: executionId,
            workflowName: '[FIN] 1 Orquestrar faturamento mensal',
          },
          run: {},
          metrics: {},
          failures: [],
          results: [],
          entityLogs: [],
          graphqlLogs: [],
          workflowLogs: [],
        }),
      };
    }
    const acc = sd.finBillingAcc[executionId].ctx;
    if (!acc.run?.runId) {
      const first = input.find((x) => asObj(x.run).runId || asObj(x.summary).run_id || asObj(x.trace).workflowExecutionId);
      if (first) {
        if (asObj(first.run).runId) {
          acc.run = clone(first.run);
        } else if (asObj(first.summary).runId) {
          acc.run.runId = first.summary.runId;
        }
      }
    }
    for (const item of input) {
      const logs = asObj(item.logs);
      if (Array.isArray(logs.workflowLogs)) acc.workflowLogs.push(...logs.workflowLogs);
      if (Array.isArray(logs.entityLogs)) acc.entityLogs.push(...logs.entityLogs);
      if (Array.isArray(logs.graphqlLogs)) acc.graphqlLogs.push(...logs.graphqlLogs);
      const failures = Array.isArray(item.failures) ? item.failures : (Array.isArray(asObj(item.result).failures) ? item.result.failures : []);
      if (failures.length > 0) acc.failures.push(...failures);
      const result = asObj(item.result);
      const metrics = asObj(result.metricsDelta);
      for (const [key, value] of Object.entries(metrics)) {
        const n = Number(value);
        if (Number.isFinite(n)) acc.metrics[key] = Number(acc.metrics[key] || 0) + n;
      }
      if (item.itemType === 'prep_summary') {
        const prep = asObj(result.prepContext);
        acc.metrics.rulesFound = Number(prep.rulesFound || prep.rulesTotal || acc.metrics.rulesFound || 0);
        acc.metrics.rulesEligible = Number(prep.rulesEligible || prep.eligibleCount || acc.metrics.rulesEligible || 0);
        continue;
      }
      if (item.itemType !== 'rule_result') continue;
      acc.results.push({
        ruleId: asObj(item.rule).ruleId ?? null,
        status: String(result.status || 'failed'),
        reason: String(result.reason || 'unexpected_payload_shape'),
        invoiceId: result.invoiceId ?? null,
        createdItemIds: Array.isArray(result.createdItemIds) ? result.createdItemIds : [],
      });
      acc.metrics.rulesProcessed = Number(acc.metrics.rulesProcessed || 0) + 1;
      if (String(result.status || '') === 'failed') {
        acc.metrics.rulesFailed = Number(acc.metrics.rulesFailed || 0) + 1;
      }
    }
    return $input.all();
    """
)


ORCH_FINALIZE_CODE = js(
    """
    const sd = $getWorkflowStaticData('global');
    const executionId = String($execution.id ?? 'manual');
    const state = asObj(asObj(sd.finBillingAcc)[executionId]);
    const ctx = ensureLogState(asObj(state.ctx));
    const failures = Array.isArray(ctx.failures) ? ctx.failures : [];
    const finishedAt = nowIso;
    const resultList = Array.isArray(ctx.results) ? ctx.results : [];
    const processed = resultList.filter((x) => x.status === 'processed').length;
    const skipped = resultList.filter((x) => x.status === 'skipped').length;
    const failed = resultList.filter((x) => x.status === 'failed').length;
    const summary = {
      runId: ctx.run?.runId ?? null,
      competenceYMD: ctx.run?.competenceYMD ?? null,
      startedAt: ctx.startedAt ?? null,
      finishedAt,
      processedRules: processed,
      skippedRules: skipped,
      failedRules: failed,
      failureCount: failures.length,
      graphqlCallsTotal: Number(ctx.metrics?.graphqlCallsTotal || 0),
      metrics: ctx.metrics,
      results: resultList,
      firstFailure: failures[0] ?? null,
    };
    addWorkflowLog(ctx, {
      status: failed > 0 || failures.length > 0 ? 'failed' : 'success',
      failure_reason: failures[0]?.reason ?? null,
      rules_found: Number(ctx.metrics?.rulesFound || 0),
      rules_eligible: Number(ctx.metrics?.rulesEligible || 0),
      rules_processed: Number(ctx.metrics?.rulesProcessed || 0),
      rules_failed: Number(ctx.metrics?.rulesFailed || 0),
      items_created: Number(ctx.metrics?.createItems || 0),
      items_updated: Number((ctx.metrics?.updateItemInvoiceLinks || 0) + (ctx.metrics?.parentItemsInactivated || 0) + (ctx.metrics?.updateParcelasPagas || 0)),
      duplicate_items_blocked: Number(ctx.metrics?.duplicateItemsBlocked || 0),
      invoices_created: Number(ctx.metrics?.createInvoices || 0),
      duplicate_invoices_blocked: Number(ctx.metrics?.duplicateInvoicesBlocked || 0),
      installments_incremented: Number(ctx.metrics?.updateParcelasPagas || 0),
      parent_items_inactivated: Number(ctx.metrics?.parentItemsInactivated || 0),
      summary_json: jsonString(summary),
      started_at: ctx.startedAt ?? null,
      finished_at: finishedAt,
    });
    if (sd.finBillingAcc && sd.finBillingAcc[executionId]) delete sd.finBillingAcc[executionId];
    return [{
      json: {
        ok: failed === 0 && failures.length === 0,
        summary,
        run: {
          failures,
          results: resultList,
          metrics: ctx.metrics,
        },
        workflowLogRows: ctx.workflowLogs,
        entityLogRows: ctx.entityLogs,
        graphqlLogRows: ctx.graphqlLogs,
      }
    }];
    """
)


PREP_RULES_INIT_CODE = js(
    """
    const input = asObj($json);
    const incomingRun = clone(asObj(input.runContext));
    const incomingTrace = clone(asObj(input.trace));
    const ctx = ensureLogState({
      startedAt: nowIso,
      trace: {
        parentExecutionId: incomingTrace.workflowExecutionId ?? incomingTrace.parentExecutionId ?? null,
        workflowExecutionId: String($execution.id ?? 'manual'),
        workflowName: '[FIN] 1.1 Preparar regras elegiveis',
      },
      run: {
        runId: incomingRun.runId ?? null,
        triggerType: incomingRun.triggerType ?? null,
        todayDay: Number(incomingRun.todayDay || 0),
        competenceYMD: incomingRun.competenceYMD ?? null,
        competenceDateTime: incomingRun.competenceDateTime ?? timeWithOffset(incomingRun.competenceYMD),
      },
      metrics: {
        graphqlCallsTotal: 0,
      },
      rules: [],
      existingRuleIds: {},
    });
    return [{ json: { ctx, after: null, prefetchAfter: null } }];
    """
)


PREP_RULES_PARSE_PAGE_CODE = js(
    """
    const input = asObj($json);
    const ctx = ensureLogState(clone(asObj($node['Init Run'].json.ctx)));
    const callIndex = nextGraphqlIndex(ctx);
    if (input.error) {
      addFailure(ctx, 'FIN-01: Listar regras Ativo (page)', 'rules_query_transport_error');
      addGraphqlErrorLog(ctx, {
        node_name: 'FIN-01: Listar regras Ativo (page)',
        operation_name: 'list_fin01_rules_by_phase',
        pipe_id: '306662125',
        phase_id: '341313813',
        entity_type: 'rule',
        graphql_call_index: callIndex,
        error_message: clean(input.error?.message || 'transport_error'),
        query_excerpt: 'phase(id) { cards { edges { node { id title fields } } } }',
        response_json: jsonString(input),
      });
      return [{ json: { ctx, after: null, hasNext: false, fatal: true } }];
    }
    const errors = Array.isArray(input.errors) ? input.errors : [];
    if (errors.length > 0) {
      addFailure(ctx, 'FIN-01: Listar regras Ativo (page)', 'rules_query_logical_error');
      addGraphqlErrorLog(ctx, {
        node_name: 'FIN-01: Listar regras Ativo (page)',
        operation_name: 'list_fin01_rules_by_phase',
        pipe_id: '306662125',
        phase_id: '341313813',
        entity_type: 'rule',
        graphql_call_index: callIndex,
        error_message: clean(errors[0]?.message || 'graphql_error'),
        query_excerpt: 'phase(id) { cards { edges { node { id title fields } } } }',
        response_json: jsonString(input),
      });
      return [{ json: { ctx, after: null, hasNext: false, fatal: true } }];
    }
    const cards = input.data?.phase?.cards;
    const edges = Array.isArray(cards?.edges) ? cards.edges : [];
    for (const edge of edges) {
      if (edge?.node?.id) ctx.rules.push(edge.node);
    }
    return [{
      json: {
        ctx,
        after: cards?.pageInfo?.endCursor ?? null,
        hasNext: cards?.pageInfo?.hasNextPage === true,
        fatal: false,
      }
    }];
    """
)


PREP_RULES_PARSE_PREFETCH_CODE = js(
    """
    const input = asObj($json);
    const ctx = ensureLogState(clone(asObj($node['Parse rules page'].json.ctx)));
    const callIndex = nextGraphqlIndex(ctx);
    if (input.error) {
      addFailure(ctx, 'FIN-03: Prefetch faturas (competência)', 'invoice_prefetch_transport_error');
      addGraphqlErrorLog(ctx, {
        node_name: 'FIN-03: Prefetch faturas (competência)',
        operation_name: 'prefetch_fin03_by_competence',
        pipe_id: '306859731',
        entity_type: 'invoice',
        graphql_call_index: callIndex,
        error_message: clean(input.error?.message || 'transport_error'),
        query_excerpt: 'findCards(pipeId, search: competence)',
        response_json: jsonString(input),
      });
      return [{ json: { ctx, prefetchAfter: null, hasNext: false, fatal: true } }];
    }
    const errors = Array.isArray(input.errors) ? input.errors : [];
    if (errors.length > 0) {
      addFailure(ctx, 'FIN-03: Prefetch faturas (competência)', 'invoice_prefetch_logical_error');
      addGraphqlErrorLog(ctx, {
        node_name: 'FIN-03: Prefetch faturas (competência)',
        operation_name: 'prefetch_fin03_by_competence',
        pipe_id: '306859731',
        entity_type: 'invoice',
        graphql_call_index: callIndex,
        error_message: clean(errors[0]?.message || 'graphql_error'),
        query_excerpt: 'findCards(pipeId, search: competence)',
        response_json: jsonString(input),
      });
      return [{ json: { ctx, prefetchAfter: null, hasNext: false, fatal: true } }];
    }
    const find = input.data?.findCards;
    const edges = Array.isArray(find?.edges) ? find.edges : [];
    for (const edge of edges) {
      const node = edge?.node;
      if (!node) continue;
      const fields = fieldMap(node.fields || []);
      const ruleId = toId(pickScalar(fields['id_da_regra_de_faturamento']));
      const comp = clean(pickScalar(fields['data_de_compet_ncia'])).slice(0, 10);
      if (ruleId && comp === clean(ctx.run.competenceYMD)) {
        ctx.existingRuleIds[ruleId] = String(node.id);
      }
    }
    return [{
      json: {
        ctx,
        prefetchAfter: find?.pageInfo?.endCursor ?? null,
        hasNext: find?.pageInfo?.hasNextPage === true,
        fatal: false,
      }
    }];
    """
)


PREP_RULES_BUILD_CODE = js(
    """
    const pageCtx = ensureLogState(clone(asObj($node['Parse prefetch'].json.ctx)));
    const rules = Array.isArray(pageCtx.rules) ? pageCtx.rules : [];
    const eligibleRules = [];
    const results = [];
    const normalizeBillingDay = (value) => {
      if (value === undefined || value === null) return null;
      const match = String(value).match(/[0-9]+/g);
      if (!match) return null;
      const n = Number(match[match.length - 1]);
      return Number.isInteger(n) && n >= 1 && n <= 31 ? n : null;
    };
    for (const rule of rules) {
      const fm = fieldMap(rule.fields || []);
      const ruleId = clean(rule.id);
      const billingDay = normalizeBillingDay(pickScalar(fm['dia_de_gera_o_da_fatura']));
      if (!ruleId) {
        addEntityLog(pageCtx, {
          node_name: 'Build regras elegiveis',
          entity_type: 'rule',
          entity_id: null,
          action: 'classify_rule',
          status: 'failed',
          failure_reason: 'missing_rule_id',
          dedupe_key: null,
        });
        results.push({
          contractVersion: 'fin_v3',
          itemType: 'rule_result',
          trace: pageCtx.trace,
          run: pageCtx.run,
          rule: { ruleId: null, ruleTitle: clean(rule.title), prefetchDecision: 'invalid_rule' },
          result: {
            status: 'failed',
            reason: 'missing_rule_id',
            invoiceId: null,
            existingInvoiceId: null,
            createdItemIds: [],
            totals: { brl: 0, usd: 0 },
            metricsDelta: {},
            failures: [{ node: 'Build regras elegiveis', reason: 'missing_rule_id', pipefyCardIds: [] }],
          },
          logs: { workflowLogs: [], entityLogs: clone(pageCtx.entityLogs), graphqlLogs: clone(pageCtx.graphqlLogs) },
          failures: [{ node: 'Build regras elegiveis', reason: 'missing_rule_id', pipefyCardIds: [] }],
        });
        pageCtx.entityLogs = [];
        pageCtx.graphqlLogs = [];
        continue;
      }
      if (billingDay !== Number(pageCtx.run.todayDay || 0)) {
        addEntityLog(pageCtx, {
          node_name: 'Build regras elegiveis',
          entity_type: 'rule',
          entity_id: ruleId,
          action: 'classify_rule',
          status: 'skipped',
          failure_reason: 'billing_day_mismatch',
          dedupe_key: `${ruleId}|${pageCtx.run.competenceYMD}`,
          details_json: jsonString({ billingDay, expectedDay: pageCtx.run.todayDay }),
        });
        results.push({
          contractVersion: 'fin_v3',
          itemType: 'rule_result',
          trace: pageCtx.trace,
          run: pageCtx.run,
          rule: { ruleId, ruleTitle: clean(rule.title), prefetchDecision: 'billing_day_mismatch' },
          result: {
            status: 'skipped',
            reason: 'billing_day_mismatch',
            invoiceId: null,
            existingInvoiceId: null,
            createdItemIds: [],
            totals: { brl: 0, usd: 0 },
            metricsDelta: {},
            failures: [],
          },
          logs: { workflowLogs: [], entityLogs: clone(pageCtx.entityLogs), graphqlLogs: clone(pageCtx.graphqlLogs) },
          failures: [],
        });
        pageCtx.entityLogs = [];
        pageCtx.graphqlLogs = [];
        continue;
      }
      if (pageCtx.existingRuleIds[ruleId]) {
        addEntityLog(pageCtx, {
          node_name: 'Build regras elegiveis',
          entity_type: 'rule',
          entity_id: ruleId,
          action: 'dedupe_rule',
          status: 'blocked',
          failure_reason: 'invoice_already_exists',
          invoice_id: pageCtx.existingRuleIds[ruleId],
          dedupe_key: `${ruleId}|${pageCtx.run.competenceYMD}`,
        });
        results.push({
          contractVersion: 'fin_v3',
          itemType: 'rule_result',
          trace: pageCtx.trace,
          run: pageCtx.run,
          rule: { ruleId, ruleTitle: clean(rule.title), prefetchDecision: 'invoice_already_exists' },
          result: {
            status: 'failed',
            reason: 'invoice_already_exists',
            invoiceId: pageCtx.existingRuleIds[ruleId],
            existingInvoiceId: pageCtx.existingRuleIds[ruleId],
            createdItemIds: [],
            totals: { brl: 0, usd: 0 },
            metricsDelta: { duplicateInvoicesBlocked: 1 },
            failures: [{ node: 'Build regras elegiveis', reason: 'invoice_already_exists', pipefyCardIds: [ruleId, pageCtx.existingRuleIds[ruleId]] }],
          },
          logs: { workflowLogs: [], entityLogs: clone(pageCtx.entityLogs), graphqlLogs: clone(pageCtx.graphqlLogs) },
          failures: [{ node: 'Build regras elegiveis', reason: 'invoice_already_exists', pipefyCardIds: [ruleId, pageCtx.existingRuleIds[ruleId]] }],
        });
        pageCtx.entityLogs = [];
        pageCtx.graphqlLogs = [];
        continue;
      }
      eligibleRules.push({
        contractVersion: 'fin_v3',
        itemType: 'rule_input',
        trace: pageCtx.trace,
        run: pageCtx.run,
        rule: {
          ruleId,
          ruleTitle: clean(rule.title),
          itemTemplateIds: extractIds(fm['itens_da_fatura']),
          ruleFields: fm,
          prefetchDecision: 'new_rule_candidate',
        },
        logs: { workflowLogs: [], entityLogs: [], graphqlLogs: [] },
      });
    }
    addWorkflowLog(pageCtx, {
      status: pageCtx.failures.length > 0 ? 'failed' : 'success',
      failure_reason: pageCtx.failures[0]?.reason ?? null,
      rules_found: rules.length,
      rules_eligible: eligibleRules.length,
      rules_processed: 0,
      rules_failed: results.filter((x) => x.itemType === 'rule_result' && x.result.status === 'failed').length,
      duplicate_invoices_blocked: results.filter((x) => x.result.reason === 'invoice_already_exists').length,
      summary_json: jsonString({ rulesFound: rules.length, rulesEligible: eligibleRules.length }),
    });
    const prepSummary = {
      contractVersion: 'fin_v3',
      itemType: 'prep_summary',
      trace: pageCtx.trace,
      run: pageCtx.run,
      result: {
        status: pageCtx.failures.length > 0 ? 'failed' : 'processed',
        reason: pageCtx.failures.length > 0 ? pageCtx.failures[0].reason : 'prep_rules_completed',
        prepContext: {
          rulesFound: rules.length,
          rulesEligible: eligibleRules.length,
          rulesFailed: results.filter((x) => x.itemType === 'rule_result' && x.result.status === 'failed').length,
        },
        metricsDelta: {
          rulesFound: rules.length,
          rulesEligible: eligibleRules.length,
        },
        failures: pageCtx.failures,
      },
      logs: {
        workflowLogs: pageCtx.workflowLogs,
        entityLogs: pageCtx.entityLogs,
        graphqlLogs: pageCtx.graphqlLogs,
      },
      failures: pageCtx.failures,
    };
    return [{ json: prepSummary }, ...eligibleRules, ...results.map((x) => ({ json: x }))];
    """
)


PREP_ITEMS_INIT_CODE = js(
    """
    const input = asObj($json);
    const run = clone(asObj(input.run));
    const traceIn = clone(asObj(input.trace));
    const rule = clone(asObj(input.rule));
    const ctx = ensureLogState({
      startedAt: nowIso,
      trace: {
        parentExecutionId: traceIn.workflowExecutionId ?? traceIn.parentExecutionId ?? null,
        workflowExecutionId: String($execution.id ?? 'manual'),
        workflowName: '[FIN] 1.2 Preparar itens elegiveis da regra',
      },
      run,
      rule,
      metrics: {},
      config: {
        fin02ActivePhaseId: '341306826',
        fin04PipeId: '306859915',
        fin04PhaseId: '341351580',
        rulePhaseId: '341313813',
      },
      plannedItems: [],
      invoiceInputBase: null,
      terminal: { done: false, status: null, reason: null },
    });
    if (String(input.contractVersion || '') !== 'fin_v3' || String(input.itemType || '') !== 'rule_input') {
      addFailure(ctx, 'Init contexto item_plan', 'unexpected_payload_shape', rule.ruleId ?? null);
      ctx.terminal = { done: true, status: 'failed', reason: 'unexpected_payload_shape' };
    }
    if (!ctx.terminal.done && (!rule.ruleId || !Array.isArray(rule.itemTemplateIds))) {
      addFailure(ctx, 'Init contexto item_plan', 'invalid_rule_input', rule.ruleId ?? null);
      ctx.terminal = { done: true, status: 'failed', reason: 'invalid_rule_input' };
    }
    if (!ctx.terminal.done && rule.itemTemplateIds.length === 0) {
      addFailure(ctx, 'Init contexto item_plan', 'no_items_ready_for_invoice', rule.ruleId ?? null);
      ctx.terminal = { done: true, status: 'failed', reason: 'no_items_ready_for_invoice' };
    }
    return [{ json: ctx }];
    """
)


PREP_ITEMS_DIRECT_RESULT_CODE = js(
    """
    const ctx = ensureLogState(clone(asObj($json)));
    const reason = ctx.terminal?.reason || (ctx.failures[0]?.reason || 'no_items_ready_for_invoice');
    addWorkflowLog(ctx, {
      status: 'failed',
      failure_reason: reason,
      rules_processed: 1,
      rules_failed: 1,
      summary_json: jsonString({ directResult: true, reason }),
    });
    return [{
      json: {
        contractVersion: 'fin_v3',
        itemType: 'rule_result',
        trace: ctx.trace,
        run: ctx.run,
        rule: { ruleId: ctx.rule.ruleId, ruleTitle: ctx.rule.ruleTitle, prefetchDecision: ctx.rule.prefetchDecision ?? null },
        result: {
          status: 'failed',
          reason,
          invoiceId: null,
          existingInvoiceId: null,
          createdItemIds: [],
          totals: { brl: 0, usd: 0 },
          metricsDelta: {},
          failures: ctx.failures,
        },
        logs: { workflowLogs: ctx.workflowLogs, entityLogs: ctx.entityLogs, graphqlLogs: ctx.graphqlLogs },
        failures: ctx.failures,
      }
    }];
    """
)


PREP_ITEMS_CONSOLIDATE_CODE = js(
    """
    const merged = $input.all().map((item) => asObj(item.json));
    if (merged.length === 0) {
      throw new Error('No merged template items to consolidate');
    }
    const baseCtx = ensureLogState(clone(asObj(merged[0].ctx || merged[0])));
    const ctx = baseCtx;
    const ZERO_WHITELIST = new Set([
      'valor_de_base_brl',
      'valor_de_base_usd',
      'valor_base_saas_categoria_a_b_brl',
      'valor_base_saas_categoria_a_b_usd',
      'valor_base_saas_categoria_e_s_brl',
      'valor_base_saas_categoria_e_s_usd',
      'valor_base_saas_categoria_espa_os_brl',
      'valor_base_saas_categoria_espa_os_usd',
      'valor_base_saas_categoria_hospedagem_brl',
      'valor_base_saas_categoria_hospedagem_usd',
      'valor_base_saas_categoria_outros_brl',
      'valor_base_saas_categoria_outros_usd',
    ]);
    const DISCOUNT_TYPE_PERCENT = '1260337139';
    const DISCOUNT_TYPE_LOCAL_TAX = '1260337296';
    const BASE_PERCENTUAL_SAAS = '1268890278';
    const COND_ONBOARDING = '1258585767';
    const COND_SPECIFIC_DATE = '1258585802';
    const COND_SETUP = '1258585836';
    const targetMap = {
      produto: 'produto',
      tipo_de_item: 'tipo_de_item',
      modelo_de_cobran_a: 'modelo_de_cobran_a',
      moeda_da_fatura: 'moeda_da_fatura',
      base_do_c_lculo_percentual: 'base_do_c_lculo_percentual',
      percentual_a_ser_aplicado_na_base: 'percentual_a_ser_aplicado_na_base',
      unidade_de_cobran_a: 'unidade_de_cobran_a',
      quantidade: 'quantidade',
      afiliados_que_comp_em_a_cobran_a: 'afiliados_que_comp_em_a_cobran_a',
      id_backoffice_do_afiliado: 'id_backoffice_do_afiliado',
      h_algum_desconto_nesse_item: 'h_algum_desconto_nesse_item',
      tipo_de_desconto_no_item_1: 'tipo_de_desconto_no_item',
      percentual_de_desconto_a_ser_aplicado: 'percentual_de_desconto_a_ser_aplicado',
      valor_nominal_a_ser_descontado_brl: 'valor_nominal_a_ser_descontado_brl',
      valor_nominal_a_ser_descontado_usd: 'valor_nominal_a_ser_descontado_usd',
      categorias_do_saas_isentas_ou_com_desconto: 'categorias_do_saas_isentas_ou_com_desconto',
      observa_es_sobre_descontos_e_isen_es: 'observa_es_sobre_descontos_e_isen_es',
      tipo_de_desconto_a_b_1: 'tipo_de_desconto_a_b_1',
      tipo_de_desconto_e_s: 'tipo_de_desconto_e_s',
      tipo_de_desconto_hospedagem: 'tipo_de_desconto_hospedagem',
      tipo_de_desconto_espa_os: 'tipo_de_desconto_espa_os',
      tipo_de_desconto_outros: 'tipo_de_desconto_outros',
      desconto_no_valor_vari_vel_do_saas_para_a_b: 'desconto_no_valor_vari_vel_do_saas_para_a_b',
      valor_do_imposto_a_ser_isento_a_b: 'valor_do_imposto_a_ser_isento_a_b',
      desconto_no_valor_vari_vel_do_saas_para_e_s: 'desconto_no_valor_vari_vel_do_saas_para_e_s',
      valor_do_imposto_a_ser_isento_e_s: 'valor_do_imposto_a_ser_isento_e_s',
      desconto_no_valor_vari_vel_do_saas_para_hospedagem: 'desconto_no_valor_vari_vel_do_saas_para_hospedagem',
      valor_do_imposto_a_ser_isento_hospedagem: 'valor_do_imposto_a_ser_isento_hospedagem',
      desconto_no_valor_vari_vel_do_saas_para_espa_os: 'desconto_no_valor_vari_vel_do_saas_para_espa_os',
      valor_do_imposto_a_ser_isento_espa_os: 'valor_do_imposto_a_ser_isento_espa_os',
      desconto_no_valor_vari_vel_do_saas_para_outros: 'desconto_no_valor_vari_vel_do_saas_para_outros',
      valor_do_imposto_a_ser_isento_outros: 'valor_do_imposto_a_ser_isento_outros',
      valor_unit_rio_brl: 'valor_unit_rio_brl',
      valor_unit_rio_usd: 'valor_unit_rio_usd',
      subtotal_do_item_brl: 'valor_total_do_item_brl',
      subtotal_do_item_usd: 'valor_total_do_item_usd',
      valor_total_do_item_com_descontos_brl: 'valor_total_do_item_com_descontos_brl',
      valor_total_do_item_com_descontos_usd: 'valor_total_do_item_com_descontos_usd',
    };
    const addMappedValue = (fieldAttrs, target, value) => {
      if (value === undefined || value === null) return;
      if (Array.isArray(value) && value.length === 0) return;
      if (typeof value === 'string' && !value.trim()) return;
      fieldAttrs.push({ field_id: target, field_value: value });
    };
    let duplicateItemsBlocked = 0;
    let totalBrl = 0;
    let totalUsd = 0;
    for (const item of merged) {
      const templateId = clean(item.templateItemId || item.templateItem?.templateItemId || item.ctx?.templateItemId || '');
      const fetchCallIndex = nextGraphqlIndex(ctx);
      const conflictCallIndex = nextGraphqlIndex(ctx);
      const fetchResp = asObj(item.templateResponse || item);
      const conflictResp = asObj(item.conflictResponse || item);
      const fetchErrors = Array.isArray(fetchResp.errors) ? fetchResp.errors : [];
      const conflictErrors = Array.isArray(conflictResp.fin04ConflictErrors) ? conflictResp.fin04ConflictErrors : (Array.isArray(conflictResp.errors) ? conflictResp.errors : []);
      if (fetchResp.templateFetchError || fetchErrors.length > 0 || !fetchResp.data?.card?.id) {
        addGraphqlErrorLog(ctx, {
          node_name: 'FIN-02 Buscar item template',
          operation_name: 'fetch_fin02_template_item',
          pipe_id: '306852209',
          phase_id: '341306826',
          entity_type: 'template_item',
          entity_id: templateId || null,
          card_id: templateId || null,
          graphql_call_index: fetchCallIndex,
          error_message: clean(fetchResp.error?.message || fetchErrors[0]?.message || 'template_not_found_or_transport_error'),
          query_excerpt: 'query GetTemplateItem',
          response_json: jsonString(fetchResp),
        });
        addFailure(ctx, 'FIN-02 Buscar item template', 'fetch_template_item_error', ctx.rule.ruleId, templateId || null);
        addEntityLog(ctx, {
          node_name: 'Consolidar elegibilidade e planos',
          entity_type: 'template_item',
          entity_id: templateId || null,
          template_item_id: templateId || null,
          action: 'prepare_template_item',
          status: 'failed',
          failure_reason: 'fetch_template_item_error',
          dedupe_key: `${templateId}|${ctx.run.competenceYMD}`,
        });
        continue;
      }
      if (item.fin04ConflictError || conflictErrors.length > 0) {
        addGraphqlErrorLog(ctx, {
          node_name: 'FIN-04 Verificar conflitos por competencia',
          operation_name: 'recheck_fin04_conflict_by_template',
          pipe_id: '306859915',
          phase_id: '341351580',
          entity_type: 'fin04_item',
          entity_id: templateId || null,
          card_id: templateId || null,
          graphql_call_index: conflictCallIndex,
          error_message: clean(conflictResp.error?.message || conflictErrors[0]?.message || 'graphql_error'),
          query_excerpt: 'findCards(pipeId, search: template_fin02_id)',
          response_json: jsonString(conflictResp),
        });
        addFailure(ctx, 'FIN-04 Verificar conflitos por competencia', 'conflict_fin04_query_error', ctx.rule.ruleId, templateId || null);
        addEntityLog(ctx, {
          node_name: 'Consolidar elegibilidade e planos',
          entity_type: 'template_item',
          entity_id: templateId || null,
          template_item_id: templateId || null,
          action: 'check_fin04_conflict',
          status: 'failed',
          failure_reason: 'conflict_fin04_query_error',
          dedupe_key: `${templateId}|${ctx.run.competenceYMD}`,
        });
        continue;
      }
      if (conflictResp.data?.findCards?.pageInfo?.hasNextPage === true) {
        addFailure(ctx, 'FIN-04 Verificar conflitos por competencia', 'conflict_fin04_query_requires_pagination', ctx.rule.ruleId, templateId || null);
        addEntityLog(ctx, {
          node_name: 'Consolidar elegibilidade e planos',
          entity_type: 'template_item',
          entity_id: templateId || null,
          template_item_id: templateId || null,
          action: 'check_fin04_conflict',
          status: 'failed',
          failure_reason: 'conflict_fin04_query_requires_pagination',
          dedupe_key: `${templateId}|${ctx.run.competenceYMD}`,
        });
        continue;
      }
      const card = fetchResp.data.card;
      const phaseId = clean(card.current_phase?.id);
      const fm = fieldMap(card.fields || []);
      if (phaseId !== clean(ctx.config.fin02ActivePhaseId)) {
        addEntityLog(ctx, {
          node_name: 'Consolidar elegibilidade e planos',
          entity_type: 'template_item',
          entity_id: templateId,
          template_item_id: templateId,
          action: 'evaluate_template_item',
          status: 'skipped',
          failure_reason: 'inactive_template_phase',
          dedupe_key: `${templateId}|${ctx.run.competenceYMD}`,
        });
        continue;
      }
      const conflictEdges = Array.isArray(conflictResp.data?.findCards?.edges) ? conflictResp.data.findCards.edges : [];
      let conflict = null;
      for (const edge of conflictEdges) {
        const node = edge?.node;
        if (!node) continue;
        const fields = fieldMap(node.fields || []);
        const sameTemplate = clean(pickScalar(fields['id_do_item_usado_como_template_fin_02'])) === templateId;
        const sameCompetence = clean(pickScalar(fields['data_de_compet_ncia'])).slice(0, 10) === clean(ctx.run.competenceYMD);
        if (sameTemplate && sameCompetence) {
          conflict = {
            id: clean(node.id),
            invoiceId: clean(pickScalar(fields['id_da_fatura']) || ''),
          };
          break;
        }
      }
      if (conflict) {
        duplicateItemsBlocked += 1;
        const conflictReason = conflict.invoiceId ? 'existing_fin04_conflict' : 'orphan_fin04_exists_for_rule_competence';
        addFailure(ctx, 'Consolidar elegibilidade e planos', conflictReason, ctx.rule.ruleId, templateId, conflict.id);
        addEntityLog(ctx, {
          node_name: 'Consolidar elegibilidade e planos',
          entity_type: 'template_item',
          entity_id: templateId,
          template_item_id: templateId,
          fin04_item_id: conflict.id,
          invoice_id: conflict.invoiceId || null,
          action: 'check_fin04_conflict',
          status: 'blocked',
          failure_reason: conflictReason,
          dedupe_key: `${templateId}|${ctx.run.competenceYMD}`,
          graphql_calls_delta: 1,
        });
        continue;
      }
      const conditions = extractIds(fm['condi_es_exigidas_para_iniciar_o_faturamento_do_item']);
      const specificStart = clean(pickScalar(fm['data_de_in_cio_do_faturamento'])).slice(0, 10);
      const specificEnd = clean(pickScalar(fm['data_de_fim_do_faturamento'])).slice(0, 10);
      const blockingReasons = [];
      if (conditions.includes(COND_SPECIFIC_DATE)) {
        if (!specificStart || specificStart > clean(ctx.run.competenceEnd)) {
          blockingReasons.push('specific_date_not_ready');
        }
        if (specificEnd && specificEnd < clean(ctx.run.competenceYMD)) {
          blockingReasons.push('specific_date_finished');
        }
      }
      if (conditions.includes(COND_ONBOARDING)) {
        blockingReasons.push('blocked_missing_external_signal_onboarding_training');
      }
      if (conditions.includes(COND_SETUP)) {
        blockingReasons.push('blocked_missing_external_signal_setup_payment');
      }
      if (blockingReasons.length > 0) {
        addFailure(ctx, 'Consolidar elegibilidade e planos', blockingReasons[0], ctx.rule.ruleId, templateId);
        addEntityLog(ctx, {
          node_name: 'Consolidar elegibilidade e planos',
          entity_type: 'template_item',
          entity_id: templateId,
          template_item_id: templateId,
          action: 'evaluate_start_conditions',
          status: 'blocked',
          failure_reason: blockingReasons[0],
          dedupe_key: `${templateId}|${ctx.run.competenceYMD}`,
          details_json: jsonString({ conditions, specificStart, specificEnd, blockingReasons }),
        });
        continue;
      }
      const currency = clean(pickScalar(fm['moeda_da_fatura']));
      const model = clean(pickScalar(fm['modelo_de_cobran_a']));
      const type = clean(pickScalar(fm['tipo_de_item']));
      const hasDiscount = clean(pickScalar(fm['h_algum_desconto_nesse_item']));
      const baseCalc = extractIds(fm['base_do_c_lculo_percentual']);
      const selectedDiscountTypes = extractIds(fm['tipo_de_desconto_no_item_1']);
      const selectedCategories = extractList(fm['categorias_do_saas_isentas_ou_com_desconto']);
      const discountTypeByCategory = {
        'A&B': extractIds(fm['tipo_de_desconto_a_b_1']),
        'E&S': extractIds(fm['tipo_de_desconto_e_s']),
        'Hospedagem': extractIds(fm['tipo_de_desconto_hospedagem']),
        'Espaços': extractIds(fm['tipo_de_desconto_espa_os']),
        'Outros': extractIds(fm['tipo_de_desconto_outros']),
      };
      const visibleRequired = new Set(['tipo_de_item', 'moeda_da_fatura', 'modelo_de_cobran_a', 'h_algum_desconto_nesse_item']);
      if (type === 'Produto') visibleRequired.add('produto');
      if (model === 'Percentual sobre base') {
        visibleRequired.add('base_do_c_lculo_percentual');
        visibleRequired.add('percentual_a_ser_aplicado_na_base');
        if (currency === 'BRL - R$') visibleRequired.add('valor_de_base_brl');
        if (currency === 'USD - $') visibleRequired.add('valor_de_base_usd');
        if (baseCalc.includes(BASE_PERCENTUAL_SAAS)) {
          if (currency === 'BRL - R$') {
            visibleRequired.add('valor_base_saas_categoria_a_b_brl');
            visibleRequired.add('valor_base_saas_categoria_e_s_brl');
            visibleRequired.add('valor_base_saas_categoria_espa_os_brl');
            visibleRequired.add('valor_base_saas_categoria_hospedagem_brl');
            visibleRequired.add('valor_base_saas_categoria_outros_brl');
          }
          if (currency === 'USD - $') {
            visibleRequired.add('valor_base_saas_categoria_a_b_usd');
            visibleRequired.add('valor_base_saas_categoria_e_s_usd');
            visibleRequired.add('valor_base_saas_categoria_espa_os_usd');
            visibleRequired.add('valor_base_saas_categoria_hospedagem_usd');
            visibleRequired.add('valor_base_saas_categoria_outros_usd');
          }
        }
      }
      if (model === 'Por unidade') {
        visibleRequired.add('unidade_de_cobran_a');
        visibleRequired.add('quantidade');
        if (currency === 'BRL - R$') visibleRequired.add('valor_unit_rio_brl');
        if (currency === 'USD - $') visibleRequired.add('valor_unit_rio_usd');
        if (clean(pickScalar(fm['unidade_de_cobran_a'])) === 'Afiliados') visibleRequired.add('afiliados_que_comp_em_a_cobran_a');
      }
      if (model === 'Fixo' || model === 'Por unidade') {
        if (currency === 'BRL - R$') visibleRequired.add('subtotal_do_item_brl');
        if (currency === 'USD - $') visibleRequired.add('subtotal_do_item_usd');
      }
      if (type === 'Ajustes de fatura') {
        if (currency === 'BRL - R$') visibleRequired.add('valor_total_do_item_com_descontos_brl');
        if (currency === 'USD - $') visibleRequired.add('valor_total_do_item_com_descontos_usd');
      }
      if (hasDiscount === 'Sim') {
        visibleRequired.add('tipo_de_desconto_no_item_1');
        if (selectedDiscountTypes.includes('Nominal')) {
          if (currency === 'BRL - R$') visibleRequired.add('valor_nominal_a_ser_descontado_brl');
          if (currency === 'USD - $') visibleRequired.add('valor_nominal_a_ser_descontado_usd');
        }
        if (selectedDiscountTypes.includes('Percentual')) {
          visibleRequired.add('percentual_de_desconto_a_ser_aplicado');
        }
        if (selectedDiscountTypes.includes('Categorias de propostas') && model === 'Percentual sobre base' && baseCalc.includes(BASE_PERCENTUAL_SAAS)) {
          visibleRequired.add('categorias_do_saas_isentas_ou_com_desconto');
          for (const category of selectedCategories) {
            if (category === 'A&B') visibleRequired.add('tipo_de_desconto_a_b_1');
            if (category === 'E&S') visibleRequired.add('tipo_de_desconto_e_s');
            if (category === 'Hospedagem') visibleRequired.add('tipo_de_desconto_hospedagem');
            if (category === 'Espaços') visibleRequired.add('tipo_de_desconto_espa_os');
            if (category === 'Outros') visibleRequired.add('tipo_de_desconto_outros');
            const chosen = discountTypeByCategory[category] || [];
            const targetSuffix = category === 'A&B' ? 'a_b' : category === 'E&S' ? 'e_s' : category === 'Hospedagem' ? 'hospedagem' : category === 'Espaços' ? 'espa_os' : 'outros';
            if (chosen.includes(DISCOUNT_TYPE_PERCENT)) visibleRequired.add(`desconto_no_valor_vari_vel_do_saas_para_${targetSuffix}`);
            if (chosen.includes(DISCOUNT_TYPE_LOCAL_TAX)) visibleRequired.add(`valor_do_imposto_a_ser_isento_${targetSuffix}`);
          }
        }
      }
      const fieldAttrs = [
        { field_id: 'data_de_compet_ncia', field_value: clean(ctx.run.competenceYMD) },
        { field_id: 'id_do_item_usado_como_template_fin_02', field_value: templateId },
      ];
      const validationErrors = [];
      for (const [source, target] of Object.entries(targetMap)) {
        const sourceField = fm[source];
        let value = null;
        if (sourceField?.field?.type === 'connector' || sourceField?.field?.type?.includes('checklist')) {
          value = sourceField?.field?.type === 'connector' ? extractIds(sourceField) : extractList(sourceField);
        } else {
          value = pickScalar(sourceField);
        }
        if ((value === null || value === undefined || (typeof value === 'string' && !value.trim()) || (Array.isArray(value) && value.length === 0)) && visibleRequired.has(source)) {
          if (ZERO_WHITELIST.has(target)) {
            value = 0;
          } else {
            validationErrors.push({ field_id: target, source_field_id: source });
          }
        }
        addMappedValue(fieldAttrs, target, value);
      }
      if (validationErrors.length > 0) {
        addFailure(ctx, 'Consolidar elegibilidade e planos', 'missing_required_nonzero_field', ctx.rule.ruleId, templateId);
        addEntityLog(ctx, {
          node_name: 'Consolidar elegibilidade e planos',
          entity_type: 'template_item',
          entity_id: templateId,
          template_item_id: templateId,
          action: 'validate_fin04_payload',
          status: 'failed',
          failure_reason: 'missing_required_nonzero_field',
          dedupe_key: `${templateId}|${ctx.run.competenceYMD}`,
          details_json: jsonString({ validationErrors, visibleRequired: Array.from(visibleRequired).sort() }),
        });
        continue;
      }
      const totalItemBrl = parseNum(pickScalar(fm['valor_total_do_item_com_descontos_brl']) ?? pickScalar(fm['subtotal_do_item_brl']));
      const totalItemUsd = parseNum(pickScalar(fm['valor_total_do_item_com_descontos_usd']) ?? pickScalar(fm['subtotal_do_item_usd']));
      totalBrl += totalItemBrl;
      totalUsd += totalItemUsd;
      const periodicidade = clean(pickScalar(fm['periodicidade']));
      const parcelasPagas = Number(parseNum(pickScalar(fm['parcelas_pagas'])));
      const parcelasTotais = Number(parseNum(pickScalar(fm['quantidade_de_parcelas_totais'])));
      const planned = {
        templateItemId: templateId,
        dedupeKey: `${templateId}|${ctx.run.competenceYMD}`,
        totalItemBrl,
        totalItemUsd,
        periodicidade,
        parcelasPagas,
        parcelasTotais,
        shouldUpdateParcelas: periodicidade === 'Parcelado',
        newParcelasPagas: periodicidade === 'Parcelado' ? parcelasPagas + 1 : null,
        shouldInactivateParent: periodicidade === 'Parcelado' && parcelasTotais > 0 && (parcelasPagas + 1) >= parcelasTotais,
        createInput: {
          pipe_id: Number(ctx.config.fin04PipeId),
          phase_id: Number(ctx.config.fin04PhaseId),
          title: `FIN-04 ${clean(ctx.run.competenceYMD).slice(0, 7)} - ${templateId}`,
          fields_attributes: fieldAttrs,
        },
      };
      ctx.plannedItems.push(planned);
      addEntityLog(ctx, {
        node_name: 'Consolidar elegibilidade e planos',
        entity_type: 'template_item',
        entity_id: templateId,
        template_item_id: templateId,
        action: 'prepare_template_item',
        status: 'success',
        dedupe_key: planned.dedupeKey,
        details_json: jsonString({ periodicidade, model, currency }),
      });
    }
    const rf = fieldMap(Object.values(asObj(ctx.rule.ruleFields)));
    const invoiceFields = [];
    const pushInvoiceField = (field_id, value) => {
      if (value === undefined || value === null) return;
      if (Array.isArray(value) && value.length === 0) return;
      if (typeof value === 'string' && !value.trim()) return;
      invoiceFields.push({ field_id, field_value: value });
    };
    pushInvoiceField('clientes_na_fatura', extractIds(rf['clientes_na_fatura']));
    pushInvoiceField('contatos_que_receber_o_a_fatura', extractIds(rf['contatos_que_receber_o_a_fatura']));
    pushInvoiceField('identificador_fiscal_na_fatura', clean(pickScalar(rf['identificador_fiscal_na_fatura'])));
    pushInvoiceField('tipo_de_cliente_final', clean(pickScalar(rf['tipo_de_cliente_final'])));
    pushInvoiceField('rede_na_fatura', extractIds(rf['rede_na_fatura']));
    pushInvoiceField('forma_de_pagamento', extractList(rf['forma_de_pagamento']));
    pushInvoiceField('moeda_do_teto_de_faturamento', extractList(rf['moeda_do_teto_de_faturamento']));
    pushInvoiceField('teto_de_faturamento_brl', parseNum(pickScalar(rf['teto_de_faturamento_brl'])));
    pushInvoiceField('teto_de_faturamento_usd', parseNum(pickScalar(rf['teto_de_faturamento_usd'])));
    pushInvoiceField('id_da_regra_de_faturamento', clean(ctx.rule.ruleId));
    pushInvoiceField('data_de_compet_ncia', clean(ctx.run.competenceYMD));
    pushInvoiceField('valor_total_da_fatura_brl', totalBrl);
    pushInvoiceField('valor_total_da_fatura_usd', totalUsd);
    ctx.invoiceInputBase = {
      pipe_id: 306859731,
      title: `FIN-03 ${clean(ctx.run.competenceYMD).slice(0, 7)} - Regra ${clean(ctx.rule.ruleId)}`,
      fields_attributes: invoiceFields,
    };
    ctx.metrics.duplicateItemsBlocked = duplicateItemsBlocked;
    if (ctx.plannedItems.length === 0) {
      const reason = ctx.failures[0]?.reason || 'no_items_ready_for_invoice';
      ctx.terminal = { done: true, status: 'failed', reason };
    }
    addWorkflowLog(ctx, {
      status: ctx.terminal.done ? 'failed' : 'success',
      failure_reason: ctx.terminal.done ? ctx.terminal.reason : null,
      rules_processed: 1,
      rules_failed: ctx.terminal.done ? 1 : 0,
      duplicate_items_blocked: duplicateItemsBlocked,
      summary_json: jsonString({ plannedItems: ctx.plannedItems.length }),
    });
    const payload = ctx.terminal.done
      ? {
          contractVersion: 'fin_v3',
          itemType: 'rule_result',
          trace: ctx.trace,
          run: ctx.run,
          rule: { ruleId: ctx.rule.ruleId, ruleTitle: ctx.rule.ruleTitle, prefetchDecision: ctx.rule.prefetchDecision ?? null },
          result: {
            status: 'failed',
            reason: ctx.terminal.reason,
            invoiceId: null,
            existingInvoiceId: null,
            createdItemIds: [],
            totals: { brl: totalBrl, usd: totalUsd },
            metricsDelta: { duplicateItemsBlocked },
            failures: ctx.failures,
          },
          logs: { workflowLogs: ctx.workflowLogs, entityLogs: ctx.entityLogs, graphqlLogs: ctx.graphqlLogs },
          failures: ctx.failures,
        }
      : {
          contractVersion: 'fin_v3',
          itemType: 'item_plan',
          trace: ctx.trace,
          run: ctx.run,
          rule: ctx.rule,
          itemPlan: {
            plannedItems: ctx.plannedItems,
            totals: { brl: totalBrl, usd: totalUsd },
            invoiceInputBase: ctx.invoiceInputBase,
          },
          logs: { workflowLogs: ctx.workflowLogs, entityLogs: ctx.entityLogs, graphqlLogs: ctx.graphqlLogs },
          failures: ctx.failures,
        };
    return [{ json: payload }];
    """
)


MAT_ITEMS_INIT_CODE = js(
    """
    const input = asObj($json);
    const plan = clone(asObj(input.itemPlan));
    const run = clone(asObj(input.run));
    const traceIn = clone(asObj(input.trace));
    const rule = clone(asObj(input.rule));
    const ctx = ensureLogState({
      startedAt: nowIso,
      trace: {
        parentExecutionId: traceIn.workflowExecutionId ?? traceIn.parentExecutionId ?? null,
        workflowExecutionId: String($execution.id ?? 'manual'),
        workflowName: '[FIN] 1.3 Materializar itens FIN-04',
      },
      run,
      rule,
      plan,
      metrics: {},
      createdItemIds: [],
      parcelUpdates: [],
      terminal: { done: false, status: null, reason: null },
    });
    ctx.workflowLogs = Array.isArray(input.logs?.workflowLogs) ? input.logs.workflowLogs : [];
    ctx.entityLogs = Array.isArray(input.logs?.entityLogs) ? input.logs.entityLogs : [];
    ctx.graphqlLogs = Array.isArray(input.logs?.graphqlLogs) ? input.logs.graphqlLogs : [];
    ctx.failures = Array.isArray(input.failures) ? input.failures : [];
    if (String(input.contractVersion || '') !== 'fin_v3' || String(input.itemType || '') !== 'item_plan') {
      addFailure(ctx, 'Init materializacao', 'unexpected_payload_shape', rule.ruleId ?? null);
      ctx.terminal = { done: true, status: 'failed', reason: 'unexpected_payload_shape' };
    }
    if (!ctx.terminal.done && !Array.isArray(plan.plannedItems)) {
      addFailure(ctx, 'Init materializacao', 'invalid_item_plan', rule.ruleId ?? null);
      ctx.terminal = { done: true, status: 'failed', reason: 'invalid_item_plan' };
    }
    if (!ctx.terminal.done && plan.plannedItems.length === 0) {
      addFailure(ctx, 'Init materializacao', 'no_items_ready_for_invoice', rule.ruleId ?? null);
      ctx.terminal = { done: true, status: 'failed', reason: 'no_items_ready_for_invoice' };
    }
    return [{ json: ctx }];
    """
)


MAT_ITEMS_PARSE_RECHECK_CODE = js(
    """
    const merged = asObj($json);
    const ctx = ensureLogState(clone(asObj(merged.ctx || merged)));
    const response = asObj(merged.recheckResponse || merged);
    const planned = asObj(merged.plannedItem || {});
    const templateItemId = clean(planned.templateItemId);
    const dedupeKey = clean(planned.dedupeKey);
    const callIndex = nextGraphqlIndex(ctx);
    const out = { ...merged, ctx, recheckGraphqlCallIndex: callIndex, canCreateItem: false };
    if (ctx.terminal?.done === true) return [{ json: out }];
    if (response.error) {
      addGraphqlErrorLog(ctx, {
        node_name: 'FIN-04 Recheck conflito antes de criar',
        operation_name: 'recheck_fin04_conflict_by_template',
        pipe_id: '306859915',
        phase_id: '341351580',
        entity_type: 'fin04_item',
        entity_id: templateItemId || null,
        card_id: templateItemId || null,
        graphql_call_index: callIndex,
        error_message: clean(response.error?.message || 'transport_error'),
        query_excerpt: 'findCards(pipeId, search: template_fin02_id)',
        response_json: jsonString(response),
      });
      out.validationErrorReason = 'conflict_fin04_query_error';
      return [{ json: out }];
    }
    const errors = Array.isArray(response.errors) ? response.errors : [];
    if (errors.length > 0) {
      addGraphqlErrorLog(ctx, {
        node_name: 'FIN-04 Recheck conflito antes de criar',
        operation_name: 'recheck_fin04_conflict_by_template',
        pipe_id: '306859915',
        phase_id: '341351580',
        entity_type: 'fin04_item',
        entity_id: templateItemId || null,
        card_id: templateItemId || null,
        graphql_call_index: callIndex,
        error_message: clean(errors[0]?.message || 'graphql_error'),
        query_excerpt: 'findCards(pipeId, search: template_fin02_id)',
        response_json: jsonString(response),
      });
      out.validationErrorReason = 'conflict_fin04_query_error';
      return [{ json: out }];
    }
    if (response.data?.findCards?.pageInfo?.hasNextPage === true) {
      out.validationErrorReason = 'conflict_fin04_query_requires_pagination';
      return [{ json: out }];
    }
    const edges = Array.isArray(response.data?.findCards?.edges) ? response.data.findCards.edges : [];
    for (const edge of edges) {
      const node = edge?.node;
      if (!node) continue;
      const fm = fieldMap(node.fields || []);
      const sameTemplate = clean(pickScalar(fm['id_do_item_usado_como_template_fin_02'])) === templateItemId;
      const sameCompetence = clean(pickScalar(fm['data_de_compet_ncia'])).slice(0, 10) === clean(ctx.run.competenceYMD);
      if (!sameTemplate || !sameCompetence) continue;
      out.recheckConflictId = clean(node.id);
      out.recheckInvoiceId = clean(pickScalar(fm['id_da_fatura']) || '');
      return [{ json: out }];
    }
    out.canCreateItem = true;
    out.createGraphqlCallIndex = nextGraphqlIndex(ctx);
    out.dedupeKey = dedupeKey || null;
    return [{ json: out }];
    """
)


MAT_ITEMS_DIRECT_RESULT_CODE = js(
    """
    const ctx = ensureLogState(clone(asObj($json)));
    const reason = ctx.terminal?.reason || (ctx.failures[0]?.reason || 'no_items_ready_for_invoice');
    addWorkflowLog(ctx, {
      status: 'failed',
      failure_reason: reason,
      rules_processed: 1,
      rules_failed: 1,
      items_created: Array.isArray(ctx.createdItemIds) ? ctx.createdItemIds.length : 0,
      summary_json: jsonString({ directResult: true, reason }),
    });
    return [{
      json: {
        contractVersion: 'fin_v3',
        itemType: 'rule_result',
        trace: ctx.trace,
        run: ctx.run,
        rule: { ruleId: ctx.rule.ruleId, ruleTitle: ctx.rule.ruleTitle, prefetchDecision: ctx.rule.prefetchDecision ?? null },
        result: {
          status: 'failed',
          reason,
          invoiceId: null,
          existingInvoiceId: null,
          createdItemIds: Array.isArray(ctx.createdItemIds) ? ctx.createdItemIds : [],
          totals: asObj(ctx.plan?.totals),
          metricsDelta: { createItems: Array.isArray(ctx.createdItemIds) ? ctx.createdItemIds.length : 0 },
          failures: ctx.failures,
        },
        logs: { workflowLogs: ctx.workflowLogs, entityLogs: ctx.entityLogs, graphqlLogs: ctx.graphqlLogs },
        failures: ctx.failures,
      }
    }];
    """
)


MAT_ITEMS_CONSOLIDATE_CODE = js(
    """
    const items = $input.all().map((item) => asObj(item.json));
    if (items.length === 0) throw new Error('No materialization items received');
    const ctx = ensureLogState(clone(asObj(items[0].ctx || items[0])));
    let anyFailure = false;
    for (const item of items) {
      const planned = asObj(item.plannedItem || item.createReq || {});
      const dedupeKey = clean(planned.dedupeKey);
      const templateItemId = clean(planned.templateItemId);
      if (item.recheckConflictId) {
        anyFailure = true;
        const reason = clean(item.recheckInvoiceId) ? 'existing_fin04_conflict' : 'orphan_fin04_exists_for_rule_competence';
        addFailure(ctx, 'Recheck FIN-04 antes de criar', reason, ctx.rule.ruleId, templateItemId, item.recheckConflictId);
        addEntityLog(ctx, {
          node_name: 'Consolidar materializacao',
          entity_type: 'template_item',
          entity_id: templateItemId,
          template_item_id: templateItemId,
          fin04_item_id: item.recheckConflictId,
          action: 'recheck_before_create',
          status: 'blocked',
          failure_reason: reason,
          dedupe_key: dedupeKey,
        });
        continue;
      }
      if (item.validationErrorReason) {
        anyFailure = true;
        addFailure(ctx, 'Recheck FIN-04 antes de criar', item.validationErrorReason, ctx.rule.ruleId, templateItemId);
        addEntityLog(ctx, {
          node_name: 'Consolidar materializacao',
          entity_type: 'template_item',
          entity_id: templateItemId,
          template_item_id: templateItemId,
          action: 'validate_before_create',
          status: 'failed',
          failure_reason: item.validationErrorReason,
          dedupe_key: dedupeKey,
        });
        continue;
      }
      if (item.error) {
        anyFailure = true;
        addFailure(ctx, 'FIN-04 Criar item', 'create_item_transport_error', ctx.rule.ruleId, templateItemId);
        addGraphqlErrorLog(ctx, {
          node_name: 'FIN-04 Criar item',
          operation_name: 'create_fin04_item',
          pipe_id: '306859915',
          phase_id: '341351580',
          entity_type: 'fin04_item',
          entity_id: templateItemId,
          graphql_call_index: Number(item.createGraphqlCallIndex || 0),
          error_message: clean(item.error?.message || 'transport_error'),
          query_excerpt: 'mutation CreateFin04',
          response_json: jsonString(item),
        });
        addEntityLog(ctx, {
          node_name: 'Consolidar materializacao',
          entity_type: 'fin04_item',
          entity_id: templateItemId,
          template_item_id: templateItemId,
          action: 'create_item',
          status: 'failed',
          failure_reason: 'create_item_transport_error',
          dedupe_key: dedupeKey,
          graphql_calls_delta: 1,
        });
        continue;
      }
      const errors = Array.isArray(item.errors) ? item.errors : [];
      const createdId = clean(item.data?.createCard?.card?.id);
      if (errors.length > 0 || !createdId) {
        anyFailure = true;
        addFailure(ctx, 'FIN-04 Criar item', 'create_item_logical_error', ctx.rule.ruleId, templateItemId);
        if (errors.length > 0) {
          addGraphqlErrorLog(ctx, {
            node_name: 'FIN-04 Criar item',
            operation_name: 'create_fin04_item',
            pipe_id: '306859915',
            phase_id: '341351580',
            entity_type: 'fin04_item',
            entity_id: templateItemId,
            graphql_call_index: Number(item.createGraphqlCallIndex || 0),
            error_message: clean(errors[0]?.message || 'graphql_error'),
            query_excerpt: 'mutation CreateFin04',
            response_json: jsonString(item),
          });
        }
        addEntityLog(ctx, {
          node_name: 'Consolidar materializacao',
          entity_type: 'fin04_item',
          entity_id: templateItemId,
          template_item_id: templateItemId,
          action: 'create_item',
          status: 'failed',
          failure_reason: 'create_item_logical_error',
          dedupe_key: dedupeKey,
          graphql_calls_delta: 1,
        });
        continue;
      }
      ctx.createdItemIds.push(createdId);
      addEntityLog(ctx, {
        node_name: 'Consolidar materializacao',
        entity_type: 'fin04_item',
        entity_id: createdId,
        template_item_id: templateItemId,
        fin04_item_id: createdId,
        action: 'create_item',
        status: 'success',
        dedupe_key: dedupeKey,
        graphql_calls_delta: 1,
      });
      if (planned.shouldUpdateParcelas === true) {
        ctx.parcelUpdates.push({
          templateItemId,
          newParcelasPagas: planned.newParcelasPagas,
          shouldInactivateParent: planned.shouldInactivateParent === true,
          parcelasTotais: planned.parcelasTotais,
          dedupeKey,
        });
      }
    }
    ctx.createdItemIds = Array.from(new Set(ctx.createdItemIds));
    addWorkflowLog(ctx, {
      status: anyFailure ? 'failed' : 'success',
      failure_reason: anyFailure ? 'failed_item_stage' : null,
      rules_processed: 1,
      rules_failed: anyFailure ? 1 : 0,
      items_created: ctx.createdItemIds.length,
      summary_json: jsonString({ createdItemIds: ctx.createdItemIds, parcelUpdates: ctx.parcelUpdates }),
    });
    if (anyFailure || ctx.createdItemIds.length === 0) {
      const reason = anyFailure ? (ctx.failures[0]?.reason || 'failed_item_stage') : 'no_items_ready_for_invoice';
      return [{
        json: {
          contractVersion: 'fin_v3',
          itemType: 'rule_result',
          trace: ctx.trace,
          run: ctx.run,
          rule: { ruleId: ctx.rule.ruleId, ruleTitle: ctx.rule.ruleTitle, prefetchDecision: ctx.rule.prefetchDecision ?? null },
          result: {
            status: 'failed',
            reason,
            invoiceId: null,
            existingInvoiceId: null,
            createdItemIds: ctx.createdItemIds,
            totals: asObj(ctx.plan.totals),
            metricsDelta: { createItems: ctx.createdItemIds.length },
            failures: ctx.failures,
          },
          logs: { workflowLogs: ctx.workflowLogs, entityLogs: ctx.entityLogs, graphqlLogs: ctx.graphqlLogs },
          failures: ctx.failures,
        }
      }];
    }
    return [{
      json: {
        contractVersion: 'fin_v3',
        itemType: 'fin04_items_ready',
        trace: ctx.trace,
        run: ctx.run,
        rule: ctx.rule,
        itemPlan: ctx.plan,
        createdItemIds: ctx.createdItemIds,
        parcelUpdates: ctx.parcelUpdates,
        logs: { workflowLogs: ctx.workflowLogs, entityLogs: ctx.entityLogs, graphqlLogs: ctx.graphqlLogs },
        failures: ctx.failures,
      }
    }];
    """
)


FINALIZE_INIT_CODE = js(
    """
    const input = asObj($json);
    const ctx = ensureLogState({
      startedAt: nowIso,
      trace: {
        parentExecutionId: asObj(input.trace).workflowExecutionId ?? asObj(input.trace).parentExecutionId ?? null,
        workflowExecutionId: String($execution.id ?? 'manual'),
        workflowName: '[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02',
      },
      run: clone(asObj(input.run)),
      rule: clone(asObj(input.rule)),
      itemPlan: clone(asObj(input.itemPlan)),
      createdItemIds: Array.isArray(input.createdItemIds) ? input.createdItemIds : [],
      parcelUpdates: Array.isArray(input.parcelUpdates) ? input.parcelUpdates : [],
      invoiceId: null,
      terminal: { done: false, status: null, reason: null },
      metrics: {},
    });
    ctx.workflowLogs = Array.isArray(input.logs?.workflowLogs) ? input.logs.workflowLogs : [];
    ctx.entityLogs = Array.isArray(input.logs?.entityLogs) ? input.logs.entityLogs : [];
    ctx.graphqlLogs = Array.isArray(input.logs?.graphqlLogs) ? input.logs.graphqlLogs : [];
    ctx.failures = Array.isArray(input.failures) ? input.failures : [];
    if (String(input.contractVersion || '') !== 'fin_v3' || String(input.itemType || '') !== 'fin04_items_ready') {
      addFailure(ctx, 'Init finalizacao', 'unexpected_payload_shape', ctx.rule.ruleId ?? null);
      ctx.terminal = { done: true, status: 'failed', reason: 'unexpected_payload_shape' };
    }
    if (!ctx.terminal.done && ctx.createdItemIds.length === 0) {
      addFailure(ctx, 'Init finalizacao', 'no_items_ready_for_invoice', ctx.rule.ruleId ?? null);
      ctx.terminal = { done: true, status: 'failed', reason: 'no_items_ready_for_invoice' };
    }
    return [{ json: ctx }];
    """
)


FINALIZE_PARSE_RECHECK_CODE = js(
    """
    const merged = asObj($json);
    const ctx = ensureLogState(clone(asObj(merged.ctx || merged)));
    const response = asObj(merged.recheckResponse || merged);
    const callIndex = nextGraphqlIndex(ctx);
    if (ctx.terminal?.done === true) return [{ json: ctx }];
    if (response.error) {
      addFailure(ctx, 'FIN-03 Recheck fatura existente', 'invoice_recheck_transport_error', ctx.rule.ruleId, ctx.createdItemIds);
      addGraphqlErrorLog(ctx, {
        node_name: 'FIN-03 Recheck fatura existente',
        operation_name: 'recheck_fin03_by_rule_competence',
        pipe_id: '306859731',
        entity_type: 'invoice',
        entity_id: ctx.rule.ruleId,
        graphql_call_index: callIndex,
        error_message: clean(response.error?.message || 'transport_error'),
        query_excerpt: 'findCards(pipeId, search: competence)',
        response_json: jsonString(response),
      });
      ctx.terminal = { done: true, status: 'failed', reason: 'invoice_recheck_transport_error' };
      return [{ json: ctx }];
    }
    const errors = Array.isArray(response.errors) ? response.errors : [];
    if (errors.length > 0) {
      addFailure(ctx, 'FIN-03 Recheck fatura existente', 'invoice_recheck_logical_error', ctx.rule.ruleId, ctx.createdItemIds);
      addGraphqlErrorLog(ctx, {
        node_name: 'FIN-03 Recheck fatura existente',
        operation_name: 'recheck_fin03_by_rule_competence',
        pipe_id: '306859731',
        entity_type: 'invoice',
        entity_id: ctx.rule.ruleId,
        graphql_call_index: callIndex,
        error_message: clean(errors[0]?.message || 'graphql_error'),
        query_excerpt: 'findCards(pipeId, search: competence)',
        response_json: jsonString(response),
      });
      ctx.terminal = { done: true, status: 'failed', reason: 'invoice_recheck_logical_error' };
      return [{ json: ctx }];
    }
    if (response.data?.findCards?.pageInfo?.hasNextPage === true) {
      addFailure(ctx, 'FIN-03 Recheck fatura existente', 'invoice_recheck_requires_pagination', ctx.rule.ruleId, ctx.createdItemIds);
      ctx.terminal = { done: true, status: 'failed', reason: 'invoice_recheck_requires_pagination' };
      return [{ json: ctx }];
    }
    const edges = Array.isArray(response.data?.findCards?.edges) ? response.data.findCards.edges : [];
    for (const edge of edges) {
      const node = edge?.node;
      if (!node) continue;
      const fm = fieldMap(node.fields || []);
      const ruleId = clean(pickScalar(fm['id_da_regra_de_faturamento']));
      const comp = clean(pickScalar(fm['data_de_compet_ncia'])).slice(0, 10);
      if (ruleId === clean(ctx.rule.ruleId) && comp === clean(ctx.run.competenceYMD)) {
        addFailure(ctx, 'FIN-03 Recheck fatura existente', 'invoice_already_exists', ctx.rule.ruleId, node.id);
        addEntityLog(ctx, {
          node_name: 'Parse recheck de invoice',
          entity_type: 'invoice',
          entity_id: clean(node.id),
          invoice_id: clean(node.id),
          action: 'recheck_invoice',
          status: 'blocked',
          failure_reason: 'invoice_already_exists',
          dedupe_key: `${clean(ctx.rule.ruleId)}|${clean(ctx.run.competenceYMD)}`,
        });
        ctx.terminal = { done: true, status: 'failed', reason: 'invoice_already_exists' };
        ctx.invoiceId = clean(node.id);
        return [{ json: ctx }];
      }
    }
    const invoiceInput = clone(asObj(ctx.itemPlan.invoiceInputBase));
    invoiceInput.pipe_id = 306859731;
    invoiceInput.phase_id = 341350484;
    invoiceInput.fields_attributes = Array.isArray(invoiceInput.fields_attributes) ? invoiceInput.fields_attributes.slice() : [];
    invoiceInput.fields_attributes.push({ field_id: 'itens_da_fatura', field_value: ctx.createdItemIds });
    ctx.invoiceInput = invoiceInput;
    ctx.createInvoiceGraphqlCallIndex = nextGraphqlIndex(ctx);
    return [{ json: ctx }];
    """
)


FINALIZE_PARSE_CREATE_CODE = js(
    """
    const merged = asObj($json);
    const ctx = ensureLogState(clone(asObj(merged.ctx || merged)));
    const response = asObj(merged.createInvoiceResponse || merged);
    if (ctx.terminal?.done === true) return [{ json: ctx }];
    if (response.error) {
      addFailure(ctx, 'FIN-03 Criar fatura', 'failed_invoice_stage', ctx.rule.ruleId, ctx.createdItemIds);
      addGraphqlErrorLog(ctx, {
        node_name: 'FIN-03 Criar fatura',
        operation_name: 'create_fin03_invoice',
        pipe_id: '306859731',
        phase_id: '341350484',
        entity_type: 'invoice',
        entity_id: ctx.rule.ruleId,
        graphql_call_index: Number(ctx.createInvoiceGraphqlCallIndex || 0),
        error_message: clean(response.error?.message || 'transport_error'),
        query_excerpt: 'mutation CreateFin03',
        response_json: jsonString(response),
      });
      ctx.terminal = { done: true, status: 'failed', reason: 'failed_invoice_stage' };
      return [{ json: ctx }];
    }
    const errors = Array.isArray(response.errors) ? response.errors : [];
    const invoiceId = clean(response.data?.createCard?.card?.id);
    if (errors.length > 0 || !invoiceId) {
      addFailure(ctx, 'FIN-03 Criar fatura', 'failed_invoice_stage', ctx.rule.ruleId, ctx.createdItemIds);
      if (errors.length > 0) {
        addGraphqlErrorLog(ctx, {
          node_name: 'FIN-03 Criar fatura',
          operation_name: 'create_fin03_invoice',
          pipe_id: '306859731',
          phase_id: '341350484',
          entity_type: 'invoice',
          entity_id: ctx.rule.ruleId,
          graphql_call_index: Number(ctx.createInvoiceGraphqlCallIndex || 0),
          error_message: clean(errors[0]?.message || 'graphql_error'),
          query_excerpt: 'mutation CreateFin03',
          response_json: jsonString(response),
        });
      }
      ctx.terminal = { done: true, status: 'failed', reason: 'failed_invoice_stage' };
      return [{ json: ctx }];
    }
    ctx.invoiceId = invoiceId;
    ctx.linkUpdates = ctx.createdItemIds.map((itemId) => ({
      itemId: clean(itemId),
      invoiceId,
      dedupeKey: `${clean(itemId)}|${clean(ctx.run.competenceYMD)}`,
      graphqlCallIndex: nextGraphqlIndex(ctx),
    }));
    addEntityLog(ctx, {
      node_name: 'Parse criacao de invoice',
      entity_type: 'invoice',
      entity_id: invoiceId,
      invoice_id: invoiceId,
      action: 'create_invoice',
      status: 'success',
      dedupe_key: `${clean(ctx.rule.ruleId)}|${clean(ctx.run.competenceYMD)}`,
      graphql_calls_delta: 1,
    });
    return [{ json: ctx }];
    """
)


FINALIZE_CONSOLIDATE_LINKS_CODE = js(
    """
    const items = $input.all().map((item) => asObj(item.json));
    if (items.length === 0) throw new Error('No link update items');
    const ctx = ensureLogState(clone(asObj(items[0].ctx || items[0])));
    let hasFailure = false;
    for (const item of items) {
      const req = asObj(item.linkReq);
      const response = asObj(item.linkResponse || item);
      if (response.error) {
        hasFailure = true;
        addFailure(ctx, 'FIN-04 Vincular id_da_fatura', 'failed_invoice_stage', req.itemId, req.invoiceId);
        addGraphqlErrorLog(ctx, {
          node_name: 'FIN-04 Vincular id_da_fatura',
          operation_name: 'link_invoice_on_fin04',
          pipe_id: '306859915',
          entity_type: 'fin04_item',
          entity_id: req.itemId ?? null,
          card_id: req.itemId ?? null,
          graphql_call_index: Number(req.graphqlCallIndex || 0),
          error_message: clean(response.error?.message || 'transport_error'),
          query_excerpt: 'mutation LinkInvoiceOnFin04',
          response_json: jsonString(response),
        });
        addEntityLog(ctx, {
          node_name: 'Consolidar vinculos',
          entity_type: 'fin04_item',
          entity_id: req.itemId ?? null,
          fin04_item_id: req.itemId ?? null,
          invoice_id: req.invoiceId ?? null,
          action: 'update_item_invoice_id',
          status: 'failed',
          failure_reason: 'failed_invoice_stage',
          dedupe_key: req.dedupeKey ?? null,
          graphql_calls_delta: 1,
        });
        continue;
      }
      const errors = Array.isArray(response.errors) ? response.errors : [];
      const success = response.data?.updateFieldsValues?.success === true && errors.length === 0;
      if (!success) {
        hasFailure = true;
        addFailure(ctx, 'FIN-04 Vincular id_da_fatura', 'failed_invoice_stage', req.itemId, req.invoiceId);
        if (errors.length > 0) {
          addGraphqlErrorLog(ctx, {
            node_name: 'FIN-04 Vincular id_da_fatura',
            operation_name: 'link_invoice_on_fin04',
            pipe_id: '306859915',
            entity_type: 'fin04_item',
            entity_id: req.itemId ?? null,
            card_id: req.itemId ?? null,
            graphql_call_index: Number(req.graphqlCallIndex || 0),
            error_message: clean(errors[0]?.message || 'graphql_error'),
            query_excerpt: 'mutation LinkInvoiceOnFin04',
            response_json: jsonString(response),
          });
        }
        addEntityLog(ctx, {
          node_name: 'Consolidar vinculos',
          entity_type: 'fin04_item',
          entity_id: req.itemId ?? null,
          fin04_item_id: req.itemId ?? null,
          invoice_id: req.invoiceId ?? null,
          action: 'update_item_invoice_id',
          status: 'failed',
          failure_reason: 'failed_invoice_stage',
          dedupe_key: req.dedupeKey ?? null,
          graphql_calls_delta: 1,
        });
        continue;
      }
      addEntityLog(ctx, {
        node_name: 'Consolidar vinculos',
        entity_type: 'fin04_item',
        entity_id: req.itemId ?? null,
        fin04_item_id: req.itemId ?? null,
        invoice_id: req.invoiceId ?? null,
        action: 'update_item_invoice_id',
        status: 'success',
        dedupe_key: req.dedupeKey ?? null,
        graphql_calls_delta: 1,
      });
    }
    if (!hasFailure) {
      ctx.parcelUpdates = (Array.isArray(ctx.parcelUpdates) ? ctx.parcelUpdates : []).map((updateReq) => ({
        ...updateReq,
        graphqlCallIndex: nextGraphqlIndex(ctx),
      }));
    }
    if (hasFailure) ctx.terminal = { done: true, status: 'failed', reason: 'failed_invoice_stage' };
    return [{ json: ctx }];
    """
)


FINALIZE_CONSOLIDATE_PARCELAS_CODE = js(
    """
    const items = $input.all().map((item) => asObj(item.json));
    if (items.length === 0) throw new Error('No parcelas items');
    const ctx = ensureLogState(clone(asObj(items[0].ctx || items[0])));
    ctx.statusUpdates = [];
    let hasFailure = false;
    for (const item of items) {
      const req = asObj(item.updateReq);
      const response = asObj(item.updateResponse || item);
      if (response.error) {
        hasFailure = true;
        addFailure(ctx, 'FIN-02 Atualizar parcelas_pagas', 'failed_invoice_stage', req.templateItemId);
        addGraphqlErrorLog(ctx, {
          node_name: 'FIN-02 Atualizar parcelas_pagas',
          operation_name: 'update_fin02_parcelas_pagas',
          pipe_id: '306852209',
          entity_type: 'fin02_parent_item',
          entity_id: req.templateItemId ?? null,
          card_id: req.templateItemId ?? null,
          graphql_call_index: Number(req.graphqlCallIndex || 0),
          error_message: clean(response.error?.message || 'transport_error'),
          query_excerpt: 'mutation UpdateParcelasPagas',
          response_json: jsonString(response),
        });
        continue;
      }
      const errors = Array.isArray(response.errors) ? response.errors : [];
      const success = response.data?.updateFieldsValues?.success === true && errors.length === 0;
      if (!success) {
        hasFailure = true;
        addFailure(ctx, 'FIN-02 Atualizar parcelas_pagas', 'failed_invoice_stage', req.templateItemId);
        if (errors.length > 0) {
          addGraphqlErrorLog(ctx, {
            node_name: 'FIN-02 Atualizar parcelas_pagas',
            operation_name: 'update_fin02_parcelas_pagas',
            pipe_id: '306852209',
            entity_type: 'fin02_parent_item',
            entity_id: req.templateItemId ?? null,
            card_id: req.templateItemId ?? null,
            graphql_call_index: Number(req.graphqlCallIndex || 0),
            error_message: clean(errors[0]?.message || 'graphql_error'),
            query_excerpt: 'mutation UpdateParcelasPagas',
            response_json: jsonString(response),
          });
        }
        continue;
      }
      addEntityLog(ctx, {
        node_name: 'Consolidar update parcelas',
        entity_type: 'fin02_parent_item',
        entity_id: req.templateItemId ?? null,
        fin02_parent_item_id: req.templateItemId ?? null,
        action: 'increment_installment_counter',
        status: 'success',
        dedupe_key: req.dedupeKey ?? null,
        graphql_calls_delta: 1,
      });
      if (req.shouldInactivateParent === true) {
        ctx.statusUpdates.push({
          templateItemId: req.templateItemId,
          dedupeKey: req.dedupeKey,
          value: ['Inativo'],
          graphqlCallIndex: nextGraphqlIndex(ctx),
        });
      }
    }
    if (hasFailure) ctx.terminal = { done: true, status: 'failed', reason: 'failed_invoice_stage' };
    return [{ json: ctx }];
    """
)


FINALIZE_CONSOLIDATE_STATUS_CODE = js(
    """
    const items = $input.all().map((item) => asObj(item.json));
    if (items.length === 0) throw new Error('No status items');
    const ctx = ensureLogState(clone(asObj(items[0].ctx || items[0])));
    let hasFailure = false;
    for (const item of items) {
      const req = asObj(item.statusReq);
      const response = asObj(item.statusResponse || item);
      if (response.error) {
        hasFailure = true;
        addFailure(ctx, 'FIN-02 Atualizar status', 'failed_invoice_stage', req.templateItemId);
        addGraphqlErrorLog(ctx, {
          node_name: 'FIN-02 Atualizar status',
          operation_name: 'update_fin02_status',
          pipe_id: '306852209',
          entity_type: 'fin02_parent_item',
          entity_id: req.templateItemId ?? null,
          card_id: req.templateItemId ?? null,
          graphql_call_index: Number(req.graphqlCallIndex || 0),
          error_message: clean(response.error?.message || 'transport_error'),
          query_excerpt: 'mutation UpdateFin02Status',
          response_json: jsonString(response),
        });
        continue;
      }
      const errors = Array.isArray(response.errors) ? response.errors : [];
      const success = response.data?.updateFieldsValues?.success === true && errors.length === 0;
      if (!success) {
        hasFailure = true;
        addFailure(ctx, 'FIN-02 Atualizar status', 'failed_invoice_stage', req.templateItemId);
        if (errors.length > 0) {
          addGraphqlErrorLog(ctx, {
            node_name: 'FIN-02 Atualizar status',
            operation_name: 'update_fin02_status',
            pipe_id: '306852209',
            entity_type: 'fin02_parent_item',
            entity_id: req.templateItemId ?? null,
            card_id: req.templateItemId ?? null,
            graphql_call_index: Number(req.graphqlCallIndex || 0),
            error_message: clean(errors[0]?.message || 'graphql_error'),
            query_excerpt: 'mutation UpdateFin02Status',
            response_json: jsonString(response),
          });
        }
        continue;
      }
      addEntityLog(ctx, {
        node_name: 'Consolidar update status',
        entity_type: 'fin02_parent_item',
        entity_id: req.templateItemId ?? null,
        fin02_parent_item_id: req.templateItemId ?? null,
        action: 'inactivate_parent_item',
        status: 'success',
        dedupe_key: req.dedupeKey ?? null,
        graphql_calls_delta: 1,
      });
    }
    if (hasFailure) ctx.terminal = { done: true, status: 'failed', reason: 'failed_invoice_stage' };
    return [{ json: ctx }];
    """
)


FINALIZE_RESULT_CODE = js(
    """
    const ctx = ensureLogState(clone(asObj($json)));
    const reason = ctx.terminal?.reason || (ctx.failures[0]?.reason || 'unexpected_payload_shape');
    addWorkflowLog(ctx, {
      status: ctx.terminal?.done === true ? 'failed' : 'success',
      failure_reason: ctx.terminal?.done === true ? reason : null,
      rules_processed: 1,
      rules_failed: ctx.terminal?.done === true ? 1 : 0,
      items_created: Array.isArray(ctx.createdItemIds) ? ctx.createdItemIds.length : 0,
      items_updated: Array.isArray(ctx.createdItemIds) ? ctx.createdItemIds.length : 0,
      invoices_created: ctx.invoiceId ? 1 : 0,
      installments_incremented: Array.isArray(ctx.parcelUpdates) ? ctx.parcelUpdates.length : 0,
      parent_items_inactivated: Array.isArray(ctx.statusUpdates) ? ctx.statusUpdates.length : 0,
      summary_json: jsonString({ invoiceId: ctx.invoiceId ?? null, createdItemIds: ctx.createdItemIds ?? [] }),
    });
    const status = ctx.terminal?.done === true ? 'failed' : 'processed';
    const outReason = ctx.terminal?.done === true ? reason : 'invoice_created_and_finalized';
    return [{
      json: {
        contractVersion: 'fin_v3',
        itemType: 'rule_result',
        trace: ctx.trace,
        run: ctx.run,
        rule: { ruleId: ctx.rule.ruleId, ruleTitle: ctx.rule.ruleTitle, prefetchDecision: ctx.rule.prefetchDecision ?? null },
        result: {
          status,
          reason: outReason,
          invoiceId: ctx.invoiceId ?? null,
          existingInvoiceId: reason === 'invoice_already_exists' ? (ctx.invoiceId ?? null) : null,
          createdItemIds: Array.isArray(ctx.createdItemIds) ? ctx.createdItemIds : [],
          totals: asObj(ctx.itemPlan?.totals),
          metricsDelta: {
            createInvoices: ctx.invoiceId ? 1 : 0,
            createItems: Array.isArray(ctx.createdItemIds) ? ctx.createdItemIds.length : 0,
            updateItemInvoiceLinks: Array.isArray(ctx.createdItemIds) ? ctx.createdItemIds.length : 0,
            updateParcelasPagas: Array.isArray(ctx.parcelUpdates) ? ctx.parcelUpdates.length : 0,
            parentItemsInactivated: Array.isArray(ctx.statusUpdates) ? ctx.statusUpdates.length : 0,
          },
          failures: ctx.failures,
        },
        logs: { workflowLogs: ctx.workflowLogs, entityLogs: ctx.entityLogs, graphqlLogs: ctx.graphqlLogs },
        failures: ctx.failures,
      }
    }];
    """
)


def run_log_schema():
    return [
        {"defaultMatch": False, "display": True, "displayName": "run_id", "id": "run_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "workflow_execution_id", "id": "workflow_execution_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "parent_execution_id", "id": "parent_execution_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "workflow_name", "id": "workflow_name", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "trigger_type", "id": "trigger_type", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "competence_date", "id": "competence_date", "readOnly": False, "removed": False, "required": False, "type": "dateTime"},
        {"defaultMatch": False, "display": True, "displayName": "started_at", "id": "started_at", "readOnly": False, "removed": False, "required": False, "type": "dateTime"},
        {"defaultMatch": False, "display": True, "displayName": "finished_at", "id": "finished_at", "readOnly": False, "removed": False, "required": False, "type": "dateTime"},
        {"defaultMatch": False, "display": True, "displayName": "status", "id": "status", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "failure_reason", "id": "failure_reason", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "graphql_calls_total", "id": "graphql_calls_total", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "rules_found", "id": "rules_found", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "rules_eligible", "id": "rules_eligible", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "rules_processed", "id": "rules_processed", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "rules_failed", "id": "rules_failed", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "items_created", "id": "items_created", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "items_updated", "id": "items_updated", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "duplicate_items_blocked", "id": "duplicate_items_blocked", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "invoices_created", "id": "invoices_created", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "duplicate_invoices_blocked", "id": "duplicate_invoices_blocked", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "installments_incremented", "id": "installments_incremented", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "parent_items_inactivated", "id": "parent_items_inactivated", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "summary_json", "id": "summary_json", "readOnly": False, "removed": False, "required": False, "type": "string"},
    ]


def entity_log_schema():
    return [
        {"defaultMatch": False, "display": True, "displayName": "run_id", "id": "run_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "workflow_execution_id", "id": "workflow_execution_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "parent_execution_id", "id": "parent_execution_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "workflow_name", "id": "workflow_name", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "node_name", "id": "node_name", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "entity_type", "id": "entity_type", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "entity_id", "id": "entity_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "rule_id", "id": "rule_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "template_item_id", "id": "template_item_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "fin02_parent_item_id", "id": "fin02_parent_item_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "invoice_id", "id": "invoice_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "fin04_item_id", "id": "fin04_item_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "competence_date", "id": "competence_date", "readOnly": False, "removed": False, "required": False, "type": "dateTime"},
        {"defaultMatch": False, "display": True, "displayName": "action", "id": "action", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "status", "id": "status", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "failure_reason", "id": "failure_reason", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "dedupe_key", "id": "dedupe_key", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "graphql_calls_delta", "id": "graphql_calls_delta", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "started_at", "id": "started_at", "readOnly": False, "removed": False, "required": False, "type": "dateTime"},
        {"defaultMatch": False, "display": True, "displayName": "finished_at", "id": "finished_at", "readOnly": False, "removed": False, "required": False, "type": "dateTime"},
        {"defaultMatch": False, "display": True, "displayName": "details_json", "id": "details_json", "readOnly": False, "removed": False, "required": False, "type": "string"},
    ]


def graphql_log_schema():
    return [
        {"defaultMatch": False, "display": True, "displayName": "run_id", "id": "run_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "workflow_execution_id", "id": "workflow_execution_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "parent_execution_id", "id": "parent_execution_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "workflow_name", "id": "workflow_name", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "node_name", "id": "node_name", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "operation_name", "id": "operation_name", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "pipe_id", "id": "pipe_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "phase_id", "id": "phase_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "card_id", "id": "card_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "entity_type", "id": "entity_type", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "entity_id", "id": "entity_id", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "request_fingerprint", "id": "request_fingerprint", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "graphql_call_index", "id": "graphql_call_index", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "requested_at", "id": "requested_at", "readOnly": False, "removed": False, "required": False, "type": "dateTime"},
        {"defaultMatch": False, "display": True, "displayName": "responded_at", "id": "responded_at", "readOnly": False, "removed": False, "required": False, "type": "dateTime"},
        {"defaultMatch": False, "display": True, "displayName": "duration_ms", "id": "duration_ms", "readOnly": False, "removed": False, "required": False, "type": "number"},
        {"defaultMatch": False, "display": True, "displayName": "status", "id": "status", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "error_code", "id": "error_code", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "error_message", "id": "error_message", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "query_excerpt", "id": "query_excerpt", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "variables_json", "id": "variables_json", "readOnly": False, "removed": False, "required": False, "type": "string"},
        {"defaultMatch": False, "display": True, "displayName": "response_json", "id": "response_json", "readOnly": False, "removed": False, "required": False, "type": "string"},
    ]


def dt_node(name: str, schema: list, mapping: dict, placeholder_key: str, position) -> dict:
    return clone_node(
        "data_table",
        name,
        position,
        parameters={
            "columns": {
                "attemptToConvertTypes": False,
                "convertFieldsToString": False,
                "mappingMode": "defineBelow",
                "matchingColumns": [],
                "schema": schema,
                "value": mapping,
            },
            "dataTableId": {
                "__rl": True,
                "cachedResultName": PLACEHOLDER_TABLES[placeholder_key]["name"],
                "cachedResultUrl": f"/projects/MxRYCPypQDED18DI/datatables/{PLACEHOLDER_TABLES[placeholder_key]['id']}",
                "mode": "list",
                "value": PLACEHOLDER_TABLES[placeholder_key]["id"],
            },
            "options": {},
        },
    )


def orch_workflow() -> dict:
    wf = workflow_base(OLD_ORCH, ORCH_ID, WF_NAMES[ORCH_ID], tags=copy.deepcopy(OLD_PREP["tags"]))
    cron = clone_node("cron", "Cron (dias 1-5 07:00)", (-1180, -140))
    manual = clone_node("manual", "Manual Trigger", (-1180, 20))
    init = clone_node("code", "Init Run", (-940, -60), parameters={"jsCode": ORCH_INIT_CODE})
    prep = clone_node(
        "exec_wf",
        "Preparar regras elegiveis",
        (-700, -60),
        parameters={
            "options": {},
            "workflowId": {
                "__rl": True,
                "cachedResultName": WF_NAMES[PREP_RULES_ID],
                "cachedResultUrl": f"/workflow/{PREP_RULES_ID}",
                "mode": "list",
                "value": PREP_RULES_ID,
            },
            "workflowInputs": {
                "attemptToConvertTypes": False,
                "convertFieldsToString": False,
                "mappingMode": "defineBelow",
                "matchingColumns": [],
                "schema": [
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "runContext", "id": "runContext", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "trace", "id": "trace", "required": False, "type": "object"},
                ],
                "value": {
                    "runContext": "={{ $json.runContext }}",
                    "trace": "={{ $json.trace }}",
                },
            },
        },
    )
    loop = clone_node("split_batches", "Loop regras", (-440, -60), parameters={"options": {}})
    if_rule = clone_node(
        "if",
        "Loop: item e rule_input?",
        (-200, -20),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.itemType === 'rule_input' }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    prep_items = clone_node(
        "exec_wf",
        "Preparar itens elegiveis da regra",
        (60, -160),
        parameters={
            "options": {},
            "workflowId": {
                "__rl": True,
                "cachedResultName": WF_NAMES[PREP_ITEMS_ID],
                "cachedResultUrl": f"/workflow/{PREP_ITEMS_ID}",
                "mode": "list",
                "value": PREP_ITEMS_ID,
            },
            "workflowInputs": {
                "attemptToConvertTypes": False,
                "convertFieldsToString": False,
                "mappingMode": "defineBelow",
                "matchingColumns": [],
                "schema": [
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "contractVersion", "id": "contractVersion", "required": False, "type": "string"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "itemType", "id": "itemType", "required": False, "type": "string"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "trace", "id": "trace", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "run", "id": "run", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "rule", "id": "rule", "required": False, "type": "object"},
                ],
                "value": {
                    "contractVersion": "={{ $json.contractVersion }}",
                    "itemType": "={{ $json.itemType }}",
                    "trace": "={{ $json.trace }}",
                    "run": "={{ $json.run }}",
                    "rule": "={{ $json.rule }}",
                },
            },
        },
    )
    if_plan = clone_node(
        "if",
        "Loop: item e item_plan?",
        (320, -160),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.itemType === 'item_plan' }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    mat_items = clone_node(
        "exec_wf",
        "Materializar itens FIN-04",
        (580, -240),
        parameters={
            "options": {},
            "workflowId": {
                "__rl": True,
                "cachedResultName": WF_NAMES[MAT_ITEMS_ID],
                "cachedResultUrl": f"/workflow/{MAT_ITEMS_ID}",
                "mode": "list",
                "value": MAT_ITEMS_ID,
            },
            "workflowInputs": {
                "attemptToConvertTypes": False,
                "convertFieldsToString": False,
                "mappingMode": "defineBelow",
                "matchingColumns": [],
                "schema": [
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "contractVersion", "id": "contractVersion", "required": False, "type": "string"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "itemType", "id": "itemType", "required": False, "type": "string"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "trace", "id": "trace", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "run", "id": "run", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "rule", "id": "rule", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "itemPlan", "id": "itemPlan", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "logs", "id": "logs", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "failures", "id": "failures", "required": False, "type": "array"},
                ],
                "value": {
                    "contractVersion": "={{ $json.contractVersion }}",
                    "itemType": "={{ $json.itemType }}",
                    "trace": "={{ $json.trace }}",
                    "run": "={{ $json.run }}",
                    "rule": "={{ $json.rule }}",
                    "itemPlan": "={{ $json.itemPlan }}",
                    "logs": "={{ $json.logs }}",
                    "failures": "={{ $json.failures }}",
                },
            },
        },
    )
    if_ready = clone_node(
        "if",
        "Loop: item e fin04_items_ready?",
        (840, -240),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.itemType === 'fin04_items_ready' }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    finalize_rule = clone_node(
        "exec_wf",
        "Criar fatura FIN-03 e concluir regra",
        (1100, -320),
        parameters={
            "options": {},
            "workflowId": {
                "__rl": True,
                "cachedResultName": WF_NAMES[FINALIZE_ID],
                "cachedResultUrl": f"/workflow/{FINALIZE_ID}",
                "mode": "list",
                "value": FINALIZE_ID,
            },
            "workflowInputs": {
                "attemptToConvertTypes": False,
                "convertFieldsToString": False,
                "mappingMode": "defineBelow",
                "matchingColumns": [],
                "schema": [
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "contractVersion", "id": "contractVersion", "required": False, "type": "string"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "itemType", "id": "itemType", "required": False, "type": "string"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "trace", "id": "trace", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "run", "id": "run", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "rule", "id": "rule", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "itemPlan", "id": "itemPlan", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "createdItemIds", "id": "createdItemIds", "required": False, "type": "array"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "parcelUpdates", "id": "parcelUpdates", "required": False, "type": "array"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "logs", "id": "logs", "required": False, "type": "object"},
                    {"canBeUsedToMatch": True, "defaultMatch": False, "display": True, "displayName": "failures", "id": "failures", "required": False, "type": "array"},
                ],
                "value": {
                    "contractVersion": "={{ $json.contractVersion }}",
                    "itemType": "={{ $json.itemType }}",
                    "trace": "={{ $json.trace }}",
                    "run": "={{ $json.run }}",
                    "rule": "={{ $json.rule }}",
                    "itemPlan": "={{ $json.itemPlan }}",
                    "createdItemIds": "={{ $json.createdItemIds }}",
                    "parcelUpdates": "={{ $json.parcelUpdates }}",
                    "logs": "={{ $json.logs }}",
                    "failures": "={{ $json.failures }}",
                },
            },
        },
    )
    acc = clone_node("code", "Accumulate results", (1360, -60), parameters={"jsCode": ORCH_ACCUMULATE_CODE})
    finalize = clone_node("code", "Finalize summary", (1600, -60), parameters={"jsCode": ORCH_FINALIZE_CODE})
    run_pairs = clone_node(
        "set",
        "Emit workflow run logs",
        (1840, -260),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "runLogRows", "type": "json", "value": "={{ $json.workflowLogRows }}"},
                ]
            },
            "options": {},
        },
    )
    run_split = clone_node("split_out", "Split workflow run logs", (2060, -260), parameters={"fieldToSplitOut": "runLogRows", "options": {}})
    run_dt = dt_node(
        "Persistir fin_billing_run_log",
        run_log_schema(),
        {col["id"]: f"={{ $json.{col['id']} }}" for col in run_log_schema()},
        "run",
        (2280, -260),
    )
    restore_run = clone_node(
        "set",
        "Restaurar summary apos run log",
        (2500, -260),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "summary", "type": "json", "value": "={{ $node['Finalize summary'].json.summary }}"},
                    {"id": make_uuid(), "name": "run", "type": "json", "value": "={{ $node['Finalize summary'].json.run }}"},
                    {"id": make_uuid(), "name": "ok", "type": "boolean", "value": "={{ $node['Finalize summary'].json.ok }}"},
                    {"id": make_uuid(), "name": "entityLogRows", "type": "json", "value": "={{ $node['Finalize summary'].json.entityLogRows }}"},
                    {"id": make_uuid(), "name": "graphqlLogRows", "type": "json", "value": "={{ $node['Finalize summary'].json.graphqlLogRows }}"},
                ]
            },
            "options": {},
        },
    )
    entity_pairs = clone_node(
        "set",
        "Emit entity logs",
        (2720, -260),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "entityLogRows", "type": "json", "value": "={{ $json.entityLogRows }}"},
                    {"id": make_uuid(), "name": "summary", "type": "json", "value": "={{ $json.summary }}"},
                    {"id": make_uuid(), "name": "run", "type": "json", "value": "={{ $json.run }}"},
                    {"id": make_uuid(), "name": "ok", "type": "boolean", "value": "={{ $json.ok }}"},
                    {"id": make_uuid(), "name": "graphqlLogRows", "type": "json", "value": "={{ $json.graphqlLogRows }}"},
                ]
            },
            "options": {},
        },
    )
    has_entity_logs = clone_node(
        "if",
        "Ha entity logs?",
        (2940, -420),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ Array.isArray($json.entityLogRows) && $json.entityLogRows.length > 0 }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    entity_split = clone_node("split_out", "Split entity logs", (2940, -260), parameters={"fieldToSplitOut": "entityLogRows", "options": {}})
    entity_dt = dt_node(
        "Persistir fin_billing_entity_log",
        entity_log_schema(),
        {col["id"]: f"={{ $json.{col['id']} }}" for col in entity_log_schema()},
        "entity",
        (3160, -260),
    )
    restore_entity = clone_node(
        "set",
        "Restaurar summary apos entity log",
        (3380, -260),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "summary", "type": "json", "value": "={{ $node['Finalize summary'].json.summary }}"},
                    {"id": make_uuid(), "name": "run", "type": "json", "value": "={{ $node['Finalize summary'].json.run }}"},
                    {"id": make_uuid(), "name": "ok", "type": "boolean", "value": "={{ $node['Finalize summary'].json.ok }}"},
                    {"id": make_uuid(), "name": "graphqlLogRows", "type": "json", "value": "={{ $node['Finalize summary'].json.graphqlLogRows }}"},
                ]
            },
            "options": {},
        },
    )
    graphql_pairs = clone_node(
        "set",
        "Emit graphql logs",
        (3600, -260),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "graphqlLogRows", "type": "json", "value": "={{ $json.graphqlLogRows }}"},
                    {"id": make_uuid(), "name": "summary", "type": "json", "value": "={{ $json.summary }}"},
                    {"id": make_uuid(), "name": "run", "type": "json", "value": "={{ $json.run }}"},
                    {"id": make_uuid(), "name": "ok", "type": "boolean", "value": "={{ $json.ok }}"},
                ]
            },
            "options": {},
        },
    )
    has_graphql_logs = clone_node(
        "if",
        "Ha graphql logs?",
        (3820, -420),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ Array.isArray($json.graphqlLogRows) && $json.graphqlLogRows.length > 0 }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    graphql_split = clone_node("split_out", "Split graphql logs", (3820, -260), parameters={"fieldToSplitOut": "graphqlLogRows", "options": {}})
    graphql_dt = dt_node(
        "Persistir fin_billing_graphql_log",
        graphql_log_schema(),
        {col["id"]: f"={{ $json.{col['id']} }}" for col in graphql_log_schema()},
        "graphql",
        (4040, -260),
    )
    restore_final = clone_node(
        "set",
        "Restaurar summary final",
        (4260, -260),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "summary", "type": "json", "value": "={{ $node['Finalize summary'].json.summary }}"},
                    {"id": make_uuid(), "name": "run", "type": "json", "value": "={{ $node['Finalize summary'].json.run }}"},
                    {"id": make_uuid(), "name": "ok", "type": "boolean", "value": "={{ $node['Finalize summary'].json.ok }}"},
                ]
            },
            "options": {},
        },
    )
    has_errors = clone_node(
        "if",
        "Run: tem erros?",
        (4480, -260),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.ok !== true }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    stop = clone_node(
        "stop",
        "Run: Stop and Error",
        (4700, -320),
        parameters={
            "errorMessage": "={{ 'FIN billing mensal falhou. failureCount=' + String($json.summary.failureCount || 0) + ', firstFailure=' + String($json.run.failures?.[0]?.reason || 'n/a') + ', competence=' + String($json.summary.competenceYMD || 'n/a') }}",
        },
    )
    success = clone_node(
        "set",
        "Run: Success output",
        (4700, -180),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "ok", "type": "boolean", "value": True},
                    {"id": make_uuid(), "name": "summary", "type": "json", "value": "={{ $json.summary }}"},
                    {"id": make_uuid(), "name": "run", "type": "json", "value": "={{ $json.run }}"},
                ]
            },
            "options": {},
        },
    )
    wf["nodes"] = [
        cron,
        manual,
        init,
        prep,
        loop,
        if_rule,
        prep_items,
        if_plan,
        mat_items,
        if_ready,
        finalize_rule,
        acc,
        finalize,
        run_pairs,
        run_split,
        run_dt,
        restore_run,
        entity_pairs,
        has_entity_logs,
        entity_split,
        entity_dt,
        restore_entity,
        graphql_pairs,
        has_graphql_logs,
        graphql_split,
        graphql_dt,
        restore_final,
        has_errors,
        stop,
        success,
    ]
    wf["connections"] = {
        cron["name"]: {"main": [[{"node": init["name"], "type": "main", "index": 0}]]},
        manual["name"]: {"main": [[{"node": init["name"], "type": "main", "index": 0}]]},
        init["name"]: {"main": [[{"node": prep["name"], "type": "main", "index": 0}]]},
        prep["name"]: {"main": [[{"node": loop["name"], "type": "main", "index": 0}]]},
        loop["name"]: {
            "main": [
                [{"node": finalize["name"], "type": "main", "index": 0}],
                [{"node": if_rule["name"], "type": "main", "index": 0}],
            ]
        },
        if_rule["name"]: {
            "main": [
                [{"node": prep_items["name"], "type": "main", "index": 0}],
                [{"node": acc["name"], "type": "main", "index": 0}],
            ]
        },
        prep_items["name"]: {"main": [[{"node": if_plan["name"], "type": "main", "index": 0}]]},
        if_plan["name"]: {
            "main": [
                [{"node": mat_items["name"], "type": "main", "index": 0}],
                [{"node": acc["name"], "type": "main", "index": 0}],
            ]
        },
        mat_items["name"]: {"main": [[{"node": if_ready["name"], "type": "main", "index": 0}]]},
        if_ready["name"]: {
            "main": [
                [{"node": finalize_rule["name"], "type": "main", "index": 0}],
                [{"node": acc["name"], "type": "main", "index": 0}],
            ]
        },
        finalize_rule["name"]: {"main": [[{"node": acc["name"], "type": "main", "index": 0}]]},
        acc["name"]: {"main": [[{"node": loop["name"], "type": "main", "index": 0}]]},
        finalize["name"]: {"main": [[{"node": run_pairs["name"], "type": "main", "index": 0}]]},
        run_pairs["name"]: {"main": [[{"node": run_split["name"], "type": "main", "index": 0}]]},
        run_split["name"]: {"main": [[{"node": run_dt["name"], "type": "main", "index": 0}]]},
        run_dt["name"]: {"main": [[{"node": restore_run["name"], "type": "main", "index": 0}]]},
        restore_run["name"]: {"main": [[{"node": entity_pairs["name"], "type": "main", "index": 0}]]},
        entity_pairs["name"]: {"main": [[{"node": has_entity_logs["name"], "type": "main", "index": 0}]]},
        has_entity_logs["name"]: {
            "main": [
                [{"node": entity_split["name"], "type": "main", "index": 0}],
                [{"node": restore_entity["name"], "type": "main", "index": 0}],
            ]
        },
        entity_split["name"]: {"main": [[{"node": entity_dt["name"], "type": "main", "index": 0}]]},
        entity_dt["name"]: {"main": [[{"node": restore_entity["name"], "type": "main", "index": 0}]]},
        restore_entity["name"]: {"main": [[{"node": graphql_pairs["name"], "type": "main", "index": 0}]]},
        graphql_pairs["name"]: {"main": [[{"node": has_graphql_logs["name"], "type": "main", "index": 0}]]},
        has_graphql_logs["name"]: {
            "main": [
                [{"node": graphql_split["name"], "type": "main", "index": 0}],
                [{"node": restore_final["name"], "type": "main", "index": 0}],
            ]
        },
        graphql_split["name"]: {"main": [[{"node": graphql_dt["name"], "type": "main", "index": 0}]]},
        graphql_dt["name"]: {"main": [[{"node": restore_final["name"], "type": "main", "index": 0}]]},
        restore_final["name"]: {"main": [[{"node": has_errors["name"], "type": "main", "index": 0}]]},
        has_errors["name"]: {
            "main": [
                [{"node": stop["name"], "type": "main", "index": 0}],
                [{"node": success["name"], "type": "main", "index": 0}],
            ]
        },
    }
    return wf


def prep_rules_workflow() -> dict:
    wf = workflow_base(OLD_PREP, PREP_RULES_ID, WF_NAMES[PREP_RULES_ID], tags=copy.deepcopy(OLD_PREP["tags"]))
    trigger = clone_node(
        "exec_trigger",
        "Execute Workflow Trigger",
        (-1100, -60),
        parameters={
            "inputSource": "jsonExample",
            "jsonExample": json.dumps({"runContext": {"runId": "fin-billing:123:2026-02-01", "todayDay": 2, "competenceYMD": "2026-02-01"}, "trace": {"workflowExecutionId": "123"}}, ensure_ascii=False, indent=2),
        },
    )
    init = clone_node("code", "Init Run", (-860, -60), parameters={"jsCode": PREP_RULES_INIT_CODE})
    list_rules = clone_node(
        "graphql",
        "FIN-01: Listar regras Ativo (page)",
        (-620, -200),
        parameters={
            "authentication": "oAuth2",
            "endpoint": "https://api.pipefy.com/graphql",
            "query": "query RulesByPhase($phaseId: ID!, $first: Int!, $after: String) { phase(id: $phaseId) { id cards(first: $first, after: $after) { edges { node { id title fields { field { id label type } value report_value array_value } } } pageInfo { hasNextPage endCursor } } } }",
            "variables": "={{ { phaseId: '341313813', first: 100, after: (() => { const v = $json.after; return v ? String(v) : null; })() } }}",
        },
    )
    parse_rules = clone_node("code", "Parse rules page", (-380, -200), parameters={"jsCode": PREP_RULES_PARSE_PAGE_CODE})
    has_next_rules = clone_node(
        "if",
        "Ha proxima pagina de regras?",
        (-140, -200),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [{"leftValue": "={{ $json.hasNext === true && $json.fatal !== true }}", "operator": {"operation": "true", "singleValue": True, "type": "boolean"}, "rightValue": ""}],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    prep_fetch = clone_node(
        "graphql",
        "FIN-03: Prefetch faturas (competencia)",
        (120, -60),
        parameters={
            "authentication": "oAuth2",
            "endpoint": "https://api.pipefy.com/graphql",
            "query": "query PrefetchInvoices($pipeId: ID!, $fieldId: String!, $competencia: String!, $first: Int!, $after: String) { findCards(pipeId: $pipeId, first: $first, after: $after, search: { fieldId: $fieldId, fieldValue: $competencia }) { edges { node { id fields { field { id label type } value report_value array_value } } } pageInfo { hasNextPage endCursor } } }",
            "variables": "={{ { pipeId: '306859731', fieldId: 'data_de_compet_ncia', competencia: $node['Parse rules page'].json.ctx.run.competenceYMD, first: 100, after: (() => { const v = $json.prefetchAfter; return v ? String(v) : null; })() } }}",
        },
    )
    parse_prefetch = clone_node("code", "Parse prefetch", (360, -60), parameters={"jsCode": PREP_RULES_PARSE_PREFETCH_CODE})
    has_next_prefetch = clone_node(
        "if",
        "Ha proxima pagina de prefetch?",
        (600, -60),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [{"leftValue": "={{ $json.hasNext === true && $json.fatal !== true }}", "operator": {"operation": "true", "singleValue": True, "type": "boolean"}, "rightValue": ""}],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    build = clone_node("code", "Build regras elegiveis", (840, -60), parameters={"jsCode": PREP_RULES_BUILD_CODE})
    wf["nodes"] = [trigger, init, list_rules, parse_rules, has_next_rules, prep_fetch, parse_prefetch, has_next_prefetch, build]
    wf["connections"] = {
        trigger["name"]: {"main": [[{"node": init["name"], "type": "main", "index": 0}]]},
        init["name"]: {"main": [[{"node": list_rules["name"], "type": "main", "index": 0}]]},
        list_rules["name"]: {"main": [[{"node": parse_rules["name"], "type": "main", "index": 0}]]},
        parse_rules["name"]: {"main": [[{"node": has_next_rules["name"], "type": "main", "index": 0}]]},
        has_next_rules["name"]: {
            "main": [
                [{"node": list_rules["name"], "type": "main", "index": 0}],
                [{"node": prep_fetch["name"], "type": "main", "index": 0}],
            ]
        },
        prep_fetch["name"]: {"main": [[{"node": parse_prefetch["name"], "type": "main", "index": 0}]]},
        parse_prefetch["name"]: {"main": [[{"node": has_next_prefetch["name"], "type": "main", "index": 0}]]},
        has_next_prefetch["name"]: {
            "main": [
                [{"node": prep_fetch["name"], "type": "main", "index": 0}],
                [{"node": build["name"], "type": "main", "index": 0}],
            ]
        },
    }
    return wf


def prep_items_workflow() -> dict:
    wf = workflow_base(OLD_PROC, PREP_ITEMS_ID, WF_NAMES[PREP_ITEMS_ID], tags=copy.deepcopy(OLD_PROC["tags"]))
    trigger = clone_node(
        "exec_trigger",
        "Execute Workflow Trigger",
        (-1180, -60),
        parameters={"inputSource": "jsonExample", "jsonExample": json.dumps({"contractVersion": "fin_v3", "itemType": "rule_input"}, ensure_ascii=False, indent=2)},
    )
    init = clone_node("code", "Init contexto item_plan", (-940, -60), parameters={"jsCode": PREP_ITEMS_INIT_CODE})
    can_prepare = clone_node(
        "if",
        "Pode preparar itens?",
        (-820, -60),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.terminal?.done !== true && Array.isArray($json.rule?.itemTemplateIds) && $json.rule.itemTemplateIds.length > 0 }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    direct_result = clone_node("code", "Emitir rule_result direto", (-580, 120), parameters={"jsCode": PREP_ITEMS_DIRECT_RESULT_CODE})
    set_pairs = clone_node(
        "set",
        "Set pares template IDs",
        (-700, -60),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "emitTemplatePairs", "type": "json", "value": "={{ (Array.isArray($json.rule?.itemTemplateIds) ? $json.rule.itemTemplateIds : []).map((templateItemId) => ({ ctx: $json, templateItemId: String(templateItemId) })) }}"},
                ]
            },
            "options": {},
        },
    )
    split_out = clone_node("split_out", "Split Out template IDs", (-460, -60), parameters={"fieldToSplitOut": "emitTemplatePairs", "options": {}})
    fetch_template = clone_node(
        "graphql",
        "FIN-02 Buscar item template",
        (-220, -200),
        parameters={
            "authentication": "oAuth2",
            "endpoint": "https://api.pipefy.com/graphql",
            "query": "query GetTemplateItem($id: ID!) { card(id: $id) { id title current_phase { id name } fields { field { id label type } value report_value array_value } } }",
            "variables": "={{ { id: String($json.templateItemId) } }}",
        },
    )
    wrap_template = clone_node(
        "set",
        "Embalar retorno template",
        (-100, -200),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "templateResponse", "type": "json", "value": "={{ $json }}"},
                ]
            },
            "options": {},
        },
    )
    merge_template = clone_node("merge", "Merge retorno template", (20, -200))
    check_conflicts = clone_node(
        "graphql",
        "FIN-04 Verificar conflitos por competencia",
        (-220, 80),
        parameters={
            "authentication": "oAuth2",
            "endpoint": "https://api.pipefy.com/graphql",
            "query": "query FindFin04ByTemplate($pipeId: ID!, $fieldId: String!, $templateId: String!, $first: Int!) { findCards(pipeId: $pipeId, first: $first, search: { fieldId: $fieldId, fieldValue: $templateId }) { edges { node { id fields { field { id label type } value report_value array_value } } } pageInfo { hasNextPage endCursor } } }",
            "variables": "={{ { pipeId: '306859915', fieldId: 'id_do_item_usado_como_template_fin_02', templateId: String($json.templateItemId), first: 100 } }}",
        },
    )
    wrap_conflicts = clone_node(
        "set",
        "Embalar retorno conflitos",
        (-100, 80),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "conflictResponse", "type": "json", "value": "={{ $json }}"},
                ]
            },
            "options": {},
        },
    )
    merge_conflicts = clone_node("merge", "Merge retorno conflitos", (20, 80))
    consolidate = clone_node("code", "Consolidar elegibilidade e planos", (280, -60), parameters={"jsCode": PREP_ITEMS_CONSOLIDATE_CODE})
    wf["nodes"] = [trigger, init, can_prepare, direct_result, set_pairs, split_out, fetch_template, wrap_template, merge_template, check_conflicts, wrap_conflicts, merge_conflicts, consolidate]
    wf["connections"] = {
        trigger["name"]: {"main": [[{"node": init["name"], "type": "main", "index": 0}]]},
        init["name"]: {"main": [[{"node": can_prepare["name"], "type": "main", "index": 0}]]},
        can_prepare["name"]: {
            "main": [
                [{"node": set_pairs["name"], "type": "main", "index": 0}],
                [{"node": direct_result["name"], "type": "main", "index": 0}],
            ]
        },
        set_pairs["name"]: {"main": [[{"node": split_out["name"], "type": "main", "index": 0}]]},
        split_out["name"]: {"main": [[{"node": fetch_template["name"], "type": "main", "index": 0}, {"node": merge_template["name"], "type": "main", "index": 1}, {"node": check_conflicts["name"], "type": "main", "index": 0}, {"node": merge_conflicts["name"], "type": "main", "index": 1}]]},
        fetch_template["name"]: {"main": [[{"node": wrap_template["name"], "type": "main", "index": 0}]]},
        wrap_template["name"]: {"main": [[{"node": merge_template["name"], "type": "main", "index": 0}]]},
        merge_template["name"]: {"main": [[{"node": merge_conflicts["name"], "type": "main", "index": 0}]]},
        check_conflicts["name"]: {"main": [[{"node": wrap_conflicts["name"], "type": "main", "index": 0}]]},
        wrap_conflicts["name"]: {"main": [[{"node": merge_conflicts["name"], "type": "main", "index": 1}]]},
        merge_conflicts["name"]: {"main": [[{"node": consolidate["name"], "type": "main", "index": 0}]]},
    }
    return wf


def materialize_workflow() -> dict:
    wf = workflow_base(OLD_PROC, MAT_ITEMS_ID, WF_NAMES[MAT_ITEMS_ID], tags=copy.deepcopy(OLD_PROC["tags"]))
    trigger = clone_node(
        "exec_trigger",
        "Execute Workflow Trigger",
        (-1180, -60),
        parameters={"inputSource": "jsonExample", "jsonExample": json.dumps({"contractVersion": "fin_v3", "itemType": "item_plan"}, ensure_ascii=False, indent=2)},
    )
    init = clone_node("code", "Init materializacao", (-940, -60), parameters={"jsCode": MAT_ITEMS_INIT_CODE})
    can_materialize = clone_node(
        "if",
        "Pode materializar itens?",
        (-820, -60),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.terminal?.done !== true && Array.isArray($json.plan?.plannedItems) && $json.plan.plannedItems.length > 0 }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    direct_result = clone_node("code", "Emitir rule_result direto", (-580, 120), parameters={"jsCode": MAT_ITEMS_DIRECT_RESULT_CODE})
    set_pairs = clone_node(
        "set",
        "Set pares create requests",
        (-700, -60),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "emitCreatePairs", "type": "json", "value": "={{ (Array.isArray($json.plan?.plannedItems) ? $json.plan.plannedItems : []).map((plannedItem) => ({ ctx: $json, plannedItem })) }}"},
                ]
            },
            "options": {},
        },
    )
    split_out = clone_node("split_out", "Split Out create requests", (-460, -60), parameters={"fieldToSplitOut": "emitCreatePairs", "options": {}})
    recheck = clone_node(
        "graphql",
        "FIN-04 Recheck conflito antes de criar",
        (-220, -180),
        parameters={
            "authentication": "oAuth2",
            "endpoint": "https://api.pipefy.com/graphql",
            "query": "query FindFin04ByTemplate($pipeId: ID!, $fieldId: String!, $templateId: String!, $first: Int!) { findCards(pipeId: $pipeId, first: $first, search: { fieldId: $fieldId, fieldValue: $templateId }) { edges { node { id fields { field { id label type } value report_value array_value } } } pageInfo { hasNextPage endCursor } } }",
            "variables": "={{ { pipeId: '306859915', fieldId: 'id_do_item_usado_como_template_fin_02', templateId: String($json.plannedItem.templateItemId), first: 100 } }}",
        },
    )
    merge_recheck = clone_node("merge", "Merge retorno recheck", (20, -180))
    parse_recheck = clone_node("code", "Parse recheck conflito", (240, -180), parameters={"jsCode": MAT_ITEMS_PARSE_RECHECK_CODE})
    can_create_item = clone_node(
        "if",
        "Pode criar item FIN-04?",
        (480, -180),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.canCreateItem === true }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    create_item = clone_node(
        "graphql",
        "FIN-04 Criar item",
        (720, -300),
        parameters={
            "authentication": "oAuth2",
            "endpoint": "https://api.pipefy.com/graphql",
            "query": "mutation CreateFin04($input: CreateCardInput!) { createCard(input: $input) { card { id url } } }",
            "variables": "={{ { input: $json.plannedItem.createInput } }}",
        },
    )
    merge_create = clone_node("merge", "Merge criacao item", (960, -180))
    consolidate = clone_node("code", "Consolidar materializacao", (1200, -60), parameters={"jsCode": MAT_ITEMS_CONSOLIDATE_CODE})
    wf["nodes"] = [trigger, init, can_materialize, direct_result, set_pairs, split_out, recheck, merge_recheck, parse_recheck, can_create_item, create_item, merge_create, consolidate]
    wf["connections"] = {
        trigger["name"]: {"main": [[{"node": init["name"], "type": "main", "index": 0}]]},
        init["name"]: {"main": [[{"node": can_materialize["name"], "type": "main", "index": 0}]]},
        can_materialize["name"]: {
            "main": [
                [{"node": set_pairs["name"], "type": "main", "index": 0}],
                [{"node": direct_result["name"], "type": "main", "index": 0}],
            ]
        },
        set_pairs["name"]: {"main": [[{"node": split_out["name"], "type": "main", "index": 0}]]},
        split_out["name"]: {"main": [[{"node": recheck["name"], "type": "main", "index": 0}, {"node": merge_recheck["name"], "type": "main", "index": 1}]]},
        recheck["name"]: {"main": [[{"node": merge_recheck["name"], "type": "main", "index": 0}]]},
        merge_recheck["name"]: {"main": [[{"node": parse_recheck["name"], "type": "main", "index": 0}]]},
        parse_recheck["name"]: {"main": [[{"node": can_create_item["name"], "type": "main", "index": 0}, {"node": merge_create["name"], "type": "main", "index": 1}]]},
        can_create_item["name"]: {
            "main": [
                [{"node": create_item["name"], "type": "main", "index": 0}],
                [{"node": merge_create["name"], "type": "main", "index": 0}],
            ]
        },
        create_item["name"]: {"main": [[{"node": merge_create["name"], "type": "main", "index": 0}]]},
        merge_create["name"]: {"main": [[{"node": consolidate["name"], "type": "main", "index": 0}]]},
    }
    return wf


def finalize_workflow() -> dict:
    wf = workflow_base(OLD_PROC, FINALIZE_ID, WF_NAMES[FINALIZE_ID], tags=copy.deepcopy(OLD_PROC["tags"]))
    trigger = clone_node(
        "exec_trigger",
        "Execute Workflow Trigger",
        (-1420, -60),
        parameters={"inputSource": "jsonExample", "jsonExample": json.dumps({"contractVersion": "fin_v3", "itemType": "fin04_items_ready"}, ensure_ascii=False, indent=2)},
    )
    init = clone_node("code", "Init finalizacao", (-1180, -60), parameters={"jsCode": FINALIZE_INIT_CODE})
    can_start = clone_node(
        "if",
        "Pode iniciar finalizacao?",
        (-1060, -60),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.terminal?.done !== true && Array.isArray($json.createdItemIds) && $json.createdItemIds.length > 0 }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    recheck = clone_node(
        "graphql",
        "FIN-03 Recheck fatura existente",
        (-940, -180),
        parameters={
            "authentication": "oAuth2",
            "endpoint": "https://api.pipefy.com/graphql",
            "query": "query FindInvoicesForRule($pipeId: ID!, $fieldId: String!, $ruleId: String!, $first: Int!) { findCards(pipeId: $pipeId, first: $first, search: { fieldId: $fieldId, fieldValue: $ruleId }) { edges { node { id fields { field { id label type } value report_value array_value } } } pageInfo { hasNextPage endCursor } } }",
            "variables": "={{ { pipeId: '306859731', fieldId: 'id_da_regra_de_faturamento', ruleId: String($json.rule.ruleId), first: 100 } }}",
        },
    )
    merge_recheck = clone_node("merge", "Merge recheck invoice", (-700, -180))
    parse_recheck = clone_node("code", "Parse recheck invoice", (-460, -180), parameters={"jsCode": FINALIZE_PARSE_RECHECK_CODE})
    can_create_invoice = clone_node(
        "if",
        "Pode criar invoice?",
        (-340, -180),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.terminal?.done !== true && !!$json.invoiceInput }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    create_invoice = clone_node(
        "graphql",
        "FIN-03 Criar fatura",
        (-100, -320),
        parameters={
            "authentication": "oAuth2",
            "endpoint": "https://api.pipefy.com/graphql",
            "query": "mutation CreateFin03($input: CreateCardInput!) { createCard(input: $input) { card { id title url } } }",
            "variables": "={{ { input: $json.invoiceInput } }}",
        },
    )
    merge_create = clone_node("merge", "Merge criacao invoice", (20, -180))
    parse_create = clone_node("code", "Parse criacao invoice", (260, -180), parameters={"jsCode": FINALIZE_PARSE_CREATE_CODE})
    can_link = clone_node(
        "if",
        "Pode vincular itens FIN-04?",
        (380, -180),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.terminal?.done !== true && Array.isArray($json.linkUpdates) && $json.linkUpdates.length > 0 }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    set_link = clone_node(
        "set",
        "Set pares link updates",
        (500, -180),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "emitLinkPairs", "type": "json", "value": "={{ (Array.isArray($json.linkUpdates) ? $json.linkUpdates : []).map((linkReq) => ({ ctx: $json, linkReq })) }}"},
                ]
            },
            "options": {},
        },
    )
    split_link = clone_node("split_out", "Split Out link updates", (740, -180), parameters={"fieldToSplitOut": "emitLinkPairs", "options": {}})
    link_item = clone_node(
        "graphql",
        "FIN-04 Vincular id_da_fatura",
        (980, -320),
        parameters={
            "authentication": "oAuth2",
            "endpoint": "https://api.pipefy.com/graphql",
            "query": "mutation LinkInvoiceOnFin04($itemId: ID!, $value: [UndefinedInput!]!) { updateFieldsValues(input: { nodeId: $itemId, values: [{ fieldId: \\\"id_da_fatura\\\", value: $value }] }) { success } }",
            "variables": "={{ { itemId: String($json.linkReq.itemId), value: [String($json.linkReq.invoiceId)] } }}",
        },
    )
    merge_link = clone_node("merge", "Merge retorno link", (1220, -320))
    consolidate_link = clone_node("code", "Consolidar vinculos", (1460, -320), parameters={"jsCode": FINALIZE_CONSOLIDATE_LINKS_CODE})
    can_update_parcel = clone_node(
        "if",
        "Ha parcelas para atualizar?",
        (1580, -320),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.terminal?.done !== true && Array.isArray($json.parcelUpdates) && $json.parcelUpdates.length > 0 }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    set_parcel = clone_node(
        "set",
        "Set pares parcelas",
        (1700, -320),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "emitParcelaPairs", "type": "json", "value": "={{ (Array.isArray($json.parcelUpdates) ? $json.parcelUpdates : []).map((updateReq) => ({ ctx: $json, updateReq })) }}"},
                ]
            },
            "options": {},
        },
    )
    split_parcel = clone_node("split_out", "Split Out parcelas", (1940, -320), parameters={"fieldToSplitOut": "emitParcelaPairs", "options": {}})
    update_parcel = clone_node(
        "graphql",
        "FIN-02 Atualizar parcelas_pagas",
        (2180, -460),
        parameters={
            "authentication": "oAuth2",
            "endpoint": "https://api.pipefy.com/graphql",
            "query": "mutation UpdateParcelasPagas($cardId: ID!, $value: [UndefinedInput!]!) { updateFieldsValues(input: { nodeId: $cardId, values: [{ fieldId: \\\"parcelas_pagas\\\", value: $value }] }) { success } }",
            "variables": "={{ { cardId: String($json.updateReq.templateItemId), value: [Number($json.updateReq.newParcelasPagas)] } }}",
        },
    )
    merge_parcel = clone_node("merge", "Merge retorno parcelas", (2420, -460))
    consolidate_parcel = clone_node("code", "Consolidar update parcelas", (2660, -460), parameters={"jsCode": FINALIZE_CONSOLIDATE_PARCELAS_CODE})
    can_update_status = clone_node(
        "if",
        "Ha status para atualizar?",
        (2780, -460),
        parameters={
            "conditions": {
                "combinator": "and",
                "conditions": [
                    {
                        "leftValue": "={{ $json.terminal?.done !== true && Array.isArray($json.statusUpdates) && $json.statusUpdates.length > 0 }}",
                        "operator": {"operation": "true", "singleValue": True, "type": "boolean"},
                        "rightValue": "",
                    }
                ],
                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 2},
            },
            "options": {},
        },
    )
    set_status = clone_node(
        "set",
        "Set pares status",
        (2900, -460),
        parameters={
            "assignments": {
                "assignments": [
                    {"id": make_uuid(), "name": "emitStatusPairs", "type": "json", "value": "={{ (Array.isArray($json.statusUpdates) ? $json.statusUpdates : []).map((statusReq) => ({ ctx: $json, statusReq })) }}"},
                ]
            },
            "options": {},
        },
    )
    split_status = clone_node("split_out", "Split Out status", (3140, -460), parameters={"fieldToSplitOut": "emitStatusPairs", "options": {}})
    update_status = clone_node(
        "graphql",
        "FIN-02 Atualizar status",
        (3380, -600),
        parameters={
            "authentication": "oAuth2",
            "endpoint": "https://api.pipefy.com/graphql",
            "query": "mutation UpdateFin02Status($cardId: ID!, $value: [UndefinedInput!]!) { updateFieldsValues(input: { nodeId: $cardId, values: [{ fieldId: \\\"status\\\", value: $value }] }) { success } }",
            "variables": "={{ { cardId: String($json.statusReq.templateItemId), value: $json.statusReq.value } }}",
        },
    )
    merge_status = clone_node("merge", "Merge retorno status", (3620, -600))
    consolidate_status = clone_node("code", "Consolidar update status", (3860, -600), parameters={"jsCode": FINALIZE_CONSOLIDATE_STATUS_CODE})
    final_result = clone_node("code", "Finalizar rule_result", (4100, -320), parameters={"jsCode": FINALIZE_RESULT_CODE})
    wf["nodes"] = [
        trigger,
        init,
        can_start,
        recheck,
        merge_recheck,
        parse_recheck,
        can_create_invoice,
        create_invoice,
        merge_create,
        parse_create,
        can_link,
        set_link,
        split_link,
        link_item,
        merge_link,
        consolidate_link,
        can_update_parcel,
        set_parcel,
        split_parcel,
        update_parcel,
        merge_parcel,
        consolidate_parcel,
        can_update_status,
        set_status,
        split_status,
        update_status,
        merge_status,
        consolidate_status,
        final_result,
    ]
    wf["connections"] = {
        trigger["name"]: {"main": [[{"node": init["name"], "type": "main", "index": 0}]]},
        init["name"]: {"main": [[{"node": can_start["name"], "type": "main", "index": 0}]]},
        can_start["name"]: {
            "main": [
                [{"node": recheck["name"], "type": "main", "index": 0}, {"node": merge_recheck["name"], "type": "main", "index": 1}],
                [{"node": final_result["name"], "type": "main", "index": 0}],
            ]
        },
        recheck["name"]: {"main": [[{"node": merge_recheck["name"], "type": "main", "index": 0}]]},
        merge_recheck["name"]: {"main": [[{"node": parse_recheck["name"], "type": "main", "index": 0}]]},
        parse_recheck["name"]: {"main": [[{"node": can_create_invoice["name"], "type": "main", "index": 0}, {"node": merge_create["name"], "type": "main", "index": 1}]]},
        can_create_invoice["name"]: {
            "main": [
                [{"node": create_invoice["name"], "type": "main", "index": 0}],
                [{"node": merge_create["name"], "type": "main", "index": 0}],
            ]
        },
        create_invoice["name"]: {"main": [[{"node": merge_create["name"], "type": "main", "index": 0}]]},
        merge_create["name"]: {"main": [[{"node": parse_create["name"], "type": "main", "index": 0}]]},
        parse_create["name"]: {"main": [[{"node": can_link["name"], "type": "main", "index": 0}]]},
        can_link["name"]: {
            "main": [
                [{"node": set_link["name"], "type": "main", "index": 0}],
                [{"node": final_result["name"], "type": "main", "index": 0}],
            ]
        },
        set_link["name"]: {"main": [[{"node": split_link["name"], "type": "main", "index": 0}]]},
        split_link["name"]: {"main": [[{"node": link_item["name"], "type": "main", "index": 0}, {"node": merge_link["name"], "type": "main", "index": 1}]]},
        link_item["name"]: {"main": [[{"node": merge_link["name"], "type": "main", "index": 0}]]},
        merge_link["name"]: {"main": [[{"node": consolidate_link["name"], "type": "main", "index": 0}]]},
        consolidate_link["name"]: {"main": [[{"node": can_update_parcel["name"], "type": "main", "index": 0}]]},
        can_update_parcel["name"]: {
            "main": [
                [{"node": set_parcel["name"], "type": "main", "index": 0}],
                [{"node": final_result["name"], "type": "main", "index": 0}],
            ]
        },
        set_parcel["name"]: {"main": [[{"node": split_parcel["name"], "type": "main", "index": 0}]]},
        split_parcel["name"]: {"main": [[{"node": update_parcel["name"], "type": "main", "index": 0}, {"node": merge_parcel["name"], "type": "main", "index": 1}]]},
        update_parcel["name"]: {"main": [[{"node": merge_parcel["name"], "type": "main", "index": 0}]]},
        merge_parcel["name"]: {"main": [[{"node": consolidate_parcel["name"], "type": "main", "index": 0}]]},
        consolidate_parcel["name"]: {"main": [[{"node": can_update_status["name"], "type": "main", "index": 0}]]},
        can_update_status["name"]: {
            "main": [
                [{"node": set_status["name"], "type": "main", "index": 0}],
                [{"node": final_result["name"], "type": "main", "index": 0}],
            ]
        },
        set_status["name"]: {"main": [[{"node": split_status["name"], "type": "main", "index": 0}]]},
        split_status["name"]: {"main": [[{"node": update_status["name"], "type": "main", "index": 0}, {"node": merge_status["name"], "type": "main", "index": 1}]]},
        update_status["name"]: {"main": [[{"node": merge_status["name"], "type": "main", "index": 0}]]},
        merge_status["name"]: {"main": [[{"node": consolidate_status["name"], "type": "main", "index": 0}]]},
        consolidate_status["name"]: {"main": [[{"node": final_result["name"], "type": "main", "index": 0}]]},
    }
    return wf


def write_workflow(workflow: dict):
    path = WORKFLOWS_DIR / f"{workflow['id']}.json"
    path.write_text(json.dumps(workflow, ensure_ascii=False, indent=2) + "\n")


def update_index(workflows: list):
    existing = json.loads(INDEX_PATH.read_text())
    data = [item for item in existing.get("data", []) if item.get("id") not in {wf["id"] for wf in workflows}]
    for wf in workflows:
        idx_item = clone = copy.deepcopy(wf)
        idx_item["shared"] = simplify_shared_for_index(wf.get("shared", []))
        data.append(idx_item)
    data.sort(key=lambda item: (item.get("name") or "", item.get("id") or ""))
    INDEX_PATH.write_text(json.dumps({"data": data}, ensure_ascii=False, indent=2) + "\n")


def main():
    workflows = [
        orch_workflow(),
        prep_rules_workflow(),
        prep_items_workflow(),
        materialize_workflow(),
        finalize_workflow(),
    ]
    for workflow in workflows:
        write_workflow(workflow)
    update_index(workflows)
    print("generated", len(workflows), "workflows")


if __name__ == "__main__":
    main()
