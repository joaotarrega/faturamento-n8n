# Visão geral da suíte FIN mensal

## Executive summary
Os cinco workflows implementam uma esteira fail-closed de faturamento mensal no Pipefy. O fluxo começa localizando regras FIN-01 ativas elegíveis para a competência anterior, transforma cada item-template FIN-02 em um plano de criação de FIN-04, materializa os itens FIN-04, cria uma FIN-03 com os itens gerados, vincula a fatura nos FIN-04 e atualiza o FIN-02 pai quando a cobrança é parcelada.

O orquestrador não faz GraphQL direto no Pipefy. Ele controla a execução, roteia itens pelo marcador `itemType`, agrega métricas/resultados, persiste um log técnico por regra em `fin_billing_rule_log` e encerra a execução com erro quando qualquer regra falha, é bloqueada ou produz inconsistência estrutural.

## Sequência ponta a ponta
1. `[FIN] 1.0 Orquestrar faturamento mensal` dispara por `Cron (dias 1-5 07:00)` ou `Manual Trigger`, calcula a competência como o primeiro dia do mês anterior e chama `[FIN] 1.1 Preparar regras elegiveis`.
2. `[FIN] 1.1 Preparar regras elegiveis` pagina os cards FIN-01 na fase `341313813`, pré-carrega FIN-03 já existentes na competência e emite três tipos de saída: `prep_summary`, `rule_input` e `rule_result`.
3. O orquestrador envia apenas `rule_input` para `[FIN] 1.2 Preparar itens elegiveis da regra`.
4. `[FIN] 1.2 Preparar itens elegiveis da regra` expande os `itemTemplateIds`, carrega cada FIN-02, verifica conflitos FIN-04 por competência, aplica guardas de fase/início/campos obrigatórios e emite `item_plan` ou `rule_result`.
5. O orquestrador envia apenas `item_plan` para `[FIN] 1.3 Materializar itens FIN-04`.
6. `[FIN] 1.3 Materializar itens FIN-04` revalida conflito por template imediatamente antes de criar, cria os FIN-04 e emite `fin04_items_ready` ou `rule_result`.
7. O orquestrador envia apenas `fin04_items_ready` para `[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02`.
8. `[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02` revalida a inexistência de FIN-03 para a mesma regra+competência, cria a fatura, grava `id_da_fatura` nos FIN-04, incrementa `parcelas_pagas` nos FIN-02 parcelados e, quando necessário, muda o status do FIN-02 para `Inativo`.
9. O orquestrador acumula todos os `rule_result`, monta um sumário final, grava uma linha por regra na Data Table `fin_billing_rule_log` e encerra a run com sucesso ou erro.

## Mapa dos workflows
| Workflow | Papel na pipeline | Trigger / entrada | Saída principal | Objetos Pipefy tocados |
| --- | --- | --- | --- | --- |
| `[FIN] 1.0 Orquestrar faturamento mensal` | Controle da run, roteamento, agregação e logging | `Cron (dias 1-5 07:00)` e `Manual Trigger` | `summary` final, `run`, `ok` | Data Table `[Logs] FIN - Regras mensais` (`5CPaGUez5Ex8tJHi`) |
| `[FIN] 1.1 Preparar regras elegiveis` | Descobrir regras FIN-01 a processar na competência | `Execute Workflow Trigger` com `runContext` e `trace` | `prep_summary`, `rule_input`, `rule_result` | FIN-01 (`pipe 306662125`, fase `341313813`), FIN-03 (`pipe 306859731`) |
| `[FIN] 1.2 Preparar itens elegiveis da regra` | Validar templates FIN-02 e montar payloads de criação | `rule_input` | `item_plan` ou `rule_result` | FIN-02 (`pipe 306852209`, fase `341306826`), FIN-04 (`pipe 306859915`, fase `341351580`) |
| `[FIN] 1.3 Materializar itens FIN-04` | Criar os cards FIN-04 previstos no plano | `item_plan` | `fin04_items_ready` ou `rule_result` | FIN-04 (`pipe 306859915`, fase `341351580`) |
| `[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02` | Criar FIN-03 e concluir a cadeia de efeitos colaterais | `fin04_items_ready` | `rule_result` | FIN-03 (`pipe 306859731`, fase `341350484`), FIN-04 (`id_da_fatura`), FIN-02 (`parcelas_pagas`, `status`) |

## End-to-end handoff contracts
`runContext`  
Criado pelo orquestrador e entregue a `[FIN] 1.1`. Contém `runId`, `executionId`, `triggerType`, `timezone`, `todayDay`, `competenceYMD`, `competenceStart`, `competenceEnd` e `competenceDateTime`.

`prep_summary`  
Emitido por `[FIN] 1.1` para atualizar o acumulador do orquestrador com `rulesFound` e `rulesEligible`. Traz `result.prepContext`, `result.metricsDelta`, `failures` e, neste workflow específico, também `logs`.

`rule_input`  
Emitido por `[FIN] 1.1` e consumido por `[FIN] 1.2`. Campos relevantes: `contractVersion: fin_v3`, `itemType: rule_input`, `trace`, `run`, `rule.ruleId`, `rule.ruleTitle`, `rule.itemTemplateIds`, `rule.ruleFields` e `rule.prefetchDecision`.

`item_plan`  
Emitido por `[FIN] 1.2` e consumido por `[FIN] 1.3`. Campos relevantes: `itemPlan.plannedItems[]`, `itemPlan.totals`, `itemPlan.invoiceInputBase` e `failures`. Cada `plannedItem` já contém `createInput` pronto para `createCard` em FIN-04, além de flags para atualização de parcelas e inativação do FIN-02 pai.

`fin04_items_ready`  
Emitido por `[FIN] 1.3` e consumido por `[FIN] 1.4`. Campos relevantes: `itemPlan`, `createdItemIds`, `parcelUpdates` e `failures`.

`rule_result`  
Pode ser emitido por qualquer workflow filho. O orquestrador trata esse contrato como saída terminal por regra. Campos recorrentes: `result.status`, `result.reason`, `result.invoiceId`, `result.existingInvoiceId`, `result.createdItemIds`, `result.totals`, `result.metricsDelta`, `result.failures`, `result.startedAt` e `result.finishedAt`.

## Key risks / important implementation characteristics
O desenho é explicitamente fail-closed. Conflitos FIN-04, FIN-03 existente, payloads inesperados, paginação não suportada e campos obrigatórios faltantes encerram a regra como falha, não como skip silencioso.

Os nós GraphQL críticos usam `retryOnFail: true`, `maxTries: 5`, `waitBetweenTries: 2000` e `onError: continueErrorOutput`. O tratamento do erro não fica no nó GraphQL; ele é consolidado nos `Code` nodes seguintes.

Os handoffs internos dependem de `itemType`: `rule_input`, `item_plan`, `fin04_items_ready`, `rule_result` e `prep_summary`. O orquestrador roteia cada item com `If` nodes; só a trilha compatível com o `itemType` segue adiante.

`[FIN] 1.2` e `[FIN] 1.3` bloqueiam explicitamente conflitos/orfandades FIN-04 por `templateItemId + competenceYMD`. A presença de `id_da_fatura` no FIN-04 encontrado diferencia `existing_fin04_conflict` de `orphan_fin04_exists_for_rule_competence`.

`[FIN] 1.4` recria o `invoiceInput` a partir de `itemPlan.invoiceInputBase`, força `pipe_id: 306859731`, `phase_id: 341350484` e anexa `itens_da_fatura` com os FIN-04 criados. O dedupe final é `ruleId + competenceYMD`, validado por query antes do `createCard`.

Inferência: há risco operacional de sucesso parcial. Se FIN-04 ou FIN-03 forem criados e alguma etapa posterior falhar, o rerun tende a bloquear na checagem de duplicidade/orfandade em vez de “continuar de onde parou”, exigindo reconciliação manual.

O orquestrador usa `alwaysOutputData` nas chamadas para `[FIN] 1.2`, `[FIN] 1.3` e `[FIN] 1.4`, depois normaliza retornos vazios em `rule_result` sintético com `child_workflow_returned_no_items`.

A persistência em `fin_billing_rule_log` é best effort: o nó `Persistir fin_billing_rule_log` está com `continueOnFail: true`, então falhas de logging não alteram diretamente o `ok` da run.
