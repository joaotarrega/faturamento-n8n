# [FIN] 1.1 Preparar regras elegiveis

## 1. Purpose
Localiza as regras FIN-01 ativas que devem rodar no dia corrente da competência, elimina regras já faturadas na mesma competência e converte o resultado em contratos consumíveis pelo orquestrador: `prep_summary`, `rule_input` e `rule_result`.

## 2. Trigger and inputs
O workflow inicia por `Execute Workflow Trigger`.

O `jsonExample` mostra o contrato esperado:
`runContext.runId`
`runContext.todayDay`
`runContext.competenceYMD`
`trace.workflowExecutionId`

O upstream é `[FIN] 1.0 Orquestrar faturamento mensal`, via nó `Preparar regras elegiveis`.

Campos realmente necessários para a lógica:
`todayDay`, usado para comparar com `dia_de_gera_o_da_fatura`.
`competenceYMD`, usado para pré-carregar FIN-03 já existentes.
`trace.workflowExecutionId`, usado para encadear a rastreabilidade.

## 3. High-level flow
1. `Init Run` normaliza `runContext` e cria `ctx` com listas vazias de regras, falhas e logs.
2. `FIN-01: Listar regras Ativo (page)` consulta a fase FIN-01 ativa (`341313813`) em páginas de 100 cards.
3. `Parse rules page` trata erro de transporte/lógico, acumula os cards em `ctx.rules` e controla paginação.
4. `Ha proxima pagina de regras?` recircula enquanto houver `hasNextPage` e nenhuma falha fatal.
5. Quando a listagem termina, `FIN-03: Prefetch faturas (competencia)` busca FIN-03 do pipe `306859731` filtrando apenas por `data_de_compet_ncia = competenceYMD`.
6. `Parse prefetch` filtra em código apenas as FIN-03 cujo `id_da_regra_de_faturamento` coincide com a regra e monta `ctx.existingRuleIds`.
7. `Ha proxima pagina de prefetch?` pagina até o fim.
8. `Build regras elegiveis` classifica cada regra FIN-01: inválida, fora do dia, já faturada ou elegível. O retorno é uma lista heterogênea: primeiro `prep_summary`, depois `rule_input` elegíveis e por fim `rule_result` imediatos.

## 4. Node-by-node analysis
- `Execute Workflow Trigger` (`executeWorkflowTrigger`) recebe `runContext` e `trace`. O `jsonExample` é mínimo e não inclui campos extras além do necessário para descoberta de regras.
- `Init Run` (`code`) cria `ctx` com `trace.parentExecutionId`, `trace.workflowExecutionId` atual, `run.runId`, `run.triggerType`, `run.todayDay`, `run.competenceYMD`, `run.competenceDateTime`, `rules: []` e `existingRuleIds: {}`.
- `FIN-01: Listar regras Ativo (page)` (`graphql`) executa `query RulesByPhase($phaseId, $first, $after)` contra `https://api.pipefy.com/graphql`, com `phaseId: 341313813`, `first: 100` e `after` paginado. Está com `retryOnFail: true`, `maxTries: 5`, `waitBetweenTries: 2000` e `onError: continueErrorOutput`.
- `Parse rules page` (`code`) recupera o `ctx` via `linkedJson(...)`, incrementa o índice lógico de chamadas GraphQL, transforma erros em `rules_query_transport_error` ou `rules_query_logical_error`, registra `graphqlLogs` e acumula cada `edge.node` em `ctx.rules`.
- `Ha proxima pagina de regras?` (`if`) testa `hasNext === true && fatal !== true`. Se verdadeiro, volta para `FIN-01: Listar regras Ativo (page)` com o novo cursor.
- `FIN-03: Prefetch faturas (competencia)` (`graphql`) executa `query PrefetchInvoices($pipeId, $fieldId, $competencia, $first, $after)` com `pipeId: 306859731`, `fieldId: data_de_compet_ncia` e `competencia = competenceYMD`.
- `Parse prefetch` (`code`) trata erros como `invoice_prefetch_transport_error` ou `invoice_prefetch_logical_error`, registra `graphqlLogs`, percorre `findCards.edges` e marca `ctx.existingRuleIds[ruleId] = invoiceId` apenas quando o campo `data_de_compet_ncia` do card bate com a competência da run.
- `Ha proxima pagina de prefetch?` (`if`) repete a consulta enquanto `hasNext === true && fatal !== true`.
- `Build regras elegiveis` (`code`) materializa a decisão final por regra. Regras sem `rule.id` viram `rule_result` com `missing_rule_id`. Regras cujo `dia_de_gera_o_da_fatura` não bate com `todayDay` viram `rule_result` `skipped` com `billing_day_mismatch`. Regras que já têm FIN-03 na competência viram `rule_result` com `invoice_already_exists`. Regras novas viram `rule_input` com `itemTemplateIds = extractIds(fm['itens_da_fatura'])` e `ruleFields = fm`. Ao final, o nó também gera `prep_summary`.

## 5. Pipefy interactions
- `RulesByPhase`: consulta a fase FIN-01 ativa `341313813` no pipe `306662125` via `phase(id) { cards(...) }`.
- `PrefetchInvoices`: consulta FIN-03 no pipe `306859731` buscando por `data_de_compet_ncia`.
- Leitura de campos da regra FIN-01 em código:
`dia_de_gera_o_da_fatura`
`itens_da_fatura`
- Leitura de campos da FIN-03 pré-existente em código:
`id_da_regra_de_faturamento`
`data_de_compet_ncia`

## 6. Data contracts and transformations
Entrada:
`runContext` entra como JSON bruto e vira `ctx.run`.
`trace` entra como JSON bruto e vira `ctx.trace`.

Estruturas internas relevantes:
`ctx.rules`: array dos cards FIN-01 coletados em todas as páginas.
`ctx.existingRuleIds`: mapa `ruleId -> invoiceId` construído a partir do prefetch de FIN-03.
`ctx.failures`: falhas técnicas/estruturais da preparação.

Saídas:
`prep_summary` traz `result.status`, `result.reason`, `result.prepContext.rulesFound`, `result.prepContext.rulesEligible`, `result.prepContext.rulesFailed`, `result.metricsDelta` e `failures`.
`rule_input` traz `trace`, `run`, `rule.ruleId`, `rule.ruleTitle`, `rule.itemTemplateIds`, `rule.ruleFields` e `rule.prefetchDecision`.
`rule_result` traz `result.status`, `result.reason`, `existingInvoiceId`, `createdItemIds`, `totals`, `metricsDelta` e `failures`.

Importante: este workflow ainda inclui `logs` em `prep_summary`, `rule_input` e `rule_result` imediatos, ao contrário dos handoffs compactos usados depois por `[FIN] 1.2`, `[FIN] 1.3` e `[FIN] 1.4`.

## 7. Branches, validations, and failure paths
Erros de transporte em `FIN-01: Listar regras Ativo (page)` geram `rules_query_transport_error`.

Erros lógicos GraphQL em `FIN-01: Listar regras Ativo (page)` geram `rules_query_logical_error`.

Erros equivalentes em `FIN-03: Prefetch faturas (competencia)` geram `invoice_prefetch_transport_error` ou `invoice_prefetch_logical_error`.

Quando um desses erros acontece, `Parse rules page` ou `Parse prefetch` marca `fatal: true`, interrompe a paginação e deixa o `prep_summary` final com `status: failed`. O workflow ainda pode classificar regras já acumuladas até aquele ponto.

`missing_rule_id` gera `rule_result` falho imediato.

`billing_day_mismatch` gera `rule_result` com `status: skipped` e não adiciona falha estrutural.

`invoice_already_exists` gera `rule_result` terminal com `existingInvoiceId`, `metricsDelta.duplicateInvoicesBlocked = 1` e falha explícita.

Não existe branch silencioso de descarte: toda regra sai como `rule_input`, `rule_result` ou contribui para `prep_summary`.

## 8. Outputs
`prep_summary` sempre sai uma vez por execução do workflow.

`rule_input` sai uma vez por regra elegível inédita na competência.

`rule_result` sai uma vez por regra inválida, fora da janela do dia ou já faturada.

## 9. Relationship to the other workflows
Recebe o contexto inicial de `[FIN] 1.0`.

Entrega `rule_input` para `[FIN] 1.2` via roteamento do orquestrador.

Entrega `rule_result` imediato para o acumulador do orquestrador, sem passar pelos workflows seguintes.

`prep_summary` é consumido somente pelo acumulador do orquestrador para preencher `rulesFound` e `rulesEligible`.

## 10. Notes and implementation details
O prefetch de FIN-03 é feito por competência, não por `ruleId + competence` na query. O filtro por regra é concluído em código usando `id_da_regra_de_faturamento`.

O dedupe real da etapa é `ruleId + competenceYMD`, mesmo que a query de prefetch só filtre por `data_de_compet_ncia`.

`rule.ruleFields` carrega o field map inteiro da regra FIN-01 e é reaproveitado por `[FIN] 1.2` para montar `invoiceInputBase` mais tarde.

O `trace.workflowName` gravado por `Init Run` é `[FIN] 1.1 Preparar regras elegiveis`, coerente com o nome do workflow.

## 11. Short pseudo-flow
`Execute Workflow Trigger -> Init Run -> listar FIN-01 ativos por página -> prefetch FIN-03 por competência -> classificar regra -> emitir prep_summary / rule_input / rule_result`

## 12. Evidence
- `Execute Workflow Trigger` usa `jsonExample` com `runContext.runId`, `runContext.todayDay`, `runContext.competenceYMD` e `trace.workflowExecutionId`.
- `FIN-01: Listar regras Ativo (page)` consulta `phaseId: 341313813`.
- `FIN-03: Prefetch faturas (competencia)` consulta `pipeId: 306859731` e `fieldId: data_de_compet_ncia`.
- `Build regras elegiveis` lê `dia_de_gera_o_da_fatura`, `itens_da_fatura`, `id_da_regra_de_faturamento` e `data_de_compet_ncia`.
- Reasons explícitos: `missing_rule_id`, `billing_day_mismatch`, `invoice_already_exists`, `rules_query_transport_error`, `rules_query_logical_error`, `invoice_prefetch_transport_error`, `invoice_prefetch_logical_error`, `prep_rules_completed`.
- Marcadores de payload: `contractVersion: fin_v3`, `itemType: prep_summary`, `itemType: rule_input`, `itemType: rule_result`.
