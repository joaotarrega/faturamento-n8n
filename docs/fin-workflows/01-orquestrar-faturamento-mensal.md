# [FIN] 1.0 Orquestrar faturamento mensal

## 1. Purpose
É o workflow pai da suíte. Ele calcula a janela de competência, chama os quatro workflows filhos na ordem correta, roteia itens pelo `itemType`, agrega métricas/resultados por regra, persiste o log técnico em Data Table e encerra a run com sucesso ou erro.

## 2. Trigger and inputs
O workflow começa por `Cron (dias 1-5 07:00)` ou `Manual Trigger`.

Não há payload de entrada externo obrigatório. `Init Run` cria internamente o contexto da execução e produz:
`runContext`: `runId`, `executionId`, `triggerType`, `timezone`, `todayDay`, `competenceYMD`, `competenceStart`, `competenceEnd`, `competenceDateTime`.
`trace`: `workflowExecutionId`, `workflowName`, `parentExecutionId`.
`orchestratorContext`: acumulador inicial com métricas zeradas.

## 3. High-level flow
1. O gatilho dispara e `Init Run` calcula a competência como o primeiro dia do mês anterior na timezone `America/Sao_Paulo`.
2. O nó `Preparar regras elegiveis` executa `[FIN] 1.1 Preparar regras elegiveis`.
3. `Loop regras` itera item a item sobre a saída de `[FIN] 1.1`.
4. `Loop: item e rule_input?` envia apenas `rule_input` para `[FIN] 1.2 Preparar itens elegiveis da regra`; `prep_summary` e `rule_result` vão direto para `Accumulate results`.
5. `Normalizar retorno preparar itens` garante que a chamada do filho devolveu `item_plan` ou `rule_result`; se vier vazio/inválido, sintetiza `rule_result` com `failed_prepare_items_stage`.
6. `Loop: item e item_plan?` envia apenas `item_plan` para `[FIN] 1.3 Materializar itens FIN-04`; `rule_result` volta para `Accumulate results`.
7. `Normalizar retorno materializacao` garante retorno `fin04_items_ready` ou `rule_result`; caso contrário, sintetiza `rule_result` com `failed_materialize_items_stage`.
8. `Loop: item e fin04_items_ready?` envia apenas `fin04_items_ready` para `[FIN] 1.4 Criar fatura FIN-03 e concluir regra`; `rule_result` volta para `Accumulate results`.
9. `Normalizar retorno finalizacao` garante retorno `rule_result`; se o filho voltar vazio, gera `rule_result` com `failed_invoice_stage`.
10. `Accumulate results` acumula `prep_summary` e `rule_result` em `workflow static data`, incrementa métricas e reabre o `Loop regras`.
11. Quando o loop termina, `Finalize summary` monta o resumo final e `Emit rule logs` expõe `ruleLogRows`.
12. Se houver linhas, `Split rule logs` grava uma por vez em `Persistir fin_billing_rule_log`; depois `Restaurar summary final` recompõe o payload final.
13. `Run: tem erros?` envia a execução para `Run: Stop and Error` quando `ok !== true`; caso contrário, `Run: Success output` retorna o resumo da run.

## 4. Node-by-node analysis
- `Cron (dias 1-5 07:00)` (`cron`) dispara diariamente às 07:00 nos dias 1 a 5 do mês. Ele existe para cobrir a janela operacional de início de mês.
- `Manual Trigger` (`manualTrigger`) permite rerun manual com o mesmo cálculo automático de competência.
- `Init Run` (`code`) determina `todayDay` na timezone `America/Sao_Paulo`, calcula `competenceYMD` como `YYYY-MM-01` do mês anterior, calcula `competenceEnd`, define `triggerType` (`cron` ou `manual`) e cria o `runId` no formato `fin-billing:<executionId>:<competenceYMD>`.
- `Preparar regras elegiveis` (`executeWorkflow`) chama o workflow filho de preparação de regras e recebe um lote heterogêneo com `prep_summary`, `rule_input` e `rule_result`.
- `Loop regras` (`splitInBatches`) dirige a iteração da lista de saída de `[FIN] 1.1`. O JSON não define `batchSize`, então o comportamento depende do padrão do nó.
- `Loop: item e rule_input?` (`if`) verifica `{{$json.itemType === 'rule_input'}}`. Verdadeiro segue para preparação de itens; falso envia o item diretamente para o acumulador.
- `Preparar itens elegiveis da regra` (`executeWorkflow`) chama `[FIN] 1.2`. Está com `alwaysOutputData: true` para permitir normalização mesmo quando o filho falha ou não emite itens.
- `Normalizar retorno preparar itens` (`code`) aceita somente `item_plan` ou `rule_result`. Se o filho devolveu payload inesperado ou nada útil, produz um `rule_result` sintético com `reason: failed_prepare_items_stage` e adiciona `child_workflow_returned_no_items` à lista de falhas.
- `Loop: item e item_plan?` (`if`) deixa passar somente `item_plan` para a etapa de materialização; `rule_result` curto-circuita para o acumulador.
- `Materializar itens FIN-04` (`executeWorkflow`) chama `[FIN] 1.3`. Também usa `alwaysOutputData: true`.
- `Normalizar retorno materializacao` (`code`) aceita `fin04_items_ready` ou `rule_result`. Em retorno vazio/inesperado, gera `rule_result` com `failed_materialize_items_stage`.
- `Loop: item e fin04_items_ready?` (`if`) envia `fin04_items_ready` para a finalização. `rule_result` volta para o acumulador.
- `Criar fatura FIN-03 e concluir regra` (`executeWorkflow`) chama `[FIN] 1.4`. Está com `alwaysOutputData: true`.
- `Normalizar retorno finalizacao` (`code`) exige `rule_result`. Se não houver item válido, sintetiza falha `failed_invoice_stage`.
- `Accumulate results` (`code`) usa `$getWorkflowStaticData('global').finBillingAcc[executionId]` para manter estado entre iterações. `prep_summary` alimenta `rulesFound` e `rulesEligible`. Cada `rule_result` incrementa `rulesProcessed`, soma `metricsDelta`, replica `failures` e guarda um snapshot resumido por regra.
- `Finalize summary` (`code`) lê o acumulador, conta regras `processed`, `skipped` e `failed`, calcula `ok = failedRules === 0 && failureCount === 0`, monta `summary`, `run` e `ruleLogRows`, e limpa o estado estático da execução.
- `Emit rule logs` (`set`) expõe `ruleLogRows` para o fluxo de persistência.
- `Ha rule logs?` (`if`) só passa para persistência quando `ruleLogRows.length > 0`.
- `Split rule logs` (`splitOut`) transforma o array em uma linha por item para gravação em lote.
- `Persistir fin_billing_rule_log` (`dataTable`) grava na Data Table `5CPaGUez5Ex8tJHi`. O mapeamento inclui `run_id`, `workflow_execution_id`, `competence_date`, `rule_id`, `rule_title`, `status`, `reason`, `failure_count`, `failed_nodes`, `failed_reasons`, `pipefy_card_ids`, `invoice_id`, `created_item_ids_json`, `failures_json`, `started_at` e `finished_at`. Está com `continueOnFail: true`.
- `Restaurar summary final` (`set`) recompõe `summary`, `run` e `ok` a partir do primeiro item de `Finalize summary`, desacoplando o payload final do desdobramento de logs.
- `Run: tem erros?` (`if`) testa `{{$json.ok !== true}}`.
- `Run: Stop and Error` (`stopAndError`) encerra a execução com uma mensagem do tipo `FIN billing mensal falhou. failureCount=..., firstFailure=..., competence=...`.
- `Run: Success output` (`set`) devolve `ok: true`, `summary` e `run`.

## 5. Pipefy interactions
O workflow não faz GraphQL no Pipefy.

A única integração externa direta é `Persistir fin_billing_rule_log`, que grava na Data Table `[Logs] FIN - Regras mensais` com ID `5CPaGUez5Ex8tJHi`.

As interações com pipes e cards do Pipefy acontecem indiretamente via `[FIN] 1.1`, `[FIN] 1.2`, `[FIN] 1.3` e `[FIN] 1.4`.

## 6. Data contracts and transformations
`Init Run` cria o contrato-base da run. Os campos relevantes são:
`runContext.triggerType` distingue execução de `cron` e `manual`.
`runContext.competenceYMD` é sempre o primeiro dia do mês anterior.
`runContext.competenceEnd` é usado depois em `[FIN] 1.2` para avaliar datas específicas de início.

O roteamento interno depende de `itemType`:
`prep_summary` atualiza métricas globais.
`rule_input` dispara `[FIN] 1.2`.
`item_plan` dispara `[FIN] 1.3`.
`fin04_items_ready` dispara `[FIN] 1.4`.
`rule_result` é sempre terminal para a regra e vai para o acumulador.

`Accumulate results` colapsa o `rule_result` para um snapshot menor com `ruleId`, `ruleTitle`, `status`, `reason`, `invoiceId`, `createdItemIds`, `failures`, `startedAt` e `finishedAt`.

`Finalize summary` transforma esse acumulado em:
`summary`: totais da run, métricas agregadas e `firstFailure`.
`run`: `failures`, `results` e `metrics`.
`ruleLogRows`: uma linha por regra pronta para Data Table.

## 7. Branches, validations, and failure paths
Os três `If` principais (`Loop: item e rule_input?`, `Loop: item e item_plan?`, `Loop: item e fin04_items_ready?`) garantem que cada item siga apenas pela etapa compatível com seu contrato.

Os três normalizadores convertem retorno vazio ou fora do contrato em `rule_result` explícito, usando `child_workflow_returned_no_items` e reason de estágio (`failed_prepare_items_stage`, `failed_materialize_items_stage`, `failed_invoice_stage`).

O workflow não encerra a regra silenciosamente. Tudo o que não for `rule_input`, `item_plan` ou `fin04_items_ready` em ponto indevido vira falha estruturada ou bypass para `rule_result`.

O erro final da run é decidido por `Finalize summary`: qualquer `rule_result` com `status: failed` ou qualquer falha agregada deixa `ok = false`.

Falhas de gravação na Data Table não são usadas para compor `ok`, porque `Persistir fin_billing_rule_log` está com `continueOnFail: true`.

## 8. Outputs
Em sucesso, `Run: Success output` emite `ok: true`, `summary` e `run`.

Em falha, `Run: Stop and Error` aborta a execução com mensagem textual; o payload final deixa de seguir adiante.

Como artefato intermediário, `Finalize summary` sempre produz `ruleLogRows`, mesmo quando a run falha.

## 9. Relationship to the other workflows
É o workflow pai dos quatro demais.

Ele entrega `runContext` e `trace` a `[FIN] 1.1`.

Ele consome a saída heterogênea de `[FIN] 1.1` e, regra a regra, passa `rule_input` para `[FIN] 1.2`, `item_plan` para `[FIN] 1.3` e `fin04_items_ready` para `[FIN] 1.4`.

Todo `rule_result`, inclusive os produzidos diretamente por `[FIN] 1.1`, volta para o acumulador aqui.

## 10. Notes and implementation details
Há uma inconsistência interna de nomenclatura: o `name` do workflow é `[FIN] 1.0 Orquestrar faturamento mensal`, mas `Init Run` grava `trace.workflowName = '[FIN] 1 Orquestrar faturamento mensal'`.

O acumulador usa `workflow static data` global (`sd.finBillingAcc`) indexado por `executionId`. `Finalize summary` remove a entrada ao terminar; se a execução for interrompida antes disso, pode sobrar estado transitório até um rerun.

`Accumulate results` não reconta `rulesFound` nem `rulesEligible` a partir dos `rule_result`; ele depende do `prep_summary` emitido por `[FIN] 1.1`.

O workflow separa explicitamente agregação de resultados de persistência de logs. Isso reduz o acoplamento entre “estado da run” e “sucesso do sink”.

## 11. Short pseudo-flow
`cron/manual -> Init Run -> [FIN] 1.1 -> loop por itemType -> [FIN] 1.2 -> [FIN] 1.3 -> [FIN] 1.4 -> accumulate rule_result -> finalize summary -> persist fin_billing_rule_log -> success/error`

## 12. Evidence
- Nós de trigger: `Cron (dias 1-5 07:00)` com `cronExpression: 0 7 1-5 * *` e `Manual Trigger`.
- `Init Run` define `competenceYMD` a partir do mês anterior e `competenceEnd`.
- `Loop: item e rule_input?`, `Loop: item e item_plan?` e `Loop: item e fin04_items_ready?` testam `itemType`.
- `Preparar itens elegiveis da regra`, `Materializar itens FIN-04` e `Criar fatura FIN-03 e concluir regra` estão com `alwaysOutputData: true`.
- `Normalizar retorno preparar itens` aceita somente `item_plan` ou `rule_result` e sintetiza `failed_prepare_items_stage`.
- `Normalizar retorno materializacao` aceita somente `fin04_items_ready` ou `rule_result` e sintetiza `failed_materialize_items_stage`.
- `Normalizar retorno finalizacao` aceita somente `rule_result` e sintetiza `failed_invoice_stage`.
- `Finalize summary` calcula `ok = failed === 0 && failures.length === 0`.
- `Persistir fin_billing_rule_log` grava na Data Table `5CPaGUez5Ex8tJHi` com colunas `run_id`, `workflow_execution_id`, `competence_date`, `rule_id`, `rule_title`, `status`, `reason`, `failure_count`, `failed_nodes`, `failed_reasons`, `pipefy_card_ids`, `invoice_id`, `created_item_ids_json`, `failures_json`, `started_at`, `finished_at`.
