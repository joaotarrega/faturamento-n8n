# [FIN] 1.2 Preparar itens elegiveis da regra

## 1. Purpose
Transforma um `rule_input` em um plano materializável por item-template FIN-02. O workflow valida o contrato recebido, carrega cada FIN-02 vinculado à regra, bloqueia duplicidades/conflitos FIN-04, verifica condições de início e campos obrigatórios visíveis e monta dois payloads: `plannedItems[].createInput` para FIN-04 e `invoiceInputBase` para FIN-03.

## 2. Trigger and inputs
O workflow inicia por `Execute Workflow Trigger`.

O `jsonExample` mostra o contrato esperado:
`contractVersion: fin_v3`
`itemType: rule_input`
`trace.workflowExecutionId`
`run.runId`
`run.todayDay`
`run.competenceYMD`
`rule.ruleId`
`rule.itemTemplateIds[]`

O upstream é `[FIN] 1.0 Orquestrar faturamento mensal`, que repassa o `rule_input` produzido por `[FIN] 1.1 Preparar regras elegiveis`.

Campos obrigatórios na prática:
`rule.ruleId`
`rule.ruleTitle`
`rule.itemTemplateIds[]`
`rule.ruleFields`
`run.competenceYMD`
`run.competenceEnd`

## 3. High-level flow
1. `Init contexto item_plan` valida o envelope `fin_v3/rule_input`, normaliza `rule.itemTemplateIds`, define IDs de configuração do Pipefy e prepara `plannedItems`, `invoiceInputBase` e `terminal`.
2. `Pode preparar itens?` só continua quando a regra não está terminal e há `itemTemplateIds`.
3. `Set pares template IDs` e `Split Out template IDs` explodem a regra em um item por template FIN-02.
4. Para cada template, `FIN-02 Buscar item template` carrega o card FIN-02 e `Normalizar retorno template` recompõe `ctx`, `templateItemId`, resposta e metadados.
5. `Template retornou?` decide entre seguir ou emitir `fetch_template_item_error`.
6. `FIN-04 Verificar conflitos por competencia` busca FIN-04 existentes por `templateItemId`; `Normalizar retorno conflitos`, `Consulta de conflitos retornou?` e `Consulta de conflitos paginou?` bloqueiam erros de consulta e paginação acima de 100 resultados.
7. `Template está Ativo?` exige `current_phase.id = 341306826`.
8. `Mapear conflito FIN-04`, `Há FIN-04 na competência?` e `Tipo de conflito FIN-04` classificam o template como sem conflito, `existing_fin04_conflict` ou `orphan_fin04_exists_for_rule_competence`.
9. `Condições de início`, `Há bloqueio de início?` e `Motivo de bloqueio de início` aplicam guardas de datas específicas e de sinais externos.
10. `Dimensões comerciais do item` lê modelo, moeda, descontos, periodicidade e totais.
11. Os switches/ifs de modelo, tipo e desconto calculam o conjunto de campos visíveis obrigatórios.
12. `Consolidar campos obrigatórios visíveis` une os requisitos e marca a decisão como `ready`.
13. Os merges `Append guardas iniciais item_plan`, `Append conflitos FIN-04 item_plan`, `Append bloqueios de inicio item_plan` e `Append decisões item_plan` juntam todos os caminhos possíveis.
14. `Consolidar elegibilidade e planos` percorre os itens consolidados, registra falhas/bloqueios, valida campos obrigatórios, monta `plannedItems`, calcula os totais da regra e emite `item_plan` ou `rule_result`.

## 4. Node-by-node analysis
- `Execute Workflow Trigger` (`executeWorkflowTrigger`) espera um `rule_input` `fin_v3`.
- `Init contexto item_plan` (`code`) cria `ctx` com `config.fin02ActivePhaseId = 341306826`, `config.fin04PipeId = 306859915`, `config.fin04PhaseId = 341351580`, `config.rulePhaseId = 341313813`, além de `plannedItems: []`, `invoiceInputBase: null` e `terminal`.
- `Pode preparar itens?` (`if`) só segue quando `terminal.done !== true` e `rule.itemTemplateIds.length > 0`.
- `Emitir rule_result direto` (`set`) retorna `rule_result` imediato quando o input já chegou inválido ou vazio.
- `Set pares template IDs` (`set`) monta `emitTemplatePairs = [{ ctx, templateItemId }]`.
- `Split Out template IDs` (`splitOut`) gera uma execução por template FIN-02.
- `FIN-02 Buscar item template` (`graphql`) executa `query GetTemplateItem($id)` sobre o card FIN-02. Usa retry/`continueErrorOutput`.
- `Normalizar retorno template` (`set`) reanexa `ctx`, `templateItemId`, `templateResponse`, `templateRequestMeta` e `templatePhaseId`.
- `Template retornou?` (`if`) exige ausência de erro local e presença de `data.card.id`.
- `Definir fetch_template_item_error` (`set`) grava `decision.status = failed` e `decision.reason = fetch_template_item_error`.
- `FIN-04 Verificar conflitos por competencia` (`graphql`) executa `query FindFin04ByTemplate($pipeId, $fieldId, $templateId, $first)` em `pipeId: 306859915`, `fieldId: id_do_item_usado_como_template_fin_02`.
- `Normalizar retorno conflitos` (`set`) reanexa `ctx`, `templateItemId`, `templatePhaseId`, `conflictRequestMeta` e `conflictResponse`.
- `Consulta de conflitos retornou?` (`if`) bloqueia transporte, erros GraphQL e erros normalizados.
- `Definir conflict_fin04_query_error` (`set`) marca a decisão como falha de consulta.
- `Consulta de conflitos paginou?` (`if`) falha explicitamente quando `hasNextPage === true`.
- `Definir conflict_fin04_query_requires_pagination` (`set`) impede o uso de resultado parcial.
- `Template está Ativo?` (`if`) exige `templatePhaseId === 341306826`.
- `Definir inactive_template_phase` (`set`) marca o template como `skipped`.
- `Mapear conflito FIN-04` (`set`) percorre os FIN-04 retornados, filtra pelos campos `id_do_item_usado_como_template_fin_02` e `data_de_compet_ncia`, lê `id_da_fatura` e produz `decision.conflict = { hasConflict, id, invoiceId, kind }`.
- `Há FIN-04 na competência?` (`if`) decide se entra no switch de conflito.
- `Tipo de conflito FIN-04` (`switch`) distingue `existing_fin04_conflict` de `orphan_fin04_exists_for_rule_competence`.
- `Definir existing_fin04_conflict` e `Definir orphan_fin04_exists_for_rule_competence` (`set`) transformam a decisão em `blocked`.
- `Condições de início` (`set`) lê `condi_es_exigidas_para_iniciar_o_faturamento_do_item`, `data_de_in_cio_do_faturamento` e `data_de_fim_do_faturamento` e monta `decision.startConditions`.
- `Há bloqueio de início?` (`if`) verifica se existem bloqueios calculados.
- `Motivo de bloqueio de início` (`switch`) escolhe um dos quatro motivos suportados: `specific_date_not_ready`, `specific_date_finished`, `blocked_missing_external_signal_onboarding_training`, `blocked_missing_external_signal_setup_payment`.
- `Dimensões comerciais do item` (`set`) cria `decision.commercial` com `currency`, `model`, `type`, flags de desconto, base percentual, categorias SaaS, `periodicidade`, `parcelasPagas`, `parcelasTotais`, `totalItemBrl` e `totalItemUsd`. Também define `requiredBase`.
- `Modelo de cobrança`, `Base percentual é SaaS?`, `Unidade é Afiliados?` e nós `Campos modelo ...` determinam `requiredByModel` conforme o modelo do item.
- `Tipo de item` e nós `Campos tipo produto` / `Campos tipo ajuste` determinam `requiredByType`.
- `Há desconto?`, `Desconto nominal?`, `Desconto percentual?`, `Desconto por categorias?` e nós `Campos desconto ...` determinam `requiredByDiscount`.
- `Merge requisitos modelo + tipo` e `Merge requisitos + desconto` (`merge` em `combineByPosition`) recombinam os ramos paralelos do mesmo template.
- `Consolidar campos obrigatórios visíveis` (`set`) junta `requiredBase`, `requiredByModel`, `requiredByType` e `requiredByDiscount` em `decision.visibleRequired`, preservando `status` e `reason`.
- `Append guardas iniciais item_plan`, `Append conflitos FIN-04 item_plan`, `Append bloqueios de inicio item_plan` e `Append decisões item_plan` (`merge` em `append`) reúnem os possíveis caminhos de decisão.
- `Consolidar elegibilidade e planos` (`code`) é o nó central. Ele aplica whitelist de zero técnico, valida campos obrigatórios, registra `entityLogs`/`graphqlLogs`, soma totais BRL/USD, cria `plannedItems`, cria `invoiceInputBase`, define `duplicateItemsBlocked` e emite `item_plan` ou `rule_result`.

## 5. Pipefy interactions
- `GetTemplateItem`: `card(id: $id)` no pipe FIN-02. O código usa `current_phase.id`, `title` e o mapa completo de fields.
- `FindFin04ByTemplate`: `findCards(pipeId: 306859915, search: { fieldId: id_do_item_usado_como_template_fin_02, fieldValue: templateItemId })`.
- Fase FIN-02 ativa exigida: `341306826`.
- Fase FIN-04 de criação exigida no payload final: `341351580`.
- Campos FIN-02 lidos na decisão:
`condi_es_exigidas_para_iniciar_o_faturamento_do_item`
`data_de_in_cio_do_faturamento`
`data_de_fim_do_faturamento`
`modelo_de_cobran_a`
`tipo_de_item`
`moeda_da_fatura`
`base_do_c_lculo_percentual`
`unidade_de_cobran_a`
`parcelas_pagas`
`quantidade_de_parcelas_totais`
`h_algum_desconto_nesse_item`
`tipo_de_desconto_no_item_1`
`categorias_do_saas_isentas_ou_com_desconto`
`subtotal_do_item_brl`
`subtotal_do_item_usd`
`valor_total_do_item_com_descontos_brl`
`valor_total_do_item_com_descontos_usd`
- Campos FIN-04 lidos na checagem de conflito:
`id_do_item_usado_como_template_fin_02`
`data_de_compet_ncia`
`id_da_fatura`

## 6. Data contracts and transformations
Entrada:
`rule_input` com `rule.ruleFields` vindo de `[FIN] 1.1`.

Estruturas intermediárias:
`decision.templatePhase`: fase atual do FIN-02.
`decision.conflict`: resultado da busca FIN-04 por template/competência.
`decision.startConditions`: datas específicas e bloqueios externos.
`decision.commercial`: modelo, moeda, desconto, periodicidade e totais.
`decision.visibleRequired`: conjunto final de campos que precisam estar preenchidos.

`Consolidar elegibilidade e planos` usa um `targetMap` para copiar campos do FIN-02 para o `createInput` de FIN-04. O payload sempre inclui:
`data_de_compet_ncia`
`id_do_item_usado_como_template_fin_02`

Além disso, o `targetMap` cobre as famílias:
identificação do item (`produto`, `tipo_de_item`, `modelo_de_cobran_a`, `moeda_da_fatura`)
base/modelo (`base_do_c_lculo_percentual`, `percentual_a_ser_aplicado_na_base`, `unidade_de_cobran_a`, `quantidade`, `afiliados_que_comp_em_a_cobran_a`, `id_backoffice_do_afiliado`)
descontos (`tipo_de_desconto_no_item`, `percentual_de_desconto_a_ser_aplicado`, `valor_nominal_a_ser_descontado_brl/usd`, categorias SaaS e campos por categoria)
valores (`valor_unit_rio_brl/usd`, `valor_total_do_item_brl/usd`, `valor_total_do_item_com_descontos_brl/usd`)

`ZERO_WHITELIST` permite preencher com zero técnico apenas:
`valor_de_base_brl`
`valor_de_base_usd`
`valor_base_saas_categoria_*`

Saída de sucesso:
`item_plan.itemPlan.plannedItems[]`: cada item traz `templateItemId`, `dedupeKey`, `totalItemBrl`, `totalItemUsd`, `periodicidade`, `parcelasPagas`, `parcelasTotais`, `shouldUpdateParcelas`, `newParcelasPagas`, `shouldInactivateParent` e `createInput`.
`item_plan.itemPlan.invoiceInputBase`: base da futura FIN-03 com `pipe_id: 306859731`, `title` e `fields_attributes`.
`item_plan.itemPlan.totals`: somatório BRL/USD da regra.

`invoiceInputBase.fields_attributes` inclui:
`clientes_na_fatura`
`contatos_que_receber_o_a_fatura`
`identificador_fiscal_na_fatura`
`tipo_de_cliente_final`
`rede_na_fatura`
`forma_de_pagamento`
`moeda_do_teto_de_faturamento`
`teto_de_faturamento_brl`
`teto_de_faturamento_usd`
`id_da_regra_de_faturamento`
`data_de_compet_ncia`
`valor_total_da_fatura_brl`
`valor_total_da_fatura_usd`

## 7. Branches, validations, and failure paths
`unexpected_payload_shape`, `invalid_rule_input` e `no_items_ready_for_invoice` são validados em `Init contexto item_plan`.

`fetch_template_item_error` bloqueia template que não pôde ser carregado.

`conflict_fin04_query_error` e `conflict_fin04_query_requires_pagination` bloqueiam a regra por não confiar em leitura parcial/errada de FIN-04.

`inactive_template_phase` é tratado como `skipped` no nível do template, não como falha explícita do array `failures`.

`existing_fin04_conflict` e `orphan_fin04_exists_for_rule_competence` contam em `duplicateItemsBlocked` e entram em `failures`.

`specific_date_not_ready`, `specific_date_finished`, `blocked_missing_external_signal_onboarding_training` e `blocked_missing_external_signal_setup_payment` bloqueiam a criação.

`missing_required_nonzero_field` é disparado quando um campo visível obrigatório fica vazio e não pertence ao `ZERO_WHITELIST`.

Se nenhum `plannedItem` sobreviver, o workflow termina com `rule_result`. O reason final é a primeira falha registrada ou `no_items_ready_for_invoice`.

Os snapshots atuais da tabela conectada `KsEXACBL` confirmam o significado textual das opções de `condi_es_exigidas_para_iniciar_o_faturamento_do_item`: `1258585767 = Treinamento de onboarding realizado`, `1258585802 = Data específica` e `1258585836 = Finalização do pagamento de setup`.

## 8. Outputs
Em sucesso, o workflow emite um único `item_plan`.

Em falha terminal, o workflow emite um único `rule_result` com `status: failed`.

Não há saída intermediária por template; todo o consolidado é colapsado em uma única saída por regra.

## 9. Relationship to the other workflows
Consome `rule_input` vindo de `[FIN] 1.1`.

Entrega `item_plan` para `[FIN] 1.3`.

Se falhar ou bloquear a regra, devolve `rule_result` diretamente ao orquestrador, encerrando a cadeia da regra antes da criação de FIN-04 e FIN-03.

## 10. Notes and implementation details
O workflow replica a lógica de visibilidade do formulário por meio de switches/ifs. Isso evita `Code` excessivo, mas espalha a regra por muitos nós pequenos.

`inactive_template_phase` não entra em `ctx.failures`. Se todos os templates estiverem inativos, a saída final tende a ser `rule_result` com `reason: no_items_ready_for_invoice`.

O snapshot atual da tabela conectada `jvn6AIAT` confirma que `1268890278` em `base_do_c_lculo_percentual` representa `Valor dos eventos definitivos do afiliado`, que é o valor que ativa a exigência dos campos `valor_base_saas_categoria_*`.

O snapshot atual da tabela conectada `oSEdXvjs` confirma que `1260337139 = Valor percentual` e `1260337296 = Isenção de impostos locais`. No workflow, esses IDs continuam controlando a inclusão dos campos `desconto_no_valor_vari_vel_do_saas_para_*` e `valor_do_imposto_a_ser_isento_*`.

## 11. Short pseudo-flow
`rule_input -> validar envelope -> buscar cada FIN-02 -> verificar conflito FIN-04 -> avaliar guardas de início -> calcular campos obrigatórios -> montar createInput/invoiceInputBase -> emitir item_plan`

## 12. Evidence
- `Init contexto item_plan` exige `contractVersion: fin_v3` e `itemType: rule_input`.
- `FIN-02 Buscar item template` usa `query GetTemplateItem`.
- `FIN-04 Verificar conflitos por competencia` usa `query FindFin04ByTemplate` com `pipeId: 306859915` e `fieldId: id_do_item_usado_como_template_fin_02`.
- `Template está Ativo?` compara `templatePhaseId` com `341306826`.
- `Condições de início` usa o campo `condi_es_exigidas_para_iniciar_o_faturamento_do_item` e os IDs `1258585802`, `1258585767`, `1258585836`.
- Reasons explícitos: `fetch_template_item_error`, `conflict_fin04_query_error`, `conflict_fin04_query_requires_pagination`, `inactive_template_phase`, `existing_fin04_conflict`, `orphan_fin04_exists_for_rule_competence`, `specific_date_not_ready`, `specific_date_finished`, `blocked_missing_external_signal_onboarding_training`, `blocked_missing_external_signal_setup_payment`, `missing_required_nonzero_field`, `no_items_ready_for_invoice`.
- `Consolidar elegibilidade e planos` define `ZERO_WHITELIST` para `valor_de_base_*` e `valor_base_saas_categoria_*`.
- Marcadores de payload: `itemType: rule_result` e `itemType: item_plan`.
