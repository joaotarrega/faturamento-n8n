# [FIN] 1.3 Materializar itens FIN-04

## 1. Purpose
Executa a criação física dos cards FIN-04 a partir de um `item_plan`. Antes de cada `createCard`, ele revalida se já surgiu um FIN-04 para o mesmo template e competência. Em sucesso, devolve `fin04_items_ready`; em qualquer falha estrutural ou parcial, devolve `rule_result`.

## 2. Trigger and inputs
O workflow inicia por `Execute Workflow Trigger`.

O `jsonExample` documenta o contrato:
`contractVersion: fin_v3`
`itemType: item_plan`
`trace`
`run`
`rule`
`itemPlan.plannedItems[]`
`failures[]`

O upstream é `[FIN] 1.0 Orquestrar faturamento mensal`, repassando a saída de `[FIN] 1.2 Preparar itens elegiveis da regra`.

Campos obrigatórios na prática:
`itemPlan.plannedItems[]`
`plannedItems[].templateItemId`
`plannedItems[].dedupeKey`
`plannedItems[].createInput`

## 3. High-level flow
1. `Init materializacao` valida o envelope `fin_v3/item_plan`, carrega o plano, herda `failures` e inicializa `createdItemIds` e `parcelUpdates`.
2. `Pode materializar itens?` segue apenas se houver `plannedItems`.
3. `Set pares create requests` e `Split Out create requests` quebram o plano em um item por criação de FIN-04.
4. `FIN-04 Recheck conflito antes de criar` busca novamente FIN-04 por template.
5. `Merge retorno recheck` recompõe request e resposta; `Parse recheck conflito` decide se pode criar, se houve conflito existente/orfandade ou se houve erro de consulta/paginação.
6. `Pode criar item FIN-04?` envia apenas os itens autorizados para `FIN-04 Criar item`.
7. `Merge criacao item` recompõe request e resposta de criação.
8. `Consolidar materializacao` transforma o lote inteiro em um único `fin04_items_ready` ou `rule_result`.

## 4. Node-by-node analysis
- `Execute Workflow Trigger` (`executeWorkflowTrigger`) espera `item_plan`.
- `Init materializacao` (`code`) copia `run`, `rule`, `itemPlan`, `failures`, `logs` herdados, inicia `createdItemIds: []`, `parcelUpdates: []` e valida `contractVersion`, `itemType` e `plan.plannedItems`.
- `Pode materializar itens?` (`if`) só continua quando `terminal.done !== true` e `plannedItems.length > 0`.
- `Emitir rule_result direto` (`code`) transforma input inválido ou vazio em `rule_result` imediato com o mesmo envelope `fin_v3`.
- `Set pares create requests` (`set`) monta `emitCreatePairs = [{ ctx, plannedItem }]`.
- `Split Out create requests` (`splitOut`) processa cada item planejado separadamente.
- `FIN-04 Recheck conflito antes de criar` (`graphql`) faz a mesma busca por template usada em `[FIN] 1.2`, agora imediatamente antes do `createCard`.
- `Merge retorno recheck` (`merge` em `combineByPosition`) une o request do item com a resposta da consulta.
- `Parse recheck conflito` (`code`) trata erro de transporte/lógico como `conflict_fin04_query_error`, falha de paginação como `conflict_fin04_query_requires_pagination`, e conflito real quando acha um FIN-04 com mesmo `templateItemId` e `competenceYMD`. Se houver `id_da_fatura`, o conflito é `existing_fin04_conflict`; sem `id_da_fatura`, vira `orphan_fin04_exists_for_rule_competence`. Só quando nada disso ocorre ele marca `canCreateItem = true` e já reserva `createGraphqlCallIndex`.
- `Pode criar item FIN-04?` (`if`) deixa passar apenas os itens com `canCreateItem === true`.
- `FIN-04 Criar item` (`graphql`) executa `mutation CreateFin04($input: CreateCardInput!)` usando exatamente `plannedItem.createInput`.
- `Merge criacao item` (`merge` em `combineByPosition`) volta a unir request e resposta.
- `Consolidar materializacao` (`code`) percorre todos os itens: registra conflitos rechecados, falhas de validação, falhas de transporte/lógicas no `createCard`, acumula `createdItemIds`, registra `entityLogs` por FIN-04 criado e gera `parcelUpdates` para itens parcelados. Se houve qualquer falha ou nenhum item criado, emite `rule_result`; caso contrário, emite `fin04_items_ready`.

## 5. Pipefy interactions
- `FindFin04ByTemplate`: query `findCards` no pipe FIN-04 `306859915`, buscando por `fieldId: id_do_item_usado_como_template_fin_02`.
- `CreateFin04`: mutation `createCard(input: $json.plannedItem.createInput)`.
- O recheck lê, em código, os campos:
`id_do_item_usado_como_template_fin_02`
`data_de_compet_ncia`
`id_da_fatura`
- A criação usa `pipe_id: 306859915` e `phase_id: 341351580`, já definidos em `[FIN] 1.2`.

## 6. Data contracts and transformations
Entrada:
`item_plan.itemPlan.plannedItems[]`
`item_plan.itemPlan.totals`
`item_plan.itemPlan.invoiceInputBase`
`failures[]`

Estado interno relevante:
`ctx.createdItemIds[]`: IDs reais dos FIN-04 criados.
`ctx.parcelUpdates[]`: apenas para `plannedItem.shouldUpdateParcelas === true`.

Cada item de `parcelUpdates` contém:
`templateItemId`
`newParcelasPagas`
`shouldInactivateParent`
`parcelasTotais`
`dedupeKey`

Saída de sucesso:
`contractVersion: fin_v3`
`itemType: fin04_items_ready`
`trace`
`run`
`rule`
`itemPlan`
`createdItemIds`
`parcelUpdates`
`failures`

Saída de falha:
`itemType: rule_result`
`result.reason` vindo da primeira falha ou de `no_items_ready_for_invoice`
`result.createdItemIds` preservando IDs já criados antes da falha

## 7. Branches, validations, and failure paths
`Init materializacao` falha com `unexpected_payload_shape`, `invalid_item_plan` ou `no_items_ready_for_invoice`.

`Parse recheck conflito` falha com `conflict_fin04_query_error` em erro de transporte/lógica GraphQL.

`Parse recheck conflito` falha com `conflict_fin04_query_requires_pagination` quando a busca retornaria mais de 100 cards.

`Parse recheck conflito` bloqueia criação quando encontra FIN-04 existente/orfandade na mesma competência.

`Consolidar materializacao` trata erro no `createCard` como `create_item_transport_error` ou `create_item_logical_error`.

O workflow considera falha global da regra quando qualquer item falha, mesmo que outros tenham sido criados com sucesso.

## 8. Outputs
Em sucesso total, sai um único `fin04_items_ready`.

Em falha total ou parcial, sai um único `rule_result`.

O payload nunca emite um item por FIN-04 criado; o resultado é sempre consolidado por regra.

## 9. Relationship to the other workflows
Recebe `item_plan` de `[FIN] 1.2`.

Entrega `fin04_items_ready` para `[FIN] 1.4`.

Se falhar, devolve `rule_result` diretamente ao orquestrador e interrompe a cadeia da regra antes da criação de FIN-03.

## 10. Notes and implementation details
O workflow faz recheck de conflito na borda do efeito colateral. Isso reduz a janela entre “verificação” e “criação”, mas não elimina corridas entre execuções concorrentes.

Inferência: se alguns FIN-04 forem criados e depois outro item falhar, o rerun tende a bloquear por `existing_fin04_conflict` ou `orphan_fin04_exists_for_rule_competence`, porque a nova leitura já verá os FIN-04 persistidos.

`parcelUpdates` não atualiza Pipefy aqui; ele só prepara a próxima etapa (`[FIN] 1.4`) para mexer em `parcelas_pagas`.

## 11. Short pseudo-flow
`item_plan -> validar envelope -> recheck FIN-04 por template -> criar FIN-04 autorizados -> consolidar createdItemIds/parcelUpdates -> emitir fin04_items_ready ou rule_result`

## 12. Evidence
- `Init materializacao` exige `contractVersion: fin_v3` e `itemType: item_plan`.
- `FIN-04 Recheck conflito antes de criar` usa `query FindFin04ByTemplate`.
- `Parse recheck conflito` lê `id_do_item_usado_como_template_fin_02`, `data_de_compet_ncia` e `id_da_fatura`.
- Reasons explícitos: `conflict_fin04_query_error`, `conflict_fin04_query_requires_pagination`, `existing_fin04_conflict`, `orphan_fin04_exists_for_rule_competence`, `create_item_transport_error`, `create_item_logical_error`, `invalid_item_plan`, `no_items_ready_for_invoice`.
- `FIN-04 Criar item` usa `mutation CreateFin04`.
- Marcadores de payload: `itemType: fin04_items_ready` e `itemType: rule_result`.
