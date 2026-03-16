# [FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02

## 1. Purpose
Finaliza a regra já materializada: cria a FIN-03, grava o `id_da_fatura` nos FIN-04 criados, incrementa `parcelas_pagas` nos FIN-02 parcelados e, quando a última parcela é consumida, muda o FIN-02 pai para `Inativo`. O resultado final é sempre um `rule_result`.

## 2. Trigger and inputs
O workflow inicia por `Execute Workflow Trigger`.

O `jsonExample` documenta o contrato:
`contractVersion: fin_v3`
`itemType: fin04_items_ready`
`trace`
`run`
`rule`
`itemPlan.invoiceInputBase`
`createdItemIds[]`
`failures[]`

O upstream é `[FIN] 1.0 Orquestrar faturamento mensal`, repassando a saída de `[FIN] 1.3 Materializar itens FIN-04`.

Campos obrigatórios na prática:
`rule.ruleId`
`run.competenceYMD`
`itemPlan.invoiceInputBase`
`createdItemIds[]`
`parcelUpdates[]` quando a regra é parcelada

## 3. High-level flow
1. `Init finalizacao` valida o envelope `fin_v3/fin04_items_ready`, herda `failures` e carrega `createdItemIds`, `parcelUpdates` e `itemPlan`.
2. `Pode iniciar finalizacao?` exige que exista ao menos um FIN-04 criado.
3. `FIN-03 Recheck fatura existente` busca FIN-03 por `id_da_regra_de_faturamento`; `Parse recheck invoice` filtra em código a competência e bloqueia duplicidade por `ruleId + competenceYMD`.
4. Se não houver FIN-03, `Parse recheck invoice` monta `invoiceInput` a partir de `itemPlan.invoiceInputBase`, força `pipe_id`/`phase_id` e acrescenta `itens_da_fatura = createdItemIds`.
5. `Pode criar invoice?` envia o payload para `FIN-03 Criar fatura`.
6. `Parse criacao invoice` valida o resultado, guarda `invoiceId` e prepara um `linkUpdate` por FIN-04 criado.
7. `Pode vincular itens FIN-04?` dispara `FIN-04 Vincular id_da_fatura` para cada item.
8. `Consolidar vinculos` valida cada update em FIN-04; em sucesso, libera a etapa de parcelas.
9. `Ha parcelas para atualizar?` dispara `FIN-02 Atualizar parcelas_pagas` para cada update parcelado.
10. `Consolidar update parcelas` valida cada update em FIN-02 e gera `statusUpdates` para os itens que devem ser inativados.
11. `Ha status para atualizar?` dispara `FIN-02 Atualizar status` quando houver `statusUpdates`.
12. `Consolidar update status` valida os updates de status.
13. `Finalizar rule_result` produz o `rule_result` final da regra.

## 4. Node-by-node analysis
- `Execute Workflow Trigger` (`executeWorkflowTrigger`) espera `fin04_items_ready`.
- `Init finalizacao` (`code`) normaliza `trace`, `run`, `rule`, `itemPlan`, `createdItemIds`, `parcelUpdates`, `logs` e `failures`. Se o envelope não for `fin_v3/fin04_items_ready`, marca `unexpected_payload_shape`. Se `createdItemIds` vier vazio, marca `no_items_ready_for_invoice`.
- `Pode iniciar finalizacao?` (`if`) só continua quando a regra não está terminal e há `createdItemIds`.
- `FIN-03 Recheck fatura existente` (`graphql`) executa `query FindInvoicesForRule($pipeId, $fieldId, $ruleId, $first)` em `pipeId: 306859731`, `fieldId: id_da_regra_de_faturamento`.
- `Merge recheck invoice` (`merge` em `combineByPosition`) recompõe request e resposta.
- `Parse recheck invoice` (`code`) trata erro de transporte como `invoice_recheck_transport_error`, erro lógico como `invoice_recheck_logical_error`, paginação como `invoice_recheck_requires_pagination` e duplicidade como `invoice_already_exists` quando encontra um card FIN-03 com o mesmo `ruleId` e `data_de_compet_ncia`. Se não houver duplicidade, ele monta `ctx.invoiceInput`, força `pipe_id = 306859731`, `phase_id = 341350484` e adiciona `{ field_id: 'itens_da_fatura', field_value: createdItemIds }`.
- `Pode criar invoice?` (`if`) só segue quando `ctx.invoiceInput` existe.
- `FIN-03 Criar fatura` (`graphql`) executa `mutation CreateFin03($input: CreateCardInput!)`.
- `Merge criacao invoice` (`merge` em `combineByPosition`) recompõe request e resposta da criação.
- `Parse criacao invoice` (`code`) trata falha de transporte/lógica como `failed_invoice_stage`. Em sucesso, grava `ctx.invoiceId`, registra `entityLog` de `create_invoice` e gera `ctx.linkUpdates = [{ itemId, invoiceId, dedupeKey, graphqlCallIndex }]`.
- `Pode vincular itens FIN-04?` (`if`) só segue quando há `linkUpdates`.
- `Set pares link updates` e `Split Out link updates` desdobram um update por FIN-04 criado.
- `FIN-04 Vincular id_da_fatura` (`graphql`) executa `updateFieldsValues` em `fieldId: id_da_fatura`.
- `Merge retorno link` (`merge` em `combineByPosition`) recompõe request e resposta.
- `Consolidar vinculos` (`code`) valida cada update. Em sucesso, registra `update_item_invoice_id`. Só quando todos os vínculos passam ele reanexa `graphqlCallIndex` a cada `parcelUpdate`.
- `Ha parcelas para atualizar?` (`if`) só segue quando há `parcelUpdates`.
- `Set pares parcelas` e `Split Out parcelas` desdobram um update por FIN-02 pai parcelado.
- `FIN-02 Atualizar parcelas_pagas` (`graphql`) executa `updateFieldsValues` em `fieldId: parcelas_pagas`.
- `Merge retorno parcelas` (`merge` em `combineByPosition`) recompõe request e resposta.
- `Consolidar update parcelas` (`code`) valida o update de parcela. Em sucesso, registra `increment_installment_counter`. Se `shouldInactivateParent === true`, gera `ctx.statusUpdates = [{ templateItemId, dedupeKey, value: ['Inativo'], graphqlCallIndex }]`.
- `Ha status para atualizar?` (`if`) só segue quando há `statusUpdates`.
- `Set pares status` e `Split Out status` desdobram um update por FIN-02 a ser inativado.
- `FIN-02 Atualizar status` (`graphql`) executa `updateFieldsValues` em `fieldId: status`.
- `Merge retorno status` (`merge` em `combineByPosition`) recompõe request e resposta.
- `Consolidar update status` (`code`) valida cada inativação. Em sucesso, registra `inactivate_parent_item`.
- `Finalizar rule_result` (`code`) resume tudo em `rule_result`. Em sucesso, usa `result.status = processed` e `result.reason = invoice_created_and_finalized`. Em falha, usa `result.status = failed` e o primeiro `reason` terminal disponível.

## 5. Pipefy interactions
- `FindInvoicesForRule`: query `findCards` no pipe FIN-03 `306859731`, buscando por `fieldId: id_da_regra_de_faturamento`.
- `Parse recheck invoice` também lê `data_de_compet_ncia` do card FIN-03 para fechar o dedupe por competência.
- `CreateFin03`: mutation `createCard` no pipe FIN-03. O código força `phase_id: 341350484`.
- `LinkInvoiceOnFin04`: mutation `updateFieldsValues` em FIN-04, campo `id_da_fatura`.
- `UpdateParcelasPagas`: mutation `updateFieldsValues` em FIN-02, campo `parcelas_pagas`.
- `UpdateFin02Status`: mutation `updateFieldsValues` em FIN-02, campo `status`, com valor `['Inativo']`.

## 6. Data contracts and transformations
Entrada:
`fin04_items_ready.createdItemIds[]`
`fin04_items_ready.parcelUpdates[]`
`fin04_items_ready.itemPlan.invoiceInputBase`
`failures[]`

Transformações principais:
`Parse recheck invoice` converte `itemPlan.invoiceInputBase` em `ctx.invoiceInput`, acrescentando `phase_id: 341350484` e `itens_da_fatura`.
`Parse criacao invoice` converte `invoiceId + createdItemIds` em `linkUpdates`.
`Consolidar vinculos` apenas libera a próxima fase; ele não altera `parcelUpdates`, só acrescenta `graphqlCallIndex`.
`Consolidar update parcelas` transforma `parcelUpdates` bem-sucedidos em `statusUpdates` quando a próxima parcela completa `quantidade_de_parcelas_totais`.

Saída final:
`rule_result.result.status`
`rule_result.result.reason`
`rule_result.result.invoiceId`
`rule_result.result.existingInvoiceId`
`rule_result.result.createdItemIds`
`rule_result.result.totals`
`rule_result.result.metricsDelta.createInvoices`
`rule_result.result.metricsDelta.updateItemInvoiceLinks`
`rule_result.result.metricsDelta.updateParcelasPagas`
`rule_result.result.metricsDelta.parentItemsInactivated`

## 7. Branches, validations, and failure paths
`unexpected_payload_shape` e `no_items_ready_for_invoice` são validados em `Init finalizacao`.

`Parse recheck invoice` pode falhar com:
`invoice_recheck_transport_error`
`invoice_recheck_logical_error`
`invoice_recheck_requires_pagination`
`invoice_already_exists`

`Parse criacao invoice` colapsa falhas de criação em `failed_invoice_stage`.

`Consolidar vinculos`, `Consolidar update parcelas` e `Consolidar update status` também colapsam falhas em `failed_invoice_stage`.

Se qualquer uma dessas etapas falha, `ctx.terminal.done = true` e o workflow segue até `Finalizar rule_result` com status final de falha.

## 8. Outputs
O workflow sempre emite um único `rule_result`.

Em sucesso, `result.status = processed` e `result.reason = invoice_created_and_finalized`.

Em falha, `result.status = failed` e `result.reason` reflete a causa terminal.

## 9. Relationship to the other workflows
Recebe `fin04_items_ready` de `[FIN] 1.3`.

Não chama outros workflows n8n; ele é o último estágio funcional da cadeia.

Entrega o `rule_result` final ao orquestrador `[FIN] 1.0`, que o agrega, contabiliza e persiste em `fin_billing_rule_log`.

## 10. Notes and implementation details
A fase `341350484` está confirmada no snapshot atual do pipe FIN-03 como `Validação interna`. É essa fase que `Parse recheck invoice` injeta antes do `createCard`.

Inferência: se a FIN-03 for criada e depois falhar o vínculo em FIN-04 ou a atualização em FIN-02, um rerun provavelmente vai parar em `invoice_already_exists` antes de repetir os updates pendentes. O código atual não implementa retomada automática pós-criação de FIN-03.

O workflow reaproveita `createdItemIds.length` como `metricsDelta.updateItemInvoiceLinks`, assumindo que a tentativa de vínculo é uma por item criado.

## 11. Short pseudo-flow
`fin04_items_ready -> recheck FIN-03 por regra+competência -> criar FIN-03 -> vincular id_da_fatura nos FIN-04 -> atualizar parcelas_pagas -> inativar FIN-02 se necessário -> emitir rule_result`

## 12. Evidence
- `Init finalizacao` exige `contractVersion: fin_v3` e `itemType: fin04_items_ready`.
- `FIN-03 Recheck fatura existente` usa `query FindInvoicesForRule` com `fieldId: id_da_regra_de_faturamento`.
- `Parse recheck invoice` filtra por `data_de_compet_ncia` e força `phase_id: 341350484`.
- `Parse recheck invoice` adiciona `{ field_id: 'itens_da_fatura', field_value: createdItemIds }` ao `invoiceInput`.
- `FIN-04 Vincular id_da_fatura` usa mutation `updateFieldsValues` no campo `id_da_fatura`.
- `FIN-02 Atualizar parcelas_pagas` usa mutation `updateFieldsValues` no campo `parcelas_pagas`.
- `FIN-02 Atualizar status` usa mutation `updateFieldsValues` no campo `status`.
- Reasons explícitos: `invoice_recheck_transport_error`, `invoice_recheck_logical_error`, `invoice_recheck_requires_pagination`, `invoice_already_exists`, `failed_invoice_stage`, `invoice_created_and_finalized`.
