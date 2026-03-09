# Plano de Implementacao FIN Faturamento Mensal

## 1. Arquivos e evidencias analisados no repositorio

- `graph/nodes.jsonl`: pipes, fases, campos, obrigatoriedade, opcoes, condicionais e workflows n8n indexados.
- `graph/edges.jsonl`: conectores entre pipes/tables, relacoes pai-filho, phase moves, escopo `start_form` vs `phase`, e visibilidade condicional.
- `index/pipe_field_to_refs.json`: confirmacao de quais campos sao efetivamente referenciados por condicionais, automacoes e nos n8n.
- `graph/manifest.json`: confirma que o grafo foi gerado de um snapshot Pipefy/raw.

## 2. Premissas confirmadas a partir dos arquivos

- Pipes confirmados:
  - FIN-01 `306662125`
  - FIN-02 `306852209`
  - FIN-03 `306859731`
  - FIN-04 `306859915`
- Fases relevantes por ID:
  - FIN-01 Ativo `341313813`
  - FIN-02 Ativo `341306826`
  - FIN-03 fase de cancelamento `341350581` (o grafo ainda mostra `Cancelada`)
  - FIN-04 fase ativa `341351580` (o grafo ainda mostra `Ativos`)
- O desenho deve usar sempre os campos conectores `itens_da_fatura` nos pipes, e nao depender das relacoes `Faturas associadas` ou `Itens associados`.
  - FIN-01 referencia FIN-02 por `itens_da_fatura`.
  - FIN-03 referencia FIN-04 por `itens_da_fatura`.
- FIN-02 tem o campo de elegibilidade `condi_es_exigidas_para_iniciar_o_faturamento_do_item`, ligado a table `KsEXACBL` "Condicoes exigidas para faturamento".
- As 3 opcoes hoje conhecidas para `condi_es_exigidas_para_iniciar_o_faturamento_do_item` sao:
  - `1258585767` = Treinamento de onboarding realizado
  - `1258585802` = Data especifica
  - `1258585836` = Finalizacao do pagamento de setup
- Os 3 modelos de cobranca existem em FIN-02 e FIN-04:
  - `Fixo`
  - `Percentual sobre base`
  - `Por unidade`
- O grafo ja indexa um orquestrador n8n com `Cron (dias 1-5 07:00)`, `Persistir logs (Data Table)`, um subworkflow de preparacao e um de processamento.
- A `competence_date` e sempre o primeiro dia do mes anterior ao da execucao.
- Regra de unicidade operacional:
  - em um mesmo mes de competencia, deve existir no maximo 1 fatura FIN-03 por regra FIN-01
  - cada item FIN-04 pode estar associado a 1 unica fatura por meio de `id_da_fatura` e da lista `itens_da_fatura`
  - se existirem cards conflitantes de FIN-03 ou FIN-04 para a mesma chave logica, o fluxo deve bloquear e nao reutilizar
- Para itens FIN-02 com `periodicidade = Parcelado`, o contador `parcelas_pagas` so deve ser incrementado depois que existir item FIN-04 vinculado a uma fatura real FIN-03; quando `parcelas_pagas == quantidade_de_parcelas_totais`, o item pai FIN-02 deve ser atualizado para `status = Inativo`.

## 3. Duvidas, inconsistencias ou gaps encontrados

- O grafo ainda expoe as relacoes `FIN-01 -> FIN-03` e `FIN-02 -> FIN-04`, mas este plano deliberadamente nao depende dos `field maps` internos dessas relacoes; a implementacao ficara ancorada em `itens_da_fatura`.
- O repositorio mostra os rotulos das 3 condicoes de inicio, mas nao mostra de onde viram os sinais externos que comprovam "Treinamento de onboarding realizado" ou "Finalizacao do pagamento de setup"; isso ainda precisa ser conectado/validado na implementacao.
- `motivo_de_cancelamento` em FIN-03 aparece como `required=true`, mas esta associado a fase de cancelamento no grafo, nao ao `start_form`. Entao a obrigatoriedade em criacao inicial continua nao sendo totalmente inferivel so pelo grafo.
- As formulas reais dos campos exclusivos de base em FIN-04 ainda nao aparecem no repositorio. O preenchimento com `0` descrito neste plano e temporario e restrito a uma whitelist explicita.
- O grafo mostra os nomes dos nodes GraphQL, mas nao o payload completo das queries/mutations. Isso impede cravar com 100% de seguranca se o ambiente Pipefy aceita filtros `in`, aliases de batch mutation ou update em lote; isso precisa de validacao no editor n8n/Pipefy.

## 4. Arquitetura proposta

Proponho um desenho em 5 workflows, ainda preservando o corte logico ja visivel no grafo atual: orquestracao mensal, preparacao das regras, preparacao dos itens da regra, materializacao dos itens e criacao final da fatura. O fluxo e `item-first`, mas com guarda fail-closed para nao reaproveitar FIN-03/FIN-04 preexistentes: primeiro validamos regra + competencia, depois criamos os FIN-04 do run atual, depois criamos a FIN-03 com `itens_da_fatura`, depois gravamos `id_da_fatura` nos FIN-04, e por fim atualizamos `parcelas_pagas`/`status` em FIN-02 quando o caso for `Parcelado`.

O desenho usa prioritariamente `Cron`, `Manual Trigger`, `Edit Fields`, `If`, `Switch`, `Loop Over Items`, `Merge`, `GraphQL` ou `HTTP Request`, `Execute Workflow`, `Data Table` e `Stop and Error`. Nao vejo necessidade obrigatoria de `Code` node na v1; so deixaria um `Code` opcional se o endpoint GraphQL exigir montagem dinamica de aliases de batch query ou mutation que o node nativo nao consiga parametrizar.

## 5. Lista de workflows e subworkflows

- `[FIN] 1 Orquestrar faturamento mensal`
- `[FIN] 1.1 Preparar regras elegiveis`
- `[FIN] 1.2 Preparar itens elegiveis da regra`
- `[FIN] 1.3 Materializar itens FIN-04`
- `[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02`

## 6. Passo a passo detalhado de cada workflow

### [FIN] 1 Orquestrar faturamento mensal

- Objetivo: abrir a execucao, definir a `competence_date` como o primeiro dia do mes anterior, chamar a preparacao, iterar regras elegiveis e agregar o resumo final.
- Trigger: `Cron` entre dias 1 e 5; manual opcional.
- Entrada: metadados de execucao; na v1 nao ha `override` de competencia.
- Saida: `run_summary` com totais de regras, itens, faturas, bloqueios por duplicidade, atualizacoes de parcelado, erros e chamadas GraphQL.
- Nos principais:
  - `Cron`
  - `Manual Trigger`
  - `Edit Fields (Init Run)`
  - `Execute Workflow`
  - `Loop Over Items`
  - `Merge` ou agregacao
  - `Data Table`
  - `If`
  - `Stop and Error`
- Onde ha loop: um loop por regra elegivel.
- Onde ha decisao: sem regras elegiveis, ou child retornou falha.
- Onde ha criacao de card: nao.
- Onde ha update de card: nao.
- Onde ha log: inicio, fim, contadores agregados, bloqueios por duplicidade e erro de child.
- Onde ha risco de duplicidade: `rerun` da mesma competencia.
- Como evitar retrabalho e queries desnecessarias: passar `competence_key`, receber lista ja deduplicada do subworkflow e exigir `recheck` no subworkflow final antes do `create` de FIN-03.

### [FIN] 1.1 Preparar regras elegiveis

- Objetivo: listar regras FIN-01 em fase ativa por `phase_id 341313813`, calcular a competencia fixa do run, pre-carregar FIN-03 dessa competencia e devolver so as regras elegiveis que ainda nao possuem invoice criada.
- Trigger: `Execute Workflow Trigger`.
- Entrada: `run_context` com `execution_date`, `execution_day`, `competence_date`, `parent_execution_id`.
- Saida: lista de `rule_candidates`.
- Nos principais:
  - `GraphQL FIN-01 page`
  - `If Ha proxima pagina?`
  - `GraphQL FIN-03 prefetch`
  - `Edit Fields`
  - `If`
  - `Remove Duplicates`
  - `Data Table`
- Onde ha loop: paginacao de FIN-01 e FIN-03.
- Onde ha decisao:
  - `phase_id`
  - `dia_de_gera_o_da_fatura == execution_day`
  - ausencia de FIN-03 existente para `rule_id + competence_date`
- Onde ha criacao de card: nao.
- Onde ha update de card: nao.
- Onde ha log: paginas lidas, regras encontradas, elegiveis, bloqueadas por `billing_day_mismatch` ou `invoice_already_exists`.
- Onde ha risco de duplicidade: snapshot de FIN-03 ficar desatualizado entre prep e create.
- Como evitar retrabalho e queries desnecessarias: `recheck` obrigatorio no ultimo subworkflow antes da criacao da fatura; se ja existir FIN-03, bloquear e encerrar a regra.

### [FIN] 1.2 Preparar itens elegiveis da regra

- Objetivo: carregar templates FIN-02 referenciados em `FIN-01.itens_da_fatura`, validar fase ativa `341306826`, avaliar as condicoes de inicio, detectar conflitos preexistentes em FIN-04 e montar um `item_plan`.
- Trigger: `Execute Workflow Trigger`.
- Entrada: `rule_candidate`.
- Saida: `item_plan` com `eligible_templates`, `templates_to_create`, `blocking_existing_fin04_items`, `blocked_templates`.
- Nos principais:
  - `GraphQL FIN-02 fetch`
  - `If phase ativo`
  - `Switch condicoes de inicio`
  - `Switch modelo_de_cobran_a`
  - `GraphQL check FIN-04 conflitos`
  - `Merge`
  - `Data Table`
- Onde ha loop: templates FIN-02 da regra.
- Onde ha decisao:
  - fase ativa
  - ramos explicitos `Data especifica`, `Treinamento de onboarding realizado`, `Finalizacao do pagamento de setup`
  - ramos explicitos `Fixo`, `Percentual sobre base`, `Por unidade`
  - ausencia de FIN-04 existente para `template_fin02_id + competence_date`
- Onde ha criacao de card: nao.
- Onde ha update de card: nao.
- Onde ha log: resultado por template (`eligible`, `blocked_missing_external_signal`, `blocked_existing_fin04_conflict`, `blocked_inactive_template`, `blocked_missing_required_input`).
- Onde ha risco de duplicidade: recriar FIN-04 apos falha parcial de execucao anterior.
- Como evitar retrabalho e queries desnecessarias: consultar FIN-04 so como guarda de conflito por chave `template_fin02_id + competence_date`; se houver item existente, bloquear a regra e nao reutilizar.

### [FIN] 1.3 Materializar itens FIN-04

- Objetivo: criar os itens FIN-04 planejados para o run atual, replicando FIN-02, aplicando `0` apenas na whitelist temporaria de campos base obrigatorios e gravando os identificadores tecnicos.
- Trigger: `Execute Workflow Trigger`.
- Entrada: `item_plan`.
- Saida: `fin04_items_ready` com IDs finais de FIN-04 e metadados dos templates parcelados que precisarao de update posterior em FIN-02.
- Nos principais:
  - `Loop Over Items`
  - `Edit Fields payload`
  - `If campos obrigatorios fora da whitelist-zero`
  - `GraphQL createCard FIN-04`
  - `If erro`
  - `Data Table`
- Onde ha loop: todos os templates elegiveis que passaram pelas guardas.
- Onde ha decisao: criar ou falhar explicitamente por campo obrigatorio ausente.
- Onde ha criacao de card: sim, FIN-04.
- Onde ha update de card: nao.
- Onde ha log: `create success/failure` por item, incluindo se houve preenchimento tecnico com `0`.
- Onde ha risco de duplicidade: `retry` apos `timeout`.
- Como evitar retrabalho e queries desnecessarias: antes de cada `create`, fazer `recheck` leve por `template_fin02_id + competence_date`; se houver conflito, parar com erro e nao reutilizar o item encontrado.

### [FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02

- Objetivo: revalidar unicidade, criar uma nova FIN-03 com `itens_da_fatura`, fazer `backfill` de `id_da_fatura` nos FIN-04 criados no run atual e atualizar `parcelas_pagas`/`status` em FIN-02 quando o template for `Parcelado`.
- Trigger: `Execute Workflow Trigger`.
- Entrada: `rule_candidate` + `fin04_items_ready`.
- Saida: `rule_result`.
- Nos principais:
  - `GraphQL recheck FIN-03`
  - `If existe?`
  - `GraphQL create FIN-03`
  - `Loop Over Items`
  - `GraphQL update FIN-04 id_da_fatura`
  - `If periodicidade = Parcelado`
  - `GraphQL update FIN-02 parcelas_pagas`
  - `If parcelas_pagas == quantidade_de_parcelas_totais`
  - `GraphQL update FIN-02 status`
  - `Data Table`
  - `Stop and Error`
- Onde ha loop: `backfill` dos FIN-04 criados no run e loop adicional so para templates parcelados.
- Onde ha decisao: FIN-03 ja existente vs nova; item parcelado vs nao parcelado; item pai FIN-02 finalizado vs ainda ativo.
- Onde ha criacao de card: sim, FIN-03.
- Onde ha update de card: sim, FIN-04 `id_da_fatura`; FIN-02 `parcelas_pagas`; FIN-02 `status` quando virar `Inativo`.
- Onde ha log: `invoice created`, `item links updated`, `installment counter incremented`, `parent item inactivated/skipped`.
- Onde ha risco de duplicidade: corrida entre dois runs.
- Como evitar retrabalho e queries desnecessarias: `recheck` just-in-time por `id_da_regra_de_faturamento + competence_date`; se ja existir FIN-03, interromper a regra com bloqueio e nao atualizar/reutilizar a fatura existente.

## 7. Estrategia para reduzir chamadas GraphQL

- Paginar FIN-01 uma vez por execucao e trazer ja os campos necessarios para decisao e criacao da fatura.
- `Prefetch` de FIN-03 por competencia uma vez no inicio, para bloquear duplicidade, e nao para reuso.
- Dentro de cada regra, buscar FIN-02 em lote por IDs do conector `itens_da_fatura`; se o schema nao aceitar lote, usar loop controlado so nesse ponto.
- Consultar FIN-04 por `template_fin02_id + competence_date` apenas como guarda de conflito; se encontrar item, falhar a regra sem reutilizar.
- `Recheck` de FIN-03 apenas uma vez, imediatamente antes de criar a invoice.
- Selecionar so campos usados pelo fluxo; nada de query generica de card completo.
- Fazer `backfill` apenas nos FIN-04 criados no run atual.
- Atualizar `parcelas_pagas` em FIN-02 apenas quando a FIN-03 ja existir de fato e o template tiver `periodicidade = Parcelado`.
- Se o ambiente Pipefy aceitar aliases, agrupar creates/updates em `chunks` pequenos; se nao aceitar, manter loop nativo e logs por item.

## 8. Estrategia de tratamento de erros

- Todo node GraphQL roda com captura explicita de erro, grava log antes de interromper e entao faz `Stop and Error`.
- Se um template FIN-02 falhar na criacao do item FIN-04, a regra inteira nao cria FIN-03; status da regra = `failed_item_stage`.
- Se existir FIN-04 preexistente para `template_fin02_id + competence_date`, a regra falha com `reason = existing_fin04_conflict` e nao reutiliza esse item.
- Se existir FIN-03 para `rule_id + competence_date`, a regra falha com `reason = invoice_already_exists` e nao reutiliza essa fatura.
- Se FIN-03 falhar depois de alguns FIN-04 ja criados, a regra fica `failed_invoice_stage`; a proxima execucao deve bloquear com `reason = orphan_fin04_exists_for_rule_competence` ate saneamento, e nao reaproveitar os itens orfaos.
- Se nenhum item ficar pronto para a regra, falhar explicitamente com `reason = no_items_ready_for_invoice`.
- Se algum campo obrigatorio estiver sem valor fora da whitelist de `0`, falhar explicitamente com `reason = missing_required_nonzero_field`.
- Child workflow deve retornar payload de falha estruturado; o orquestrador registra isso em Data Table e repropaga a falha.
- Nunca usar `return []` silencioso em falha GraphQL.

## 9. Estrategia de logs em data tables

- Um log de execucao por workflow para inicio, fim, contadores agregados, bloqueios por duplicidade e atualizacoes de parcelado.
- Um log operacional por entidade para cada regra, template FIN-02, item FIN-04, fatura FIN-03 e atualizacao de item pai FIN-02.
- Um log de chamada GraphQL por node ou operacao, incluindo sucesso, erro e contagem incremental.
- Cada workflow recebe e propaga:
  - `run_id`
  - `workflow_execution_id`
  - `parent_execution_id`
  - `workflow_name`
  - `graphql_call_counter`
- Cada erro GraphQL gera duas escritas:
  - linha em `fin_billing_graphql_log`
  - linha em `fin_billing_entity_log`
  - ambas antes do `Stop and Error`

## 10. Especificacao das data tables e colunas

### `fin_billing_run_log`

- `run_id`
- `workflow_execution_id`
- `parent_execution_id`
- `workflow_name`
- `trigger_type`
- `competence_date`
- `started_at`
- `finished_at`
- `status`
- `failure_reason`
- `graphql_calls_total`
- `rules_found`
- `rules_eligible`
- `rules_processed`
- `rules_failed`
- `items_created`
- `items_updated`
- `duplicate_items_blocked`
- `invoices_created`
- `duplicate_invoices_blocked`
- `installments_incremented`
- `parent_items_inactivated`
- `summary_json`

### `fin_billing_entity_log`

- `run_id`
- `workflow_execution_id`
- `parent_execution_id`
- `workflow_name`
- `node_name`
- `entity_type`
- `entity_id`
- `rule_id`
- `template_item_id`
- `fin02_parent_item_id`
- `invoice_id`
- `fin04_item_id`
- `competence_date`
- `action`
- `status`
- `failure_reason`
- `dedupe_key`
- `graphql_calls_delta`
- `started_at`
- `finished_at`
- `details_json`

### `fin_billing_graphql_log`

- `run_id`
- `workflow_execution_id`
- `parent_execution_id`
- `workflow_name`
- `node_name`
- `operation_name`
- `pipe_id`
- `phase_id`
- `card_id`
- `entity_type`
- `entity_id`
- `request_fingerprint`
- `graphql_call_index`
- `requested_at`
- `responded_at`
- `duration_ms`
- `status`
- `error_code`
- `error_message`
- `query_excerpt`
- `variables_json`
- `response_json`

## 11. Mapeamento de campos

### FIN-01 -> FIN-03

| pipe origem | campo origem | field ID origem | pipe destino | campo destino | field ID destino | obrigatorio no destino? | regra de preenchimento | evidencia no repositorio | observacoes |
|---|---|---|---|---|---|---|---|---|---|
| FIN-01 | Clientes na fatura | `clientes_na_fatura` | FIN-03 | Clientes na fatura | `clientes_na_fatura` | sim | copia 1:1 | `nodes.jsonl` | conector para DB-01 em ambos |
| FIN-01 | Contato | `contatos_que_receber_o_a_fatura` | FIN-03 | Contato | `contatos_que_receber_o_a_fatura` | sim | copia 1:1 | `nodes.jsonl` | conector para DB-04 em ambos |
| FIN-01 | Identificador fiscal na fatura | `identificador_fiscal_na_fatura` | FIN-03 | Identificador fiscal na fatura | `identificador_fiscal_na_fatura` | sim | copia 1:1 | `nodes.jsonl` | valor textual direto |
| FIN-01 | Tipo de cliente faturado | `tipo_de_cliente_final` | FIN-03 | Tipo de cliente faturado | `tipo_de_cliente_final` | sim | copia 1:1 | `nodes.jsonl` | governa visibilidade de cliente/rede |
| FIN-01 | Rede na fatura | `rede_na_fatura` | FIN-03 | Rede na fatura | `rede_na_fatura` | sim | copia 1:1 | `nodes.jsonl` | usar quando tipo = Rede |
| FIN-01 | Condicoes de pagamento | `forma_de_pagamento` | FIN-03 | Condicoes de pagamento | `forma_de_pagamento` | sim | copia 1:1 | `nodes.jsonl` | checklist identico |
| FIN-01 | Moeda do teto de faturamento | `moeda_do_teto_de_faturamento` | FIN-03 | Moeda do teto de faturamento | `moeda_do_teto_de_faturamento` | nao | copia 1:1 | `nodes.jsonl` | origem e obrigatoria; destino nao |
| FIN-01 | Teto de faturamento (BRL) | `teto_de_faturamento_brl` | FIN-03 | Teto de faturamento (BRL) | `teto_de_faturamento_brl` | sim | copia 1:1 | `nodes.jsonl` | condicionado por moeda |
| FIN-01 | Teto de faturamento (USD) | `teto_de_faturamento_usd` | FIN-03 | Teto de faturamento (USD) | `teto_de_faturamento_usd` | sim | copia 1:1 | `nodes.jsonl` | condicionado por moeda |
| FIN-01 | Itens da fatura | `itens_da_fatura` | FIN-03 | Itens da fatura | `itens_da_fatura` | sim | preencher com IDs FIN-04 criados no run atual | `nodes.jsonl`, `edges.jsonl` | nao copiar IDs FIN-02 brutos; nao reaproveitar FIN-04 antigo |
| FIN-01 | ID do card da regra | `card.id` | FIN-03 | ID da regra de faturamento | `id_da_regra_de_faturamento` | sim | derivar do ID da regra FIN-01 | `nodes.jsonl`, `edges.jsonl` | chave primaria de unicidade |
| contexto de execucao | Competencia do run | `N/A` | FIN-03 | Data de competencia | `data_de_compet_ncia` | sim | primeiro dia do mes anterior | `nodes.jsonl` | usar data estavel, nao "hoje" solto |
| FIN-04 agregado | Soma BRL dos itens da invoice | `N/A` | FIN-03 | Valor total da fatura (BRL) | `valor_total_da_fatura_brl` | sim | somatorio dos FIN-04 criados no run atual em BRL | `nodes.jsonl` | nao ha fonte em FIN-01 |
| FIN-04 agregado | Soma USD dos itens da invoice | `N/A` | FIN-03 | Valor total da fatura (USD) | `valor_total_da_fatura_usd` | sim | somatorio dos FIN-04 criados no run atual em USD | `nodes.jsonl` | nao ha fonte em FIN-01 |
| N/A | N/A | `N/A` | FIN-03 | Motivo de cancelamento | `motivo_de_cancelamento` | sim | omitir na criacao; preencher so em cancelamento | `nodes.jsonl`, `edges.jsonl` | campo fase-especifico; nao usar no `create` inicial |
| FIN-01 | Dia de geracao da fatura | `dia_de_gera_o_da_fatura` | N/A | N/A | `N/A` | nao | usar so na elegibilidade | `nodes.jsonl` | nao existe equivalente em FIN-03 |

### FIN-02 -> FIN-04

| pipe origem | campo origem | field ID origem | pipe destino | campo destino | field ID destino | obrigatorio no destino? | regra de preenchimento | evidencia no repositorio | observacoes |
|---|---|---|---|---|---|---|---|---|---|
| FIN-02 | Produto | `produto` | FIN-04 | Produto | `produto` | sim | copia 1:1 | `nodes.jsonl` | usar quando `tipo_de_item = Produto` |
| FIN-02 | Tipo de item | `tipo_de_item` | FIN-04 | Tipo de item | `tipo_de_item` | sim | copia 1:1 | `nodes.jsonl` | governa produto vs ajuste |
| FIN-02 | Modelo de cobranca | `modelo_de_cobran_a` | FIN-04 | Modelo de cobranca | `modelo_de_cobran_a` | sim | copia 1:1 | `nodes.jsonl` | 3 ramos explicitos |
| FIN-02 | Moeda | `moeda_da_fatura` | FIN-04 | Moeda | `moeda_da_fatura` | sim | copia 1:1 | `nodes.jsonl` | governa BRL/USD |
| FIN-02 | Base do calculo percentual | `base_do_c_lculo_percentual` | FIN-04 | Base do calculo percentual | `base_do_c_lculo_percentual` | sim | copia 1:1 | `nodes.jsonl`, `edges.jsonl` | so relevante no ramo percentual |
| FIN-02 | Percentual a ser aplicado na base | `percentual_a_ser_aplicado_na_base` | FIN-04 | Percentual a ser aplicado na base | `percentual_a_ser_aplicado_na_base` | sim | copia 1:1 | `nodes.jsonl` | ramo percentual |
| FIN-02 | Unidade de cobranca | `unidade_de_cobran_a` | FIN-04 | Unidade de cobranca | `unidade_de_cobran_a` | sim | copia 1:1 | `nodes.jsonl` | ramo por unidade |
| FIN-02 | Quantidade | `quantidade` | FIN-04 | Quantidade | `quantidade` | sim | copia 1:1 | `nodes.jsonl` | ramo por unidade |
| FIN-02 | Afiliados que compoem a cobranca | `afiliados_que_comp_em_a_cobran_a` | FIN-04 | Afiliados que compoem a cobranca | `afiliados_que_comp_em_a_cobran_a` | sim | copia 1:1 | `nodes.jsonl` | so quando unidade = Afiliados |
| FIN-02 | ID Backoffice do afiliado | `id_backoffice_do_afiliado` | FIN-04 | ID Backoffice do afiliado | `id_backoffice_do_afiliado` | sim | copia 1:1 | `nodes.jsonl` | ligado a base percentual especifica |
| FIN-02 | Ha algum desconto nesse item? | `h_algum_desconto_nesse_item` | FIN-04 | Ha algum desconto nesse item? | `h_algum_desconto_nesse_item` | sim | copia 1:1 | `nodes.jsonl` | abre ramo de desconto |
| FIN-02 | Tipo de desconto no item | `tipo_de_desconto_no_item_1` | FIN-04 | Tipo de desconto no item | `tipo_de_desconto_no_item` | sim | copiar valor; IDs diferem so no field id | `nodes.jsonl`, `index/pipe_field_to_refs.json` | `rename` obrigatorio |
| FIN-02 | Percentual de desconto aplicado | `percentual_de_desconto_a_ser_aplicado` | FIN-04 | Percentual de desconto aplicado | `percentual_de_desconto_a_ser_aplicado` | sim | copia 1:1 | `nodes.jsonl` | desconto percentual |
| FIN-02 | Valores nominais de desconto BRL/USD | `valor_nominal_a_ser_descontado_brl/usd` | FIN-04 | mesmos nomes | mesmos IDs | sim | copia 1:1 | `nodes.jsonl` | desconto nominal |
| FIN-02 | Categorias SaaS + tipos de desconto categoria + impostos/percentuais categoria | campos homonimos | FIN-04 | campos homonimos | campos homonimos | sim | copia 1:1 quando ramo SaaS/categoria estiver ativo | `nodes.jsonl` | mesmo comportamento condicional |
| FIN-02 | Valor unitario BRL/USD | `valor_unit_rio_brl/usd` | FIN-04 | mesmos nomes | mesmos IDs | sim | copia 1:1 | `nodes.jsonl` | ramo por unidade |
| FIN-02 | Subtotal do item BRL/USD | `subtotal_do_item_brl/usd` | FIN-04 | Subtotal do item BRL/USD | `valor_total_do_item_brl/usd` | sim | copiar subtotal do template | `nodes.jsonl` | rotulo igual; field ID mudou |
| FIN-02 | Valor total do item com descontos BRL/USD | `valor_total_do_item_com_descontos_brl/usd` | FIN-04 | mesmos nomes | mesmos IDs | sim | copia 1:1 | `nodes.jsonl` | usado em ajuste/desconto |
| FIN-02 | ID do template FIN-02 | `card.id` | FIN-04 | ID do item usado como template | `id_do_item_usado_como_template_fin_02` | nao | gravar sempre no `create` | `nodes.jsonl` | chave tecnica para auditoria; nao usar para reuso automatico |
| FIN-03/contexto | ID da fatura criada | `card.id` | FIN-04 | ID da fatura | `id_da_fatura` | nao | deixar vazio no `create` e preencher no `backfill` | `nodes.jsonl`, `edges.jsonl` | cada item deve apontar para uma unica fatura |
| contexto de execucao | Competencia do run | `N/A` | FIN-04 | Data de competencia | `data_de_compet_ncia` | sim | preencher com o primeiro dia do mes anterior | `nodes.jsonl` | chave de unicidade por mes |
| N/A | N/A | `N/A` | FIN-04 | Valor base BRL/USD | `valor_de_base_brl/usd` | sim | preencher `0` somente em `valor_de_base_brl` ou `valor_de_base_usd`, conforme a moeda visivel/obrigatoria | `nodes.jsonl`, `edges.jsonl` | regra temporaria; qualquer outro obrigatorio ausente deve falhar |
| N/A | N/A | `N/A` | FIN-04 | Valor base SaaS por categoria | `valor_base_saas_categoria_*` | sim | preencher `0` somente nos campos da whitelist explicita da secao 12.1 quando esse ramo estiver obrigatorio | `nodes.jsonl`, `edges.jsonl` | regra temporaria; formula futura |
| FIN-02 | Condicoes exigidas para iniciar faturamento | `condi_es_exigidas_para_iniciar_o_faturamento_do_item` | N/A | N/A | `N/A` | nao | usar so para elegibilidade | `nodes.jsonl`, `edges.jsonl` | nao copiar para FIN-04 |
| FIN-02 | Data de inicio do faturamento | `data_de_in_cio_do_faturamento` | N/A | N/A | `N/A` | nao | usar no ramo `Data especifica` | `nodes.jsonl` | nao ha equivalente em FIN-04 |
| FIN-02 | Periodicidade / parcelas totais / parcelas pagas | `periodicidade`, `quantidade_de_parcelas_totais`, `parcelas_pagas` | N/A | N/A | `N/A` | nao | usar no pos-processamento do ramo `Parcelado`; `parcelas_pagas` incrementa +1 apos invoice real | `nodes.jsonl`, `edges.jsonl` | se atingir o total, atualizar FIN-02 para `status = Inativo` |

### 11.1 Whitelist temporaria de campos obrigatorios com valor `0`

Preencher `0` somente quando o campo estiver efetivamente obrigatorio/visivel no ramo corrente:

- `valor_de_base_brl` = Valor base (BRL)
- `valor_de_base_usd` = Valor base (USD)
- `valor_base_saas_categoria_a_b_brl` = Valor base SaaS - Categoria A&B (BRL)
- `valor_base_saas_categoria_a_b_usd` = Valor base SaaS - Categoria A&B (USD)
- `valor_base_saas_categoria_e_s_brl` = Valor base SaaS - Categoria E&S (BRL)
- `valor_base_saas_categoria_e_s_usd` = Valor base SaaS - Categoria E&S (USD)
- `valor_base_saas_categoria_espa_os_brl` = Valor base SaaS - Categoria Espacos (BRL)
- `valor_base_saas_categoria_espa_os_usd` = Valor base SaaS - Categoria Espacos (USD)
- `valor_base_saas_categoria_hospedagem_brl` = Valor base SaaS - Categoria Hospedagem (BRL)
- `valor_base_saas_categoria_hospedagem_usd` = Valor base SaaS - Categoria Hospedagem (USD)
- `valor_base_saas_categoria_outros_brl` = Valor base SaaS - Categoria Outros (BRL)
- `valor_base_saas_categoria_outros_usd` = Valor base SaaS - Categoria Outros (USD)

Qualquer outro campo obrigatorio faltante deve interromper a criacao do item com `reason = missing_required_nonzero_field`.

### 11.2 Regras adicionais FIN-02 (sem copia direta para FIN-04)

- Se `periodicidade = Parcelado`, incrementar `parcelas_pagas` no FIN-02 somente depois que o item FIN-04 estiver vinculado a uma FIN-03 real.
- Se o novo valor de `parcelas_pagas` ficar igual a `quantidade_de_parcelas_totais`, atualizar o card pai FIN-02 para `status = Inativo`.
- Esses updates acontecem no workflow `[FIN] 1.4`, porque dependem de a invoice ter sido criada com sucesso.

## 12. Pontos que precisam de implementacao futura

- Substituir o preenchimento tecnico com `0` pelas formulas definitivas dos campos de base em FIN-04 (`valor_de_base_*`, `valor_base_saas_categoria_*`).
- Conectar as evidencias externas que comprovam "Treinamento de onboarding realizado" e "Finalizacao do pagamento de setup" para que os ramos de elegibilidade possam ser automatizados de ponta a ponta.
- Se desejado, converter creates/updates em lote com aliases GraphQL apos validar suporte do schema.
- Opcionalmente persistir um `lock` logico por `rule_id + competence_date` numa Data Table para concorrencia entre duas execucoes simultaneas.
- Atualizar o snapshot/grafo do repositorio para refletir a nomenclatura operacional atual dos pipes.

## 13. Riscos tecnicos e mitigacao

- Campos obrigatorios de base ainda sem formula definitiva.
  - Mitigacao: limitar o `0` tecnico a uma whitelist fechada e falhar qualquer outro obrigatorio ausente.
- Corrida entre dois `runs` na mesma competencia.
  - Mitigacao: chave `rule_id + competence_date`, `prefetch` inicial, `recheck` imediato antes do `create` e `lock` opcional em Data Table.
- Falha parcial depois de criar FIN-04 e antes de criar FIN-03.
  - Mitigacao: bloquear a proxima execucao com `orphan_fin04_exists_for_rule_competence`; nao reutilizar itens orfaos.
- Grafo do repositorio desatualizado em relacao aos nomes operacionais atuais de fase/status.
  - Mitigacao: usar `phase_id` como ancora tecnica e documentar explicitamente a nomenclatura operacional vigente.
- Fontes externas das condicoes "Treinamento de onboarding realizado" e "Finalizacao do pagamento de setup" ainda nao descritas no repositorio.
  - Mitigacao: criar adaptadores/checks explicitos antes do go-live desses ramos.
- Limitacoes desconhecidas do schema GraphQL para filtros em lote.
  - Mitigacao: desenhar `batch-first` com `fallback` explicito para loop nativo por item.

## 14. Checklist final de aderencia aos requisitos

- Sim: usa `phase_id` para elegibilidade de FIN-01 (`341313813`), FIN-02 (`341306826`) e FIN-04 (`341351580`), sem depender do nome antigo ainda presente no grafo.
- Sim: usa sempre `itens_da_fatura` como campo conector entre pipes e ignora as relacoes `Faturas associadas` e `Itens associados`.
- Sim: a arquitetura nao reutiliza FIN-03 nem FIN-04 preexistentes; em cada competencia ha no maximo 1 invoice por regra.
- Sim: a `competence_date` foi fixada como o primeiro dia do mes anterior.
- Sim: o fluxo cria a fatura com os IDs dos itens, depois atualiza os FIN-04 com `id_da_fatura`, e so depois atualiza `parcelas_pagas`/`status` em FIN-02 quando o caso for `Parcelado`.
- Sim: a whitelist de campos obrigatorios que podem receber `0` ficou explicita, e o restante deve falhar na criacao.
- Sim: as Data Tables de log foram definidas com colunas suficientes para `execution_id`, `parent_execution_id`, node, status, reason, contagem GraphQL, timestamps e IDs de regra/item/fatura/template.
- Sim: erros GraphQL falham explicitamente e ainda sao persistidos em log antes do `Stop and Error`.
- Sim: a proposta esta detalhada para implementacao pratica no n8n.
- Sim: fica explicito o que esta confirmado no repositorio, o que depende de informacao operacional atual e o que ainda precisa de validacao adicional.
