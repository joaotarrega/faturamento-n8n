# Operacao FIN faturamento mensal

## Workflows

Os exports implementados nesta repo sao:

- `[FIN] 1 Orquestrar faturamento mensal`
- `[FIN] 1.1 Preparar regras elegiveis`
- `[FIN] 1.2 Preparar itens elegiveis da regra`
- `[FIN] 1.3 Materializar itens FIN-04`
- `[FIN] 1.4 Criar fatura FIN-03, concluir FIN-04 e atualizar FIN-02`

Arquivos gerados:

- `raw/n8n/workflows/sbjuPdz4ipegrKh2.json`
- `raw/n8n/workflows/KRYkEmSTkuVD8Nsn.json`
- `raw/n8n/workflows/0nUKP2OUpRLgMSxq.json`
- `raw/n8n/workflows/dC7sV4pLm9QxT2aK.json`
- `raw/n8n/workflows/fN6rJ3wUz8YhL5cP.json`

## Configuracao obrigatoria

Antes de importar/ativar em n8n, substitua os placeholders de Data Table:

- `PLACEHOLDER_FIN_BILLING_RUN_LOG`
- `PLACEHOLDER_FIN_BILLING_ENTITY_LOG`
- `PLACEHOLDER_FIN_BILLING_GRAPHQL_LOG`

Esses placeholders aparecem apenas nos nodes `dataTable` do workflow orquestrador.

O cron do orquestrador esta configurado para dias `1-5` as `07:00`, e existe `Manual Trigger` para execucao sob demanda.

## Contrato operacional

- `competence_date` sempre aponta para o primeiro dia do mes anterior.
- Elegibilidade usa `phase_id`, nunca labels de fase.
- O unico conector entre FIN-01 / FIN-03 / FIN-04 e `itens_da_fatura`.
- Nao existe caminho de reuso de FIN-03 ou FIN-04 preexistente.
- Dedupe de fatura: `rule_id + competence_date`.
- Dedupe de item: `template_fin02_id + competence_date`.
- Se houver conflito FIN-03 ou FIN-04 para a chave logica, o fluxo bloqueia com erro estruturado.
- `parcelas_pagas` so incrementa depois que a FIN-03 existe e o FIN-04 foi vinculado via `id_da_fatura`.
- Quando `parcelas_pagas == quantidade_de_parcelas_totais`, o FIN-02 pai recebe `status = Inativo`.

## Rerun e estados bloqueados

Reruns sao fail-closed:

- Se a criacao de qualquer FIN-04 falhar, nenhuma FIN-03 e criada para a regra.
- Se a criacao da FIN-03 falhar depois de alguns FIN-04 serem criados, a regra termina como `failed_invoice_stage`.
- Runs futuros devem bloquear se encontrarem FIN-04 orfao para a mesma chave logica; esses itens nao sao reaproveitados.

Motivos de bloqueio mais importantes:

- `invoice_already_exists`
- `existing_fin04_conflict`
- `orphan_fin04_exists_for_rule_competence`
- `no_items_ready_for_invoice`
- `missing_required_nonzero_field`
- `failed_invoice_stage`
- `blocked_missing_external_signal_onboarding_training`
- `blocked_missing_external_signal_setup_payment`

## Logs

O orquestrador persiste tres trilhas separadas:

- `fin_billing_run_log`
- `fin_billing_entity_log`
- `fin_billing_graphql_log`

Cada linha propaga `run_id`, `workflow_execution_id`, `parent_execution_id`, `workflow_name`, timestamps de inicio/fim, status estruturado e chaves de dedupe.

## Regeneracao e validacao

Para regenerar os exports:

```bash
python3 tools/build_fin_monthly_workflows.py
```

Para validar os artifacts gerados:

```bash
python3 tools/validate_fin_monthly_workflows.py
```

O validador checa nomes, wiring essencial, placeholders de Data Table, ausencia de conectores proibidos, configuracao dos nodes GraphQL e sintaxe dos `jsCode`.
