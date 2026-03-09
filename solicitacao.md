Faça um plano de implementação de um workflow no n8n (com subworkflows) conforme os requisitos. No geral, vamos usar os pipes FIN-01 e FIN-02 como templates para criar cards nos pipes FIN-03 e FIN-04.

FIN-01: Regras de faturamento, templates "master".
FIN-02: Itens que compõem as regras.
FIN-03: Faturas mensais (baseadas no FIN-01).
FIN-04: Itens das faturas (baseadas no FIN-02).

# Instruções gerais do n8n:
- Evitar ao máximo a utilização de nós de código puro. Substitua por edit nodes, loops, etc.
- Para cada workflow, devemos registrar logs em uma datatable do n8n. É necessário definir as colunas dessas tabelas para que eu possa criá-las antes. É importante que elas tenham informações como: ID de execução do workflow pai (quando aplicável), qtd de chamadas graphQL feitas, status (falha/sucesso), motivo de falha, nó de falha.
- Erros em nós de graphQL devem dar erro explícito (com log sendo propagado adiante para salvamento na data table citada).
- No desenho do workflow, tente economizar queries GraphQL com operações de batch em uma query única. Pode visitar https://developers.pipefy.com/reference/cards para verificar a documentação técnica.

# Lógica principal:

- Trigger do workflow principal: CRON Job nos dias 1 a 5 de todo mês.

## Verificar se as regras existentes no pipefy são elegíveis
As regras estão no pipe FIN-01. Uma regra é elegível se:
- Sua fase for igual à Ativo (fazer a validação por id da fase, não pelo nome)
- Seu campo de dia de geração for igual a hoje
- Não existe fatura (no pipe FIN-03) baseada nessa mesma regra para o mês atual (de emissão de NF)

## Para cada regra elegível, fazer seu processamento
1. Identifique os IDs dos itens da fatura que estão presentes no pipe FIN-02. Fazer isso com base no campo "itens da fatura" presente na regra.
2. Para cada item, verificar se é elegível conforme os critérios:
  2.A. Está na fase ativo no pipe FIN-03 (checar por ID, não por nome da fase)
  2.B. Verificar o campo de condições exigidas para faturamento. As validações de cada condição serão implementadas futuramente, mas já podem ficar sinalizado no fluxo esses rampos com uma passagem verdadeira para todos (de forma temporária)
3. Para cada item elegível, criar um card correspondente no pipe FIN-04. Para isso, os campos do item "pai" devem ser replicados conforme os campos do pipe FIN-04. Precisamos mapear campos obrigatórios que existem apenas no FIN-04 e definir como serão preenchidos por padrão. De início, o campo de ID da fatura pode ficar vazio. Para os 3 tipos de modelo de cobrança (campo), precisaremos fazer cálculos distintos. Eles não serão implementados agora, mas deixe o rascunho de ramos explícitos.
4. Após a criação dos itens elegíveis, deve ser criada a fatura correspondente no pipe FIN-03 seguindo o mesmo padrão de replicar os dados do card de regra "pai". No campo de itens da fatura, já podem ser adicionados os ids dos itens criados na etapa anterior.
5. Após a criação da fatura, devemos revisitar os itens associados à ela e atualizar o campo de ID da fatura.
