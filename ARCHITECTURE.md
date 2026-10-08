# ARCHITECTURE

Este documento registra **o que foi decidido, por quê, o que custou e o que ficou de fora**. A regra de ouro do desenho: **o banco é o árbitro final das invariantes financeiras**. O broker, os locks e o código de aplicação são camadas que tornam o caminho feliz rápido. Se qualquer uma delas falhar ou tiver bug, o PostgreSQL ainda recusa o estado inválido.

- [1. Visão geral](#1-visão-geral)
- [2. Decisões (ADRs)](#2-decisões-adrs)
- [3. Schema e invariantes no banco](#3-schema-e-invariantes-no-banco)
- [4. Regras de negócio e interpretações](#4-regras-de-negócio-e-interpretações)
- [5. Contratos: status HTTP, failureCode, eventos](#5-contratos)
- [6. Fluxos críticos](#6-fluxos-críticos)
- [7. Segurança](#7-segurança)
- [8. Escalabilidade e performance](#8-escalabilidade-e-performance)
- [9. Estratégia de testes](#9-estratégia-de-testes)
- [10. Limitações conhecidas e próximos passos](#10-limitações-conhecidas-e-próximos-passos)

---

## 1. Visão geral

```mermaid
flowchart LR
  P[Provedores] -- HTTP + Idempotency-Key --> LB[nginx]
  P -- SQS FIFO\nMessageGroupId=walletId --> Q[(wager-transactions.fifo)]
  LB --> A1[instância 1]
  LB --> A2[instância 2]
  LB --> A3[instância 3]
  Q --> A1 & A2 & A3
  A1 & A2 & A3 -- "1 transação SQL:\ninbox + lock wallet + tx + saldo\n+ ledger + journal + outbox" --> DB[(PostgreSQL)]
  DB -- claim SKIP LOCKED + lease --> A1 & A2 & A3
  A1 & A2 & A3 -- eventos --> E[(wallet-events.fifo)]
  Q -- maxReceiveCount --> DLQ[(wager-transactions-dlq.fifo)]
```

Cada instância é stateless e roda os papéis definidos em `APP_ROLES`:
- `api`: HTTP;
- `consumer`: SQS;
- `outbox`: publisher;
- `pending-ref`: worker de referências.

Todos os papéis são seguros com N réplicas. As entradas HTTP e SQS convergem para **um único use case** (`ProcessWagerTransaction`), e todo efeito acontece numa **única transação SQL**.

**Camadas (hexagonal por contexto).** As dependências apontam sempre para dentro:

| Camada | Conteúdo | Depende de |
|---|---|---|
| `domain` | `Money`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry`, `WagerProcessor`, `InboxMessage`, `OutboxMessage`, eventos | nada (só `decimal.js`) |
| `application` | use cases, portas (`UnitOfWork`, repositórios, `AppMetrics`, `AppLogger`), erros de aplicação | domínio |
| `infrastructure` | MikroORM, SQS, HTTP (Nest), workers, observabilidade | aplicação e domínio |

O Nest é só a casca HTTP/DI. O núcleo é montado sem framework em `src/composition/core.ts` e registrado no Nest como valores. A mesma instância serve HTTP, SQS e testes, e os testes de integração exercitam o mesmo grafo de objetos da produção.

---

## 2. Decisões (ADRs)

### ADR-01 — MikroORM 7, `defineEntity` e escritas explícitas

**Decisão.** MikroORM, a opção preferencial do enunciado. Entidades de persistência são definidas com `defineEntity`, sem decorators, e ficam separadas dos agregados de domínio. *Mappers* explícitos convertem `record ↔ domínio` usando `rehydrate`.

**Estratégia transacional.** Cada unidade de trabalho faz `orm.em.fork()`, que tem identity map isolado, e roda dentro de `em.transactional()` com `READ COMMITTED`. As escritas financeiras são explícitas: `insert`, `nativeUpdate … WHERE version = ?` e SQL nomeado para `ON CONFLICT` e `SKIP LOCKED`. **Não** usamos `flush()` com change tracking implícito no caminho do dinheiro. O ORM fornece a fronteira transacional, o pool, o mapeamento, as migrations e os locks (`LockMode.PESSIMISTIC_WRITE`). As escritas, por sua vez, ficam legíveis e auditáveis no código.

**Mapeamento do `Money`.** Valor e moeda ficam em colunas separadas: `amount NUMERIC(20,2)` e `currency CHAR(3)`. O tipo é `decimal("string")`: o driver `pg` devolve NUMERIC como **string exata**, e a string vai direto para `Money.fromSigned()`. Nenhum valor monetário passa por `number` em nenhum ponto (veja o ADR-02).

**Trade-off.** Há mais código de mapeamento do que com entidades ricas anotadas. Em troca, o domínio não conhece ORM nem Nest e pode ser testado sem banco. O perfil de CPU mostrou que `em.fork()` e a construção do EM custam cerca de 12% do CPU consumido (veja o [LOAD_TEST](docs/LOAD_TEST.md)). É aceitável, porque o processo é I/O-bound.

### ADR-02 — Dinheiro: `decimal.js` + `NUMERIC(20,2)` de ponta a ponta

- O contrato de entrada é **estrito**: `^(0|[1-9]\d{0,17})\.\d{2}$`. Exige exatamente 2 casas e rejeita sinal, expoente, `NaN`, `Infinity`, zeros à esquerda e string vazia. Isso torna "25.00" a única grafia de vinte e cinco, o que é essencial para o hash de idempotência.
- **Não existe arredondamento.** Entradas com mais de 2 casas são rejeitadas. Somas e subtrações de valores com 2 casas são exatas (`decimal.js` com 40 dígitos de precisão). Overflow além de `NUMERIC(20,2)` é erro, nunca truncamento.
- **`NUMERIC` não viola a restrição "não usar float".** É decimal exato. O risco real está na fronteira DB↔JS, coberta por:
  - um teste de integração que prova que o valor hidrata como `string`;
  - `bun run check:money`, um guard no CI que barra `parseFloat`, `Number()` em código de domínio, campos monetários tipados como `number` e tipos `real`/`float`/`double precision`/`money` em migrations.
- O modelo é multi-moeda. O desafio usa `BRL`, mas conflitos de moeda são testados (`CurrencyMismatchError` no domínio e `CURRENCY_MISMATCH` como rejeição).
- Alternativa descartada: `BIGINT` em centavos. Também é exato, mas a escala ficaria implícita no código e o `CHECK` aritmético do ledger perderia legibilidade.

### ADR-03 — Concorrência: lock pessimista por wallet + versão como defesa em profundidade

**Decisão.** A unidade de concorrência é a `walletId`. Cada operação que pode alterar saldo faz `SELECT … FROM wallets WHERE id = ? FOR UPDATE` (`LockMode.PESSIMISTIC_WRITE`) **antes** de qualquer leitura de regra. O saldo é gravado com `UPDATE … WHERE id = ? AND version = ?`, e se nenhuma linha for afetada a transação inteira é refeita. Para não esperar indefinidamente, usamos `SET LOCAL lock_timeout = 2s` e `statement_timeout`.

**Por que pessimista.** Numa hot wallet, o lock otimista transforma contenção em tempestade de retries: N−1 de N transações perdem e refazem tudo. O lock pessimista serializa as transações na fila do próprio PostgreSQL, com custo previsível. A `version` continua lá como segunda barreira, e o `UNIQUE(wallet_id, wallet_version)` do ledger é a terceira. Assim, um lost update vira violação de schema.

**Por que não há deadlock.** Cada transação trava **exatamente uma** wallet. O worker de referências segue a mesma ordem do caminho online (wallet → linha da transação). O "nudge" de dependentes só toca linhas da **mesma** wallet.

**Isolamento.** Usamos `READ COMMITTED` mais o lock de linha. Depois de obter o lock, cada statement enxerga os commits anteriores, então um replay concorrente vê o resultado original e o devolve. A reconciliação usa `REPEATABLE READ READ ONLY`: um snapshot consistente sem tocar no lock da hot wallet.

**Custo medido.** A vazão de uma única wallet é limitada pelo tempo de lock: cerca de 125 TPS no ambiente local, ou ~8 ms por transação (veja o [LOAD_TEST](docs/LOAD_TEST.md)). Wallets diferentes escalam em paralelo.

**Retry transitório.** `55P03` (lock timeout), `40P01` (deadlock), `40001` (serialization), perda de conexão, `OptimisticConcurrencyError` e corrida de idempotência fazem *rollback* e refazem **a transação inteira**. São no máximo 3 tentativas, com jitter. Esgotadas, o erro vira `TransientInfrastructureError` (HTTP 503 com `Retry-After`, ou backoff de visibilidade no SQS).

### ADR-04 — Idempotência persistente

- **A fonte da verdade é o banco.** `UNIQUE(idempotency_key)` e `UNIQUE(provider_id, external_transaction_id)` em `wager_transactions`. Um cache em memória nunca participa da decisão.
- **Avaliação após o lock da wallet.** Duplicatas concorrentes da mesma operação ficam na fila do lock. A segunda vê a primeira já commitada e responde com replay. Uma única query cobre as duas chaves.
- **Replay devolve o resultado original**, inclusive o saldo observado naquele momento (`result_balance_amount`), com `idempotentReplay: true` e o header `Idempotent-Replayed: true`.
- **Mesma chave com payload diferente** gera `409 IDEMPOTENCY_CONFLICT` (`PAYLOAD_MISMATCH`). Mesmo `(provider, externalId)` com outra chave gera `EXTERNAL_ID_REUSED`.
- **`payloadHash` = `"v1:" + hex(sha256(canonicalJson(campos)))`**:
  - campos: `providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount,currency}, referenceExternalTransactionId`;
  - JSON canônico: chaves ordenadas recursivamente, sem espaços, propriedades `undefined` omitidas (ausente ≡ undefined), números proibidos (todo campo de negócio é string);
  - o header `Idempotency-Key`, o `messageId` e os metadados de transporte **não** entram no hash;
  - o prefixo de versão permite evoluir o algoritmo sem colisões.
- **SQS:** há um inbox persistente `PK(consumer_name, message_id)`, inserido como **primeiro** statement da transação (`ON CONFLICT DO NOTHING`):
  - uma reentrega concorrente bloqueia nesse índice até o commit da original e então é reconhecida como duplicata;
  - o mesmo `messageId` com corpo diferente é conflito e vai para a DLQ;
  - um `messageId` novo com uma chave já processada cai na idempotência de negócio (replay).
- **Retenção do inbox:** 30 dias, configurável. O inbox é só uma janela de dedup de transporte. A idempotência de negócio é permanente, via constraints, então expurgar o inbox nunca reabilita efeito duplicado.

### ADR-05 — Referências fora de ordem

- Uma transação que **declara** referência (obrigatória para REFUND e ROLLBACK, opcional para WIN e LOSS → BET) só é aplicada quando a referência existe e está `PROCESSED`. Se a referência não existe, ou ela própria está `PENDING_REFERENCE`, a transação fica `PENDING_REFERENCE` (HTTP 202) e emite `WagerTransactionPendingReference`.
- **Worker agendado** (`pending-ref`):
  - faz o claim com `FOR UPDATE SKIP LOCKED` e empurra `next_reference_retry_at` como lease, assim uma instância que morre libera o trabalho sozinha;
  - processa cada item na sua própria transação, travando wallet → transação;
  - usa backoff exponencial `2s·2ⁿ` com jitter e teto de 5 min.
- **Orçamento: 10 tentativas (~35 min).** Provedores enviam a operação referenciada em segundos ou minutos. Um orçamento maior prenderia dinheiro "no limbo" e atrasaria a resposta definitiva ao provedor. Esgotado o orçamento, a transação vira `REJECTED(REFERENCE_NOT_FOUND)` e o evento correspondente é publicado.
- **Nudge:** quando a referência chega e é processada, a mesma transação SQL marca os dependentes da mesma wallet como devidos *agora*. A latência de resolução cai de "próximo backoff" para "próximo poll" (~1 s). O worker continua sendo a garantia.

### ADR-06 — Transactional outbox

- O evento é gravado na **mesma transação SQL** do efeito financeiro. Nada é publicado a partir de linhas não commitadas.
- **Claim com lease:**
  - `UPDATE … SET locked_until, locked_by FROM (SELECT … FOR UPDATE SKIP LOCKED ORDER BY seq LIMIT n)`, numa transação curta;
  - a publicação acontece fora de transação;
  - depois vem `markPublished`, ou `scheduleRetry` com backoff.
  - Se o processo morre entre o claim e o mark, a lease expira e **outra instância republica**. Isso foi testado com `SIGKILL` real.
- **At-least-once.** `eventId` = id da outbox = `MessageDeduplicationId` do SQS. A dedup do broker (janela de 5 min) é otimização; o consumidor deve deduplicar por `eventId`.
- **Ordem:** `aggregateId = walletId` = `MessageGroupId`. O publisher divide o lote em *lanes* por hash do `aggregateId`: cada wallet publica sequencialmente em ordem de `seq`, e lanes diferentes publicam em paralelo. Uma falha **interrompe a lane**, para não publicar eventos de uma wallet "por cima" de um buraco. Entre publishers diferentes a ordem é *best effort*. Os eventos carregam `walletVersion` para o consumidor ordenar ou ignorar eventos obsoletos.
- No shutdown, o publisher devolve as leases imediatamente (`releaseLeases`).

### ADR-07 — Consumidor SQS

- **Mesmo use case** do HTTP. O ack (`DeleteMessage`) acontece **somente após o commit**.
- **Classificação:**
  - **negócio** (PROCESSED, REJECTED, PENDING_REFERENCE): commit e ack;
  - **transitório**: `ChangeMessageVisibility` com backoff baseado em `ApproximateReceiveCount`; depois de `maxReceiveCount = 5`, a redrive policy leva a mensagem à DLQ;
  - **permanente** (JSON inválido, schema inválido, conflito de idempotência, wallet inexistente): envio explícito à DLQ com atributo `failureReason`, seguido de delete.
- Mensagens do mesmo `MessageGroupId` (wallet) dentro de um lote são processadas **em sequência**; grupos diferentes, em paralelo. O FIFO do broker é otimização de ordem, não garantia de consistência.
- Há um heartbeat de visibilidade para processamentos longos.
- **`SIGTERM`:**
  1. a readiness passa a 503;
  2. o polling para (o long poll é abortado);
  3. as mensagens em andamento terminam dentro do grace period;
  4. as recebidas e não iniciadas voltam com `visibility = 0`;
  5. os workers param e liberam as leases;
  6. o HTTP fecha;
  7. o pool fecha.

### ADR-08 — Ledger de partidas dobradas (diferencial)

- `wallet_ledger_entries` continua sendo o **sub-ledger** da wallet, com `balance_before/after` e `wallet_version`.
- O **journal** `ledger_postings` registra as duas pernas de cada movimento:
  - `wallet:{id}`;
  - a contrapartida: `provider:{providerId}:{ccy}` (clearing) ou `system:funding:{ccy}` (OPENING).
- Um *constraint trigger* diferido exige Σdébitos = Σcréditos por transação no COMMIT. Outro exige que toda linha do sub-ledger tenha a sua perna no journal.
- **As contas de contrapartida não têm saldo materializado.** Uma linha "house" atualizada a cada aposta seria um lock **global** compartilhado por todas as wallets, que é exatamente o que o enunciado proíbe. Os saldos dessas contas são derivados sob demanda (`SUM`).
- A migration é separada, reversível e faz **backfill** do histórico.

### ADR-09 — Autenticação: ponto de extensão, não implementação

Autenticação não pontua, então o tempo foi para correção. Existe `ProviderIdentityPort`, implementado por `NoopProviderIdentity`, e o `ProviderAuthGuard` é **global e real**:
- resolve o principal;
- marca health e metrics como `@Public()`;
- aplica autorização, garantindo que um provider só atua sobre o próprio `providerId`. O adapter no-op devolve um principal `*` confiável.

**Desenho de produção (Keycloak ou Zitadel):**
- cada provedor é um *client* OIDC com `client_credentials`;
- um `JwtProviderIdentity` valida o bearer via JWKS do IdP (com cache e rotação), checando `iss`, `aud` e `exp`;
- ele mapeia a claim `provider_id` para o principal;
- o guard já impõe `principal.providerId === body.providerId`;
- mensagens SQS são canal interno confiável (escopo do enunciado), mas a identidade do provedor na mensagem continua sujeita às mesmas validações de domínio.

### ADR-10 — Observabilidade

- **Logs** pino em JSON. Um `AsyncLocalStorage` injeta `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId`, `channel` e `traceId` em toda linha. Valores monetários e payloads **não são logados**: o código não os passa, e a configuração do pino faz `redact` como defesa em profundidade.
- **Métricas** prom-client em `/metrics`:
  - `wager_transactions_total{kind,status,channel}`;
  - `wager_idempotent_replays_total`, `wager_idempotency_conflicts_total`, `inbox_duplicates_total`;
  - `db_transient_retries_total`, `db_lock_conflicts_total`;
  - `sqs_messages_total{result=ack|retry|dlq|duplicate}`, `sqs_dlq_depth`;
  - `outbox_lag_seconds` (idade do evento mais antigo não publicado), `outbox_publish_lag_seconds` (commit → broker), `outbox_pending_messages`;
  - `wager_processing_duration_seconds`, `pending_reference_transactions`, `reconciliation_mismatches_total`.
- **Health:** `live` é só o processo, porque uma queda do banco não deve reiniciar pods. `ready` checa PostgreSQL e SQS com timeout e cache de 2 s, e responde 503 durante o shutdown.
- **Tracing:** OpenTelemetry via `--preload`. Inclui auto-instrumentação de HTTP e `pg` (validada no Bun) e spans manuais de consumo SQS e outbox. O W3C trace context é propagado em atributos SQS. Fica desligado por padrão.
- **Dashboard** Grafana provisionado com 14 painéis: lag, DLQ, latência p50/p95/p99, idempotência, locks, outbox e event loop.

### ADR-11 — MiniStack em vez de LocalStack

Desde março de 2026 a imagem da LocalStack exige *auth token*, inclusive no antigo plano Community. O MiniStack (MIT) foi validado num spike para tudo o que o desafio precisa: FIFO, deduplicação, `ApproximateReceiveCount`, `ChangeMessageVisibility` e redrive para DLQ.

Há uma ressalva: em envios deduplicados, o MiniStack devolve o MD5 da mensagem **original**, o que dispara `InvalidChecksumError` no SDK. A verificação de MD5 é desligada **só** quando `SQS_VERIFY_MD5=false` (emuladores). O endpoint é configurável, e trocar por LocalStack ou AWS real é questão de variável de ambiente.

---

## 3. Schema e invariantes no banco

Migrations escritas à mão, versionadas e reversíveis (`up`/`down`, testadas), executadas pelo role `wagering_migrator`. A aplicação roda como `wagering_app`, que **não tem** `UPDATE/DELETE/TRUNCATE` no ledger nem no journal, nem DDL.

| Garantia | Mecanismo |
|---|---|
| Uma wallet por `(player, currency)` | `UNIQUE wallets_player_currency_uk` |
| Saldo nunca negativo | `CHECK (balance_amount >= 0)` em wallets; `CHECK` em `balance_before/after` no ledger |
| Valores positivos (LOSS pode ser 0) | `CHECK (amount > 0 OR (kind='LOSS' AND amount=0))`; ledger `amount > 0` |
| `version` muda **+1** se e só se o saldo muda | trigger `wallets_update_guard` |
| Colunas de identidade imutáveis; wallets nunca deletadas | trigger `wallets_update_guard` |
| Aritmética de cada lançamento | `CHECK ledger_arithmetic_ck` (`after = before ± amount`) |
| No máximo um lançamento por transação e wallet | `UNIQUE (transaction_id, wallet_id)` |
| Lost update impossível | `UNIQUE (wallet_id, wallet_version)` |
| Cadeia do ledger contínua | trigger `ledger_entry_chain_guard` (o `before` de v*n* = o `after` de v*n−1*) |
| Toda alteração de saldo tem lançamento e vice-versa | *constraint triggers* **diferidos** `wallets_ledger_consistency` / `wallet_ledger_entries_wallet_consistency` |
| Só transações `PROCESSED` que movem saldo têm ledger, com mesmo valor e moeda | constraint trigger diferido `ledger_entry_transaction_guard` |
| Ledger e journal append-only | triggers `BEFORE UPDATE/DELETE/TRUNCATE` + `REVOKE` |
| Idempotência | `UNIQUE (idempotency_key)`, `UNIQUE (provider_id, external_transaction_id)` |
| Referência na mesma wallet | FK composta `(reference_transaction_id, wallet_id) → (id, wallet_id)` |
| Reversão única | índice único **parcial** `(reference_transaction_id) WHERE kind IN ('REFUND','ROLLBACK') AND status='PROCESSED'` |
| Transações terminais imutáveis; colunas de negócio sempre imutáveis; nada é deletado | trigger `wager_transactions_guard` |
| Coerência de estado | `CHECK`: REFUND/ROLLBACK exigem referência; REJECTED/FAILED ⇔ `failure_code`; terminal ⇔ `processed_at`; PENDING_REFERENCE ⇔ `next_reference_retry_at` |
| Journal balanceado | constraint trigger diferido `ledger_postings_balanced` |
| Inbox | `PK (consumer_name, message_id)` |

Todas essas garantias têm teste que tenta violá-las com SQL direto ([schema-constraints.test.ts](test/integration/schema-constraints.test.ts)).

**Índices** pensados para o caminho quente e para os workers:
- únicos acima;
- parciais `WHERE status='PENDING_REFERENCE'` (due e lookup);
- parcial `WHERE published_at IS NULL` na outbox;
- `(wallet_id, created_at)`;
- `(account, created_at)` no journal.

**Paginação do ledger:** keyset em `wallet_version DESC`. O cursor é `base64url({"v":n})`, opaco e estável, porque a versão é imutável e monotônica por wallet.

---

## 4. Regras de negócio e interpretações

| Operação | Saldo | Ledger | Regras |
|---|---|---|---|
| `BET` | débito | 1 `DEBIT` | sem saldo → `REJECTED(INSUFFICIENT_FUNDS)` |
| `WIN` | crédito | 1 `CREDIT` | referência opcional; se declarada, precisa ser uma `BET` da mesma rodada/wallet/player/moeda |
| `LOSS` | — | — | `PROCESSED` sem mover saldo; referência opcional à `BET` |
| `REFUND` | crédito | 1 `CREDIT` | só referencia `BET` `PROCESSED`; valor igual; uma vez |
| `ROLLBACK` | inverso da referência | 1 invertido | referencia `BET`/`WIN`/`REFUND` `PROCESSED`; valor igual; uma vez; saldo negativo → `REJECTED(REVERSAL_INSUFFICIENT_FUNDS)` |
| `OPENING` | crédito | 1 `CREDIT` | interno; rejeitado pela API e pela fila |

**Máquina de estados** (`WagerTransaction`):

```
PENDING ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
PENDING_REFERENCE ──► PROCESSED | REJECTED | FAILED      (scheduleReferenceRetry não muda o estado)
PROCESSED, REJECTED, FAILED: terminais — transicionar é erro de programação (InvalidTransactionStateError)
```

`PENDING` é transitório em memória: a transação nasce e é decidida na mesma transação SQL, então nunca é persistida como PENDING. `FAILED` é reservado para falha permanente de infraestrutura auditável. No fluxo atual, falhas permanentes de mensagens vão para a DLQ sem criar linha (veja Limitações).

**Interpretações adotadas** (o enunciado pede que sejam documentadas):
1. **Reversão única no total.** A regra 4 diz "pelo mesmo tipo", mas permitir `REFUND` **e** `ROLLBACK` da mesma `BET` creditaria duas vezes. Adotamos a regra mais estrita: uma referência é revertida no máximo uma vez, por REFUND ou ROLLBACK, o que for `PROCESSED` primeiro. Para desfazer um REFUND, usa-se `ROLLBACK` *do REFUND*. ROLLBACK de ROLLBACK não é permitido.
2. **Referência declarada é dependência.** Um WIN ou LOSS com `referenceExternalTransactionId` espera a BET chegar, como um REFUND ou ROLLBACK.
3. **A rejeição de wallet inexistente não é persistida** (`422 WALLET_NOT_FOUND` / DLQ). Não há wallet para travar nem FK válida. Um reenvio após a criação da wallet é processado normalmente.
4. **A rejeição carrega o saldo observado** (inalterado) e é idempotente: o replay devolve o mesmo 422.
5. **WIN sem referência** é aceito (crédito avulso do provedor).
6. **`amount` com exatamente 2 casas** no contrato. "25" e "25.5" são rejeitados (400) e nunca normalizados.
7. **`providerId = "system"` é reservado** para OPENING.
8. **Timestamps são informativos.** Ordem é `version`. Sob carga foi observado *clock skew* de mais de 125 ms entre instâncias. O `updatedAt` da wallet é mantido monotônico no domínio, e a constraint `updated_at >= created_at` foi quem revelou o problema.

---

## 5. Contratos

### Mapeamento HTTP

O mesmo código para a primeira execução e para o replay. `idempotentReplay` e `Idempotent-Replayed` distinguem os dois casos.

| Situação | Status | Corpo | Provedor deve… |
|---|---|---|---|
| Processada | **201** | `{transactionId, status:"PROCESSED", balance, idempotentReplay}` | seguir |
| Aceita, aguardando referência | **202** | `status:"PENDING_REFERENCE"` | consultar depois ou ouvir eventos |
| Rejeição de negócio (persistida) | **422** | `status:"REJECTED", failureCode` | não reenviar (ver ação na taxonomia) |
| Payload inválido / header ausente / `OPENING` | **400** | `code:"VALIDATION_ERROR", details.issues[]` | corrigir o payload |
| Conflito de idempotência / wallet duplicada | **409** | `code:"IDEMPOTENCY_CONFLICT" \| "WALLET_ALREADY_EXISTS"` | não reenviar |
| Wallet inexistente | **422** | `code:"WALLET_NOT_FOUND"` | corrigir ou criar a wallet |
| Recurso inexistente (GET) | **404** | `code:"…_NOT_FOUND"` | — |
| Falha transitória (lock timeout, DB/SQS) | **503** + `Retry-After` | `code:"TRANSIENT_FAILURE"` | **reenviar com a mesma chave** |
| Inesperado | **500** | `code:"INTERNAL_ERROR"` (sem detalhes internos) | reenviar com a mesma chave |

### Taxonomia de `failureCode`

| Código | Ação | Significado |
|---|---|---|
| `INSUFFICIENT_FUNDS` | final | BET acima do saldo |
| `REVERSAL_INSUFFICIENT_FUNDS` | final, exige humano | ROLLBACK de crédito deixaria o saldo negativo |
| `CURRENCY_MISMATCH` | corrigir | moeda da operação ≠ moeda da wallet |
| `WALLET_PLAYER_MISMATCH` | corrigir | wallet não pertence ao player |
| `REFERENCE_NOT_FOUND` | final | referência não chegou dentro do orçamento de retries |
| `REFERENCE_MISMATCH` | corrigir | referência de outro provider/player/wallet/rodada/moeda |
| `REFERENCE_AMOUNT_MISMATCH` | corrigir | valor ≠ referência (reversão parcial fora de escopo) |
| `REFERENCE_KIND_NOT_ALLOWED` | corrigir | REFUND→BET; ROLLBACK→BET\|WIN\|REFUND; WIN/LOSS→BET |
| `REFERENCE_NOT_PROCESSED` | final | referência REJECTED/FAILED |
| `REFERENCE_ALREADY_REVERSED` | final | já existe REFUND/ROLLBACK `PROCESSED` para a referência |
| `INFRA_PERMANENT_FAILURE` | final | reservado para `FAILED` auditável |

### Eventos de integração

Todos usam a classe abstrata `IntegrationEvent<T>`, com `eventType` e `version` fixos **no tipo**, `data` em JSON estável (`MoneyProps` como string) e `aggregateId = walletId`.

| Evento | Quando |
|---|---|
| `WagerTransactionProcessed` v1 | qualquer transação aplicada, inclusive LOSS e OPENING |
| `WagerTransactionRejected` v1 | rejeição de negócio |
| `WalletBalanceChanged` v1 | **somente** quando o saldo muda (traz `walletVersion`, `balanceBefore`/`After`) |
| `WagerTransactionPendingReference` v1 | primeira vez que a transação fica pendente |

---

## 6. Fluxos críticos

**Submissão (HTTP ou SQS), uma transação SQL:**

```mermaid
sequenceDiagram
  participant C as HTTP / SQS consumer
  participant U as ProcessWagerTransaction
  participant DB as PostgreSQL
  C->>U: comando (+ inbox se SQS)
  U->>DB: BEGIN; SET LOCAL lock_timeout/statement_timeout
  opt SQS
    U->>DB: INSERT inbox ON CONFLICT DO NOTHING
    Note over U,DB: conflito → duplicata: devolve resultado original (ack)
  end
  U->>DB: SELECT wallet FOR UPDATE
  U->>DB: SELECT por idempotency_key OU (provider, externalId)
  Note over U: existe e hash igual → replay · hash diferente → 409
  U->>DB: (se referência) SELECT referência + reversed?
  Note over U: WagerProcessor aplica as regras em memória
  U->>DB: INSERT tx · UPDATE wallet WHERE version=? · INSERT ledger + journal · INSERT outbox · nudge dependentes
  U->>DB: COMMIT (constraint triggers diferidos validam saldo↔ledger↔journal)
  U-->>C: resultado → HTTP status / ack SQS
```

**Crash entre o commit e o ack.** O efeito é durável. A mensagem reaparece após o visibility timeout, o `INSERT` do inbox conflita, o consumidor devolve o resultado original e dá ack. Não há segundo débito. Isso é testado com `SIGKILL` real.

**ROLLBACK antes do BET:**
1. O ROLLBACK é persistido como `PENDING_REFERENCE` (202) e emite um evento.
2. O BET chega e é processado. Na mesma transação, o "nudge" torna o ROLLBACK devido.
3. O worker trava a wallet e depois a transação, resolve a referência e aplica a reversão.
4. Os eventos `Processed` e `BalanceChanged` são emitidos.

---

## 7. Segurança

- **Privilégio mínimo no banco:** o role de runtime só tem DML. Ledger e journal só aceitam `SELECT, INSERT`. As migrations rodam com outro role.
- **Validação estrita na borda:** zod com `strictObject`, que rejeita chaves desconhecidas; ids com charset restrito e tamanho máximo; UUIDs validados; limite de body de 32 kB; `helmet`; `x-powered-by` desligado.
- **SQL parametrizado** em todo lugar. `statement_timeout` e `lock_timeout` por transação.
- **Sem vazamento:** erros 500 não expõem detalhes. Logs não contêm valores, payloads nem credenciais (com `redact` como defesa em profundidade). O tracing de PostgreSQL não grava parâmetros.
- **Container** non-root (`USER bun`), imagem slim e `.dockerignore` sem `.env`/`.git`/`test`.
- **Configuração** validada no boot (fail fast). Segredos só por ambiente. `FAULT_INJECTION` é ignorado em `NODE_ENV=production`.
- **Rate limiting** é responsabilidade do gateway. Um throttler em memória por instância não seria garantia com N réplicas.

---

## 8. Escalabilidade e performance

- **Horizontal:** as instâncias são stateless. Todas as coordenações (lock por wallet, idempotência, inbox, claim da outbox e dos pendentes) estão no PostgreSQL. Cada papel pode escalar separadamente via `APP_ROLES`.
- **Contenção restrita à wallet.** Wallets distintas não competem. O teto de uma hot wallet é `1 / tempo de lock` (~8 ms → ~125 TPS no ambiente local). Mitigações fora do escopo: reduzir os round-trips dentro do lock com uma stored procedure, ou particionar o saldo de wallets "da casa".
- **Conexões:** `DB_POOL_MAX × instâncias < max_connections`. Por usar só `SET LOCAL`, a aplicação é compatível com PgBouncer em *transaction pooling*.
- **SQS FIFO:** o paralelismo é por `MessageGroupId` (wallet). Em AWS real, o *high throughput FIFO* remove o teto por fila.
- **Crescimento:** particionar o ledger, o journal e a outbox por tempo; arquivar a outbox publicada e o inbox expurgado; usar LISTEN/NOTIFY para reduzir a latência da outbox sem polling.
- **Medições e análise:** [docs/LOAD_TEST.md](docs/LOAD_TEST.md). O resumo: a API sustenta ~340 req/s com 0 erros no desktop local, e o gargalo do caminho assíncrono foi o **emulador** SQS (CPU a 99%), não o desenho.

---

## 9. Estratégia de testes

São 175 testes. **Nenhum mock de PostgreSQL ou SQS**: integração e concorrência rodam contra PostgreSQL 17 e MiniStack reais (`docker-compose.test.yml`). Cada suíte usa um **banco isolado**, clonado de um template migrado (`CREATE DATABASE … TEMPLATE`), e **filas com sufixo único**. O invariante final roda ao fim de todas as suítes: `assertAllWalletsConsistent` verifica saldo = Σledger, a cadeia de versões contínua e a versão final igual à da wallet.

| Exigência do enunciado | Onde |
|---|---|
| Money, Wallet, regras BET/WIN/LOSS/REFUND/ROLLBACK, moeda, payload divergente | `test/unit/*` |
| Migrations (up/down/up) e constraints | `test/integration/schema-constraints.test.ts` |
| Atomicidade wallet/ledger/inbox/outbox | `wagering-flow`, `outbox-and-references` ("failed transaction leaves no outbox row"), `schema-constraints` |
| Inbox e redelivery | `sqs-consumer.test.ts` |
| Publishers concorrentes | `outbox-and-references.test.ts` (in-process) e `multi-instance.test.ts` #6 (processos, com crash) |
| Retry e DLQ | `sqs-consumer.test.ts` (lock real → retry → sucesso; lock persistente → redrive para DLQ; poison → DLQ) |
| Recuperação após reinício | `multi-instance.test.ts` #8, `graceful-shutdown.test.ts` |
| 1. mesma aposta 50× em paralelo | `same-process-races.test.ts` (50×) e `multi-instance.test.ts` #4 (51× em 3 processos) |
| 2. disputa de saldo (100 / 2×80) | `same-process-races.test.ts` (10 rodadas) e `multi-instance.test.ts` #4 (entre processos) |
| 3. wallets distintas em paralelo | `same-process-races.test.ts` |
| 4. ≥ 3 processos | `multi-instance.test.ts` #4 (3 × `bun src/main.ts`) |
| 5. worker morto após commit e antes do ack | `multi-instance.test.ts` #5 (`FAULT_INJECTION=after-commit-before-ack` → `SIGKILL`) |
| 6. dois publishers na mesma outbox | `multi-instance.test.ts` #6 (um morre após publicar e antes de marcar) |
| 7. ROLLBACK/REFUND antes da referência | `outbox-and-references.test.ts` (+ orçamento esgotado e 3 workers concorrentes) |
| 8. reinício com prova de consistência | `multi-instance.test.ts` #8 (SIGKILL em todas as instâncias no meio do tráfego e restart) |

Ferramentas de suporte:
- `FaultInjector`: pontos de crash só para teste, ignorados em produção;
- `spawnInstance`: processos reais;
- `waitFor`: polling determinístico com timeout, em vez de sleeps fixos.

---

## 10. Limitações conhecidas e próximos passos

- **Status `FAILED`.** Falhas permanentes de mensagens vão para a DLQ com o motivo, mas não geram linha `FAILED` em `wager_transactions`, porque a transação SQL foi revertida. Próximo passo: persistir o `FAILED` auditável em transação separada antes do dead-letter, quando a identidade da transação for válida.
- **Contexto de trace na outbox.** O trace da requisição original não é gravado na outbox, então o span de publicação começa um trace novo. Próximo passo: adicionar uma coluna `traceparent`.
- **Ordem entre publishers** é *best effort*. Os consumidores devem usar `walletVersion` e deduplicar por `eventId`.
- **Testes de multi-processo e timing.** Uma execução isolada falhou no cenário #5 por mensagem residual de outro cenário. A causa foi corrigida com filas dedicadas, e depois disso foram 10 execuções seguidas sem falha. Como os testes dependem de tempo, um ambiente muito saturado pode aumentar a latência; os timeouts são generosos.
- **Hot wallet.** O limite é o tempo de lock. Reduzir os round-trips dentro do lock (stored procedure ou CTE única) é a principal otimização pendente.
- **Emulador.** Os números assíncronos no ambiente local refletem o MiniStack (Python, um core), não o SQS real.
- **Autenticação** não implementada (veja o ADR-09). Rate limiting ficaria no gateway.
