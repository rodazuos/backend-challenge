# Distributed Wagering Processor

Serviço financeiro distribuído que processa transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) de vários provedores. Ele continua **correto** com mensagens duplicadas, fora de ordem ou concorrentes, e com várias instâncias rodando ao mesmo tempo.

> Enunciado original: [docs/CHALLENGE.md](docs/CHALLENGE.md) · Decisões e trade-offs: [ARCHITECTURE.md](ARCHITECTURE.md) · Teste de carga: [docs/LOAD_TEST.md](docs/LOAD_TEST.md)

**Stack:** Bun 1.4 · TypeScript strict · NestJS 12 · MikroORM 7 · PostgreSQL 17 · AWS SQS (MiniStack) · Docker Compose · OpenTelemetry · Prometheus/Grafana/Jaeger

## Destaques

- **Invariantes no schema.** Estas regras são garantidas pelo PostgreSQL, não só pelo código:
  - saldo nunca negativo;
  - ledger append-only;
  - uma linha de ledger por versão da wallet;
  - "toda alteração de saldo tem lançamento", via constraint trigger diferido;
  - reversão única;
  - transações terminais imutáveis;
  - journal de partidas dobradas balanceado.
- **Concorrência por wallet.** Usa `SELECT … FOR UPDATE` por wallet, com `version` como defesa em profundidade. Não há lock global e não há deadlock entre wallets.
- **Idempotência persistente.** A chave de idempotência e o par (provider, externalId) são únicos no banco. Há inbox por `(consumer, messageId)` para SQS, e o replay devolve o resultado original.
- **Outbox transacional.** Publishers concorrentes coordenam o trabalho por lease (`SKIP LOCKED`). Um publisher que cai é substituído automaticamente quando a lease expira.
- **Auditoria de recusas.** Toda submissão que não vira resultado de negócio (payload inválido, conflito, wallet inexistente, retries esgotados) fica registrada, emite um evento e é consultável pelo provedor, sem ocupar a chave de idempotência. Erros inesperados que esgotam as tentativas viram `FAILED` auditável.
- **Lote por wallet no SQS.** As mensagens de uma mesma wallet chegadas juntas são aplicadas numa única transação SQL, com resultado idêntico ao processamento um a um (há teste de equivalência). Com tráfego concentrado, são 3,5× menos transações e +48% de vazão.
- **Testes com infraestrutura real.** São 204 testes, com PostgreSQL e SQS reais. Incluem 3 processos Bun simultâneos, `SIGKILL` entre o commit e o ack, publishers que caem no meio do envio e restart com reconciliação final.

## Início rápido (stack completa)

Pré-requisitos: Docker (com Compose v2) e [Bun 1.4](https://bun.sh).

```bash
docker compose up -d --build
```

Esse comando sobe:
- PostgreSQL e MiniStack (SQS);
- um job de bootstrap, que roda as migrations e cria as filas, a DLQ e a redrive policy;
- **3 réplicas** da aplicação atrás de um nginx em `http://localhost:8080`.

```bash
bun install
bun run scripts/demo.ts
```

O demo cria uma wallet e envia apostas por HTTP e SQS. Isso inclui replay, conflito de idempotência, saldo insuficiente e um `ROLLBACK` que chega antes do seu `BET` com entrega duplicada. No fim, ele mostra o ledger, a reconciliação e os eventos publicados.

Com observabilidade (Prometheus em `:9090`, Grafana em `:3001` com dashboard provisionado, Jaeger em `:16686`):

```bash
OTEL_ENABLED=true docker compose --profile observability up -d --build
```

## Desenvolvimento local (app no host)

```bash
bun install
bun run test:infra:up            # PostgreSQL :55432 + MiniStack :54566 (dados em tmpfs)
cp .env.example .env
bun run migrate:up
bun run scripts/setup-queues.ts
bun run dev                      # http://localhost:3000
```

## Comandos

| Comando | O que faz |
|---|---|
| `bun run start` / `bun run dev` | sobe uma instância (papéis via `APP_ROLES`) |
| `bun run migrate:up` · `migrate:down [--step N]` · `migrate:status` | migrations versionadas e reversíveis (role `wagering_migrator`) |
| `bun run test:unit` | domínio puro (Money, Wallet, regras, máquina de estados, hash) |
| `bun run test:integration` | PostgreSQL + SQS reais: constraints, API, consumer, outbox, shutdown |
| `bun run test:concurrency` | corridas reais e cenários com múltiplos processos e crash |
| `bun test` | tudo (exige `bun run test:infra:up`) |
| `bun run test:load` | teste de carga contra a stack Docker ([relatório](docs/LOAD_TEST.md)) |
| `bun run load/sqs-drain.ts prepare` · `measure` | vazão pura do consumidor drenando uma fila pré-carregada ([relatório](docs/LOAD_TEST.md)) |
| `bun run lint` · `typecheck` · `check:money` | Biome, `tsc` strict e guarda estática contra `number` para dinheiro |

## API

Todas as respostas de erro usam `{ code, message, details?, correlationId }`. A tabela completa de status está no [ARCHITECTURE.md](ARCHITECTURE.md#mapeamento-http).

| Método e rota | Descrição |
|---|---|
| `POST /wallets` | cria a wallet; saldo inicial > 0 gera transação `OPENING` + `CREDIT` na mesma transação SQL |
| `GET /wallets/:walletId` | saldo e versão |
| `GET /wallets/:walletId/ledger?cursor=&limit=50` | ledger paginado (cursor opaco e estável, ordem por versão desc) |
| `POST /wallets/:walletId/reconciliation` | recalcula o saldo pelo ledger e pelo journal; divergência é logada, contada e reportada, nunca corrigida |
| `POST /wagering/transactions` | submete uma transação (header `Idempotency-Key` obrigatório) |
| `GET /wagering/transactions/:transactionId` | consulta por id interno |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | consulta por id do provedor |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId/failures` | recusas auditadas dessa transação (motivo, categoria, se é `retryable`), mais recentes primeiro |
| `GET /health/live` · `GET /health/ready` · `GET /metrics` | liveness, readiness (PostgreSQL + SQS) e Prometheus; sem autenticação |

```bash
curl -s -X POST localhost:8080/wagering/transactions \
  -H 'content-type: application/json' -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"<uuid>","walletId":"<uuid>",
       "roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
# 201 {"transactionId":"…","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
```

As mensagens SQS (`wager-transactions.fifo`) seguem o envelope da seção 10 do enunciado (`messageId`, `type`, `occurredAt`, `data` + `idempotencyKey`). O produtor deve usar `MessageGroupId = walletId`. O consumidor reutiliza o mesmo use case do HTTP.

## Configuração

Toda a configuração vem do ambiente e é validada com zod no boot. Exemplos estão em [.env.example](.env.example). As principais:

| Variável | Padrão | Papel |
|---|---|---|
| `APP_ROLES` | `api,consumer,outbox,pending-ref` | loops ativos nesta instância (todas as combinações são seguras com N réplicas) |
| `DATABASE_URL` | — | role de runtime `wagering_app` (só DML; sem UPDATE/DELETE no ledger) |
| `DB_LOCK_TIMEOUT_MS` | `2000` | espera máxima pelo lock da wallet; depois disso o erro é transitório (503 / retry na fila) |
| `SQS_MAX_RECEIVE_COUNT` | `5` | tentativas antes da DLQ (redrive policy) |
| `PENDING_REF_BASE_MS` · `PENDING_REF_MAX_MS` · `PENDING_REF_MAX_ATTEMPTS` | `2000` · `300000` · `10` | backoff de referências fora de ordem (~35 min) |
| `SQS_BATCH_PER_WALLET` | `true` | aplica as mensagens de uma wallet de cada receive numa única transação SQL |
| `SUBMISSION_FAILURE_RETENTION_DAYS` | `90` | retenção da auditoria de recusas |
| `OTEL_ENABLED` | `false` | tracing OTLP (HTTP, PostgreSQL, SQS, outbox) |

## Estrutura

```
src/
  modules/{wallet,wagering,messaging}/{domain,application,infrastructure}   # hexagonal por contexto
  modules/auth/                     # ProviderIdentityPort + ProviderAuthGuard (ponto de extensão)
  infrastructure/{database,messaging,http,health,observability,workers}
  composition/core.ts               # composição sem framework (usada pelo Nest e pelos testes)
  bootstrap.ts · main.ts            # instância por papéis + shutdown ordenado
test/{unit,integration,concurrency,helpers}
load/                               # teste de carga
docker/ · docker-compose.yml · docker-compose.test.yml · Dockerfile
```
