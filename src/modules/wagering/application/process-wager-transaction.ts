import {
  IdempotencyConflictError,
  ValidationError,
  WalletNotFoundForSubmissionError,
} from "../../../shared/application/errors";
import {
  type AppLogger,
  type AppMetrics,
  type Channel,
  logContext,
} from "../../../shared/application/observability";
import type { TransactionScope, UnitOfWork } from "../../../shared/application/unit-of-work";
import { DomainError } from "../../../shared/domain/domain-error";
import { Money, type MoneyProps } from "../../../shared/domain/money";
import type { Clock, IdGenerator } from "../../../shared/domain/ports";
import { InboxMessage } from "../../messaging/domain/inbox-message";
import { OutboxMessage } from "../../messaging/domain/outbox-message";
import { counterpartyAccountFor } from "../domain/counterparty";
import { WagerTransactionFailed } from "../domain/events/wagering-events";
import type { FailureCode } from "../domain/failure-code";
import { computeWagerPayloadHash } from "../domain/payload-hash";
import { type ReferenceLookup, WagerProcessor } from "../domain/wager-processor";
import { WagerTransaction } from "../domain/wager-transaction";
import {
  EXTERNAL_KINDS,
  type ExternalWagerKind,
  type WagerTransactionKind,
  WagerTransactionStatus,
} from "../domain/wager-transaction-kind";
import type { ReferenceRetryPolicy } from "./reference-retry-policy";
import { outboxMessagesFor } from "./wager-outcome-events";

export interface SubmitWagerCommand {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: ExternalWagerKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string | undefined;
  channel: Channel;
  correlationId: string;
  causationId?: string | undefined;
  /** Present for queue deliveries: persistent de-duplication by (consumerName, messageId). */
  inbox?: { consumerName: string; messageId: string; payloadHash: string } | undefined;
}

export interface SubmitWagerResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: MoneyProps;
  failureCode?: FailureCode | undefined;
  idempotentReplay: boolean;
  /** Queue only: the inbox already had this messageId; nothing was executed. */
  duplicateMessage: boolean;
}

/**
 * The single wagering use case shared by HTTP and SQS. One SQL transaction contains:
 * inbox record → wallet row lock → idempotency check → business rules → transaction row →
 * wallet balance → ledger entry → outbox events. Either all of it commits or none of it does.
 */
export class ProcessWagerTransaction {
  private readonly processor = new WagerProcessor();

  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly retryPolicy: ReferenceRetryPolicy,
    private readonly metrics: AppMetrics,
    private readonly logger: AppLogger,
  ) {}

  async execute(cmd: SubmitWagerCommand): Promise<SubmitWagerResult> {
    const started = performance.now();
    if (!(EXTERNAL_KINDS as readonly string[]).includes(cmd.kind)) {
      throw new ValidationError(`kind ${String(cmd.kind)} cannot be submitted`, { kind: cmd.kind });
    }
    const money = this.parseMoney(cmd.money);
    const payloadHash = computeWagerPayloadHash(cmd);
    logContext.assign({
      walletId: cmd.walletId,
      providerId: cmd.providerId,
      correlationId: cmd.correlationId,
    });

    const result = await this.uow.transaction((scope) => this.run(scope, cmd, money, payloadHash), {
      label: "process-wager",
    });

    this.record(cmd, result, started);
    return result;
  }

  /**
   * Records the submission as FAILED (terminal, auditable, no balance effect) after an *unexpected* error exhausted
   * its retries. Deliberately not used for transient infrastructure failures: those stay recoverable (DLQ redrive,
   * provider resend) instead of being frozen as a permanent answer for the idempotency key.
   *
   * Returns undefined when there is nothing valid to record against (unknown wallet, invalid contract), and the
   * existing transaction when the key was meanwhile processed (FAILED never overwrites a real outcome).
   */
  async markFailed(cmd: SubmitWagerCommand, code: FailureCode): Promise<SubmitWagerResult | undefined> {
    let money: Money;
    try {
      money = this.parseMoney(cmd.money);
    } catch {
      return undefined;
    }
    const payloadHash = computeWagerPayloadHash(cmd);
    return this.uow.transaction(
      async (scope) => {
        const now = this.clock.now();
        const wallet = await scope.wallets.lockById(cmd.walletId);
        if (!wallet) return undefined;
        const { byKey, byExternal } = await scope.wagers.findIdempotencyCandidates(
          cmd.idempotencyKey,
          cmd.providerId,
          cmd.externalTransactionId,
        );
        const existing = byKey ?? byExternal;
        if (existing) return toResult(existing, true);

        let tx: WagerTransaction;
        try {
          tx = this.createTransaction(cmd, money, payloadHash, now);
        } catch {
          return undefined;
        }
        tx.fail(code, now, wallet.balance);
        await scope.wagers.insert(tx);
        await scope.outbox.add([
          OutboxMessage.enqueue(
            WagerTransactionFailed.from(tx, {
              eventId: this.ids.next(),
              correlationId: cmd.correlationId,
              causationId: cmd.causationId,
              occurredAt: now,
            }),
          ),
        ]);
        this.metrics.transactionOutcome({ kind: cmd.kind, status: tx.status, channel: cmd.channel });
        this.logger.error({ transactionId: tx.id, failureCode: code }, "wager transaction marked FAILED");
        return toResult(tx, false);
      },
      { label: "mark-wager-failed" },
    );
  }

  private async run(
    scope: TransactionScope,
    cmd: SubmitWagerCommand,
    money: Money,
    payloadHash: string,
  ): Promise<SubmitWagerResult> {
    const now = this.clock.now();

    // 1. Inbox: first statement, so a concurrent redelivery blocks here until we commit/rollback.
    //    The row only becomes visible at COMMIT, together with the effect, so it is written as processed.
    if (cmd.inbox) {
      const inbox = InboxMessage.receive({ ...cmd.inbox, receivedAt: now });
      inbox.markProcessed(now);
      const inserted = await scope.inbox.tryInsert(inbox);
      if (!inserted) return this.duplicateMessage(scope, cmd);
    }

    // 2. Serialise on the wallet (unit of concurrency). Different wallets never contend.
    const wallet = await scope.wallets.lockById(cmd.walletId);
    if (!wallet) throw new WalletNotFoundForSubmissionError(cmd.walletId);

    // 3. Idempotency — evaluated *after* the lock, so a concurrent duplicate sees the committed original.
    //    One round trip for both uniqueness keys (idempotency key, provider + external id).
    const { byKey: existing, byExternal: sameExternal } = await scope.wagers.findIdempotencyCandidates(
      cmd.idempotencyKey,
      cmd.providerId,
      cmd.externalTransactionId,
    );
    if (existing) {
      if (!existing.matchesPayload(payloadHash)) {
        throw new IdempotencyConflictError("PAYLOAD_MISMATCH", {
          idempotencyKey: cmd.idempotencyKey,
          transactionId: existing.id,
        });
      }
      return toResult(existing, true);
    }
    if (sameExternal) {
      throw new IdempotencyConflictError("EXTERNAL_ID_REUSED", {
        providerId: cmd.providerId,
        externalTransactionId: cmd.externalTransactionId,
        transactionId: sameExternal.id,
      });
    }

    // 4. Build the transaction (contract validation lives in the factory).
    const tx = this.createTransaction(cmd, money, payloadHash, now);
    logContext.assign({ transactionId: tx.id });

    // 5. Resolve reference (same wallet lock protects it) and apply business rules.
    const reference = tx.hasReference() ? await lookupReference(scope, tx) : undefined;
    const versionBefore = wallet.version;
    const outcome = this.processor.process({
      wallet,
      transaction: tx,
      reference,
      ledgerEntryId: this.ids.next(),
      at: now,
      nextReferenceRetryAt: this.retryPolicy.nextRetryAt(0, now),
      referenceRetryExhausted: false,
    });

    // 6. Persist everything in this transaction.
    await scope.wagers.insert(tx);
    if (outcome.type === "PROCESSED" && outcome.ledgerEntry) {
      await scope.wallets.saveBalance(wallet, versionBefore);
      await scope.ledger.insert(outcome.ledgerEntry, counterpartyAccountFor(tx));
    }
    const events = outboxMessagesFor(
      outcome,
      tx,
      wallet,
      { correlationId: cmd.correlationId, causationId: cmd.causationId, occurredAt: now },
      this.ids,
    );
    await scope.outbox.add(events);
    if (outcome.type === "PROCESSED") {
      await scope.wagers.nudgeDependents(wallet.id, tx.providerId, tx.externalTransactionId, now);
    }
    return toResult(tx, false);
  }

  private async duplicateMessage(
    scope: TransactionScope,
    cmd: SubmitWagerCommand,
  ): Promise<SubmitWagerResult> {
    const inboxCmd = cmd.inbox;
    if (!inboxCmd) throw new Error("unreachable: duplicate without inbox");
    const previous = await scope.inbox.find(inboxCmd.consumerName, inboxCmd.messageId);
    if (previous && previous.payloadHash !== inboxCmd.payloadHash) {
      throw new IdempotencyConflictError("INBOX_PAYLOAD_MISMATCH", { messageId: inboxCmd.messageId });
    }
    const tx = await scope.wagers.findByIdempotencyKey(cmd.idempotencyKey);
    if (!tx) {
      // Inbox row exists but its transaction does not: only possible if the original was a permanent
      // rejection that committed no transaction row (e.g. WALLET_NOT_FOUND never commits) — treat as conflict.
      throw new IdempotencyConflictError("INBOX_PAYLOAD_MISMATCH", { messageId: inboxCmd.messageId });
    }
    return { ...toResult(tx, true), duplicateMessage: true };
  }

  private createTransaction(
    cmd: SubmitWagerCommand,
    money: Money,
    payloadHash: string,
    now: Date,
  ): WagerTransaction {
    try {
      return WagerTransaction.create({
        id: this.ids.next(),
        providerId: cmd.providerId,
        externalTransactionId: cmd.externalTransactionId,
        idempotencyKey: cmd.idempotencyKey,
        payloadHash,
        walletId: cmd.walletId,
        playerId: cmd.playerId,
        roundId: cmd.roundId,
        gameId: cmd.gameId,
        kind: cmd.kind as WagerTransactionKind,
        money,
        referenceExternalTransactionId: cmd.referenceExternalTransactionId,
        createdAt: now,
      });
    } catch (error) {
      if (error instanceof DomainError) throw new ValidationError(error.message, { code: error.code });
      throw error;
    }
  }

  private parseMoney(props: MoneyProps): Money {
    try {
      return Money.from(props);
    } catch (error) {
      if (error instanceof DomainError) throw new ValidationError(error.message, { field: "money" });
      throw error;
    }
  }

  private record(cmd: SubmitWagerCommand, result: SubmitWagerResult, started: number): void {
    const seconds = (performance.now() - started) / 1000;
    if (result.duplicateMessage) this.metrics.inboxDuplicate();
    if (result.idempotentReplay) this.metrics.idempotentReplay(cmd.channel);
    else this.metrics.transactionOutcome({ kind: cmd.kind, status: result.status, channel: cmd.channel });
    this.metrics.processingDuration({ channel: cmd.channel, outcome: result.status }, seconds);
    this.logger.info(
      {
        transactionId: result.transactionId,
        status: result.status,
        failureCode: result.failureCode,
        kind: cmd.kind,
        idempotentReplay: result.idempotentReplay,
        duplicateMessage: result.duplicateMessage,
        durationMs: Math.round(seconds * 1000),
      },
      "wager transaction handled",
    );
  }
}

export async function lookupReference(
  scope: TransactionScope,
  tx: WagerTransaction,
): Promise<ReferenceLookup> {
  const refExternalId = tx.referenceExternalTransactionId;
  if (!refExternalId) throw new Error("lookupReference requires a reference");
  const ref = await scope.wagers.findByProviderExternalId(tx.providerId, refExternalId);
  if (!ref) return { found: false };
  const alreadyReversed =
    tx.isReversal() && ref.status === WagerTransactionStatus.Processed
      ? await scope.wagers.isReversed(ref.id)
      : false;
  return { found: true, transaction: ref, alreadyReversed };
}

export function toResult(tx: WagerTransaction, idempotentReplay: boolean): SubmitWagerResult {
  const balance = tx.resultBalance;
  if (!balance) throw new Error(`transaction ${tx.id} has no result balance`);
  return {
    transactionId: tx.id,
    status: tx.status,
    balance: balance.toJSON(),
    failureCode: tx.failureCode,
    idempotentReplay,
    duplicateMessage: false,
  };
}
