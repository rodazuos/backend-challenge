import type { ResolvePendingReferences } from "../../modules/wagering/application/resolve-pending-references";
import type { AppLogger } from "../../shared/application/observability";
import type { UnitOfWork } from "../../shared/application/unit-of-work";
import type { Clock } from "../../shared/domain/ports";
import { PollingWorker } from "./polling-worker";

/** Scheduled worker for PENDING_REFERENCE transactions (exponential backoff lives in the use case's policy). */
export class PendingReferenceWorker extends PollingWorker {
  constructor(
    private readonly useCase: ResolvePendingReferences,
    private readonly batchSize: number,
    pollIntervalMs: number,
    logger: AppLogger,
  ) {
    super("pending-reference-worker", logger, pollIntervalMs);
  }

  protected async tick(): Promise<boolean> {
    const result = await this.useCase.runOnce();
    if (result.claimed > 0) this.logger.debug({ ...result }, "pending references evaluated");
    return result.claimed >= this.batchSize;
  }
}

/**
 * Inbox retention. The inbox is a de-duplication window for queue redeliveries (minutes/hours in practice);
 * business idempotency is permanent through wager_transactions' unique constraints, so purging old inbox rows
 * never re-enables a double effect.
 */
export class InboxPurgeWorker extends PollingWorker {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: Clock,
    private readonly retentionDays: number,
    logger: AppLogger,
  ) {
    super("inbox-purge", logger, 60 * 60 * 1000);
  }

  protected async tick(): Promise<boolean> {
    const cutoff = new Date(this.clock.now().getTime() - this.retentionDays * 86_400_000);
    const removed = await this.uow.transaction((s) => s.inbox.purgeReceivedBefore(cutoff, 5000), {
      label: "inbox-purge",
    });
    if (removed > 0) this.logger.info({ removed }, "inbox rows purged");
    return removed >= 5000;
  }
}
