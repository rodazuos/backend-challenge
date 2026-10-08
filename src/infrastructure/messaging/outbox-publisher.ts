import type { OutboxMessage } from "../../modules/messaging/domain/outbox-message";
import type { AppLogger, AppMetrics } from "../../shared/application/observability";
import type { UnitOfWork } from "../../shared/application/unit-of-work";
import type { Clock } from "../../shared/domain/ports";
import { withSpan } from "../observability/tracing";
import type { FaultInjector } from "../workers/fault-injector";
import { PollingWorker } from "../workers/polling-worker";
import type { OutgoingEvent, SqsGateway } from "./sqs-gateway";

export interface OutboxPublisherSettings {
  owner: string;
  batchSize: number;
  leaseMs: number;
  pollIntervalMs: number;
}

/**
 * Transactional outbox relay. Safe with N concurrent publishers on N instances:
 *  1. claim a batch with FOR UPDATE SKIP LOCKED + lease (short transaction, committed immediately);
 *  2. publish outside any DB transaction;
 *  3. mark published (or schedule a retry with exponential backoff).
 * If the process dies between 1 and 3, the lease expires and another publisher re-sends: delivery is
 * at-least-once, and the eventId (= outbox id = SQS MessageDeduplicationId) lets consumers drop duplicates.
 * Events are only ever read from committed rows, so nothing is published before the financial commit.
 */
export class OutboxPublisher extends PollingWorker {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly sqs: SqsGateway,
    private readonly clock: Clock,
    private readonly settings: OutboxPublisherSettings,
    private readonly metrics: AppMetrics,
    logger: AppLogger,
    private readonly faults: FaultInjector,
  ) {
    super("outbox-publisher", logger, settings.pollIntervalMs);
  }

  protected async tick(): Promise<boolean> {
    const published = await this.publishOnce();
    return published >= this.settings.batchSize;
  }

  /** One claim/publish/mark cycle. Returns the number of claimed messages. */
  async publishOnce(): Promise<number> {
    const now = this.clock.now();
    const claimed = await this.uow.transaction(
      (s) =>
        s.outbox.claimDue(
          now,
          this.settings.batchSize,
          new Date(now.getTime() + this.settings.leaseMs),
          this.settings.owner,
        ),
      { label: "outbox-claim" },
    );
    if (claimed.length === 0) return 0;
    // Span only for non-empty cycles: idle polling would otherwise flood the tracing backend.
    return withSpan(
      "outbox publish",
      { "outbox.owner": this.settings.owner, "outbox.claimed": claimed.length },
      () => this.publishClaimed(claimed),
    );
  }

  private async publishClaimed(claimed: OutboxMessage[]): Promise<number> {
    await this.faults.hit("after-claim-before-publish");

    const failed = new Map<string, string>();
    for (let i = 0; i < claimed.length; i += 10) {
      const chunk = claimed.slice(i, i + 10);
      try {
        const { failedIds } = await this.sqs.publishEvents(chunk.map(toOutgoing));
        for (const id of failedIds) failed.set(id, "rejected by broker");
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        for (const m of chunk) failed.set(m.id, message);
      }
    }
    await this.faults.hit("after-publish-before-mark");

    const publishedAt = this.clock.now();
    const ok = claimed.filter((m) => !failed.has(m.id));
    const retry = claimed.filter((m) => failed.has(m.id));
    await this.uow.transaction(
      async (s) => {
        await s.outbox.markPublished(
          ok.map((m) => m.id),
          publishedAt,
        );
        for (const m of retry) {
          m.scheduleRetry(publishedAt, failed.get(m.id));
          await s.outbox.scheduleRetry(m);
        }
      },
      { label: "outbox-mark" },
    );

    if (ok.length > 0) {
      this.metrics.outboxPublished(
        ok.length,
        ok.map((m) => (publishedAt.getTime() - m.occurredAt.getTime()) / 1000),
      );
    }
    if (retry.length > 0) {
      for (const _ of retry) this.metrics.outboxPublishFailure();
      this.logger.warn(
        { failed: retry.length, published: ok.length },
        "outbox publish partially failed; will retry",
      );
    }
    return claimed.length;
  }

  protected override async onStopped(): Promise<void> {
    // Hand claimed-but-unpublished rows back immediately instead of waiting for the lease to expire.
    await this.uow
      .transaction((s) => s.outbox.releaseLeases(this.settings.owner), { label: "outbox-release" })
      .catch((error) => this.logger.warn({ err: error }, "could not release outbox leases"));
  }
}

function toOutgoing(m: OutboxMessage): OutgoingEvent {
  return {
    id: m.id,
    groupId: m.aggregateId,
    body: JSON.stringify(m.payload),
    attributes: { eventType: m.eventType, eventId: m.id, eventVersion: String(m.eventVersion) },
  };
}
