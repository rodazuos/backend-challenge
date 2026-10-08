import type { InboxMessage } from "../domain/inbox-message";
import type { OutboxMessage } from "../domain/outbox-message";

export interface InboxRepository {
  /** INSERT … ON CONFLICT DO NOTHING. Returns false when (consumer, messageId) already exists. */
  tryInsert(message: InboxMessage): Promise<boolean>;
  find(consumerName: string, messageId: string): Promise<InboxMessage | undefined>;
  markProcessed(message: InboxMessage): Promise<void>;
  purgeReceivedBefore(cutoff: Date, limit: number): Promise<number>;
}

export interface OutboxStats {
  pending: number;
  oldestPendingOccurredAt: Date | undefined;
}

export interface OutboxRepository {
  add(messages: readonly OutboxMessage[]): Promise<void>;
  /** Leases up to `limit` due messages (FOR UPDATE SKIP LOCKED); expired leases are reclaimable. */
  claimDue(now: Date, limit: number, leaseUntil: Date, owner: string): Promise<OutboxMessage[]>;
  markPublished(ids: readonly string[], at: Date): Promise<void>;
  scheduleRetry(message: OutboxMessage): Promise<void>;
  releaseLeases(owner: string): Promise<number>;
  stats(): Promise<OutboxStats>;
}
