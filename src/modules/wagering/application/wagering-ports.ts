import type { WagerTransaction } from "../domain/wager-transaction";

export interface ClaimedPendingReference {
  id: string;
  walletId: string;
}

export interface WagerTransactionRepository {
  findById(id: string): Promise<WagerTransaction | undefined>;
  findByIdempotencyKey(key: string): Promise<WagerTransaction | undefined>;
  findByProviderExternalId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransaction | undefined>;
  /** Row lock on the transaction; callers must already hold the wallet lock (lock ordering: wallet → transaction). */
  lockById(id: string): Promise<WagerTransaction | undefined>;
  insert(tx: WagerTransaction): Promise<void>;
  /** Persists the mutable lifecycle columns (status, outcome, retry bookkeeping). */
  update(tx: WagerTransaction): Promise<void>;
  /** True when a PROCESSED REFUND/ROLLBACK already references `transactionId`. */
  isReversed(transactionId: string): Promise<boolean>;
  /** Makes PENDING_REFERENCE dependents of a just-processed transaction due now (same wallet only). */
  nudgeDependents(
    walletId: string,
    providerId: string,
    externalTransactionId: string,
    now: Date,
  ): Promise<number>;
  /**
   * Claims due PENDING_REFERENCE transactions with FOR UPDATE SKIP LOCKED and pushes their next retry to
   * `leaseUntil`, so a crashed worker's claim simply expires and another instance picks it up.
   */
  claimDuePendingReferences(now: Date, limit: number, leaseUntil: Date): Promise<ClaimedPendingReference[]>;
  countPendingReferences(): Promise<number>;
}
