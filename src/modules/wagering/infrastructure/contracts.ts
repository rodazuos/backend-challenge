import { z } from "zod";
import { EXTERNAL_KINDS } from "../domain/wager-transaction-kind";

/*
 * Transport contracts shared by HTTP and SQS. Shape is validated here (strict: unknown keys are rejected);
 * money format and business rules are validated by the domain (Money.from, WagerTransaction.create).
 */

const id = (max: number) =>
  z
    .string()
    .min(1)
    .max(max)
    .regex(/^[A-Za-z0-9._:-]+$/, "only letters, digits and . _ : - are allowed");

export const MoneyContract = z.strictObject({
  amount: z.string(),
  currency: z.string(),
});

/** "system" is reserved for internal OPENING transactions. */
const providerId = id(64).refine((v) => v.toLowerCase() !== "system", "providerId 'system' is reserved");

export const WagerPayloadContract = z.strictObject({
  providerId,
  externalTransactionId: id(128),
  playerId: z.uuid(),
  walletId: z.uuid(),
  roundId: id(128),
  gameId: id(128),
  kind: z.enum(EXTERNAL_KINDS.map(String) as [string, ...string[]]),
  money: MoneyContract,
  referenceExternalTransactionId: id(128).optional(),
});
export type WagerPayload = z.infer<typeof WagerPayloadContract>;

export const IdempotencyKeyContract = z
  .string({ error: "Idempotency-Key header is required" })
  .min(1, "Idempotency-Key header is required")
  .max(255)
  .regex(/^[\x21-\x7E]+$/, "Idempotency-Key must be printable ASCII without spaces");

/** SQS message envelope (section 10). */
export const WagerMessageContract = z.strictObject({
  messageId: z.string().min(1).max(128),
  type: z.literal("WagerTransactionRequested"),
  occurredAt: z.iso.datetime({ offset: true }),
  correlationId: z.string().min(1).max(128).optional(),
  data: WagerPayloadContract.extend({ idempotencyKey: IdempotencyKeyContract }),
});
export type WagerMessage = z.infer<typeof WagerMessageContract>;

export const CreateWalletContract = z.strictObject({
  playerId: z.uuid(),
  initialBalance: MoneyContract,
});

export const LedgerQueryContract = z.strictObject({
  cursor: z.string().min(1).max(200).optional(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
});

export const UuidParam = z.uuid();
