import { Body, Controller, Get, Headers, HttpStatus, Param, Post, Res } from "@nestjs/common";
import type { Response } from "express";
import { parseOrThrow } from "../../../../infrastructure/http/validation";
import { logContext } from "../../../../shared/application/observability";
import { ProcessWagerTransaction, type SubmitWagerResult } from "../../application/process-wager-transaction";
import { GetWagerTransaction, type WagerTransactionView } from "../../application/wager-queries";
import type { ExternalWagerKind } from "../../domain/wager-transaction-kind";
import { WagerTransactionStatus } from "../../domain/wager-transaction-kind";
import { IdempotencyKeyContract, UuidParam, WagerPayloadContract } from "../contracts";

/**
 * Transaction outcome → HTTP status (identical for first execution and replays; `idempotentReplay`
 * and the `Idempotent-Replayed` header tell them apart):
 *   PROCESSED → 201, PENDING_REFERENCE → 202, REJECTED → 422 (persisted, carries failureCode), FAILED → 500.
 */
const OUTCOME_STATUS: Record<WagerTransactionStatus, number> = {
  [WagerTransactionStatus.Processed]: HttpStatus.CREATED,
  [WagerTransactionStatus.PendingReference]: HttpStatus.ACCEPTED,
  [WagerTransactionStatus.Pending]: HttpStatus.ACCEPTED,
  [WagerTransactionStatus.Rejected]: HttpStatus.UNPROCESSABLE_ENTITY,
  [WagerTransactionStatus.Failed]: HttpStatus.INTERNAL_SERVER_ERROR,
};

@Controller()
export class WageringController {
  constructor(
    private readonly processWager: ProcessWagerTransaction,
    private readonly getWager: GetWagerTransaction,
  ) {}

  @Post("wagering/transactions")
  async submit(
    @Headers("idempotency-key") idempotencyKey: string | undefined,
    @Body() body: unknown,
    @Res({ passthrough: true }) res: Response,
  ): Promise<Omit<SubmitWagerResult, "duplicateMessage">> {
    const key = parseOrThrow(IdempotencyKeyContract, idempotencyKey, "Idempotency-Key header");
    const payload = parseOrThrow(WagerPayloadContract, body, "wager transaction");
    const { correlationId = key } = logContext.get();

    const result = await this.processWager.execute({
      ...payload,
      kind: payload.kind as ExternalWagerKind,
      idempotencyKey: key,
      channel: "http",
      correlationId,
      causationId: key,
    });

    res.status(OUTCOME_STATUS[result.status]);
    if (result.idempotentReplay) res.setHeader("Idempotent-Replayed", "true");
    const { duplicateMessage: _ignored, ...body_ } = result;
    return body_;
  }

  @Get("wagering/transactions/:transactionId")
  byId(@Param("transactionId") transactionId: string): Promise<WagerTransactionView> {
    return this.getWager.byId(parseOrThrow(UuidParam, transactionId, "transactionId"));
  }

  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  byExternalId(
    @Param("providerId") providerId: string,
    @Param("externalTransactionId") externalTransactionId: string,
  ): Promise<WagerTransactionView> {
    return this.getWager.byProviderExternalId(providerId, externalTransactionId);
  }
}
