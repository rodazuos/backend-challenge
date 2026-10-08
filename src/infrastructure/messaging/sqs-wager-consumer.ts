import type { Message } from "@aws-sdk/client-sqs";
import type {
  ProcessWagerTransaction,
  SubmitWagerCommand,
} from "../../modules/wagering/application/process-wager-transaction";
import {
  classifySubmissionError,
  type FailureClassification,
  type RecordSubmissionFailure,
} from "../../modules/wagering/application/submission-failures";
import { FailureCode } from "../../modules/wagering/domain/failure-code";
import { SubmissionFailureCategory } from "../../modules/wagering/domain/submission-failure";
import type { ExternalWagerKind } from "../../modules/wagering/domain/wager-transaction-kind";
import { WagerMessageContract } from "../../modules/wagering/infrastructure/contracts";
import { ApplicationError } from "../../shared/application/errors";
import { type AppLogger, type AppMetrics, logContext } from "../../shared/application/observability";
import { computeBackoffMs } from "../../shared/domain/backoff";
import { canonicalJson, type Json, sha256Hex } from "../../shared/domain/canonical-json";
import { extractTraceContext, SpanKind, withSpan } from "../observability/tracing";
import type { FaultInjector } from "../workers/fault-injector";
import type { SqsGateway } from "./sqs-gateway";

export interface ConsumerSettings {
  consumerName: string;
  concurrency: number;
  waitTimeSeconds: number;
  visibilityTimeoutSeconds: number;
  maxReceiveCount: number;
  backoffBaseSeconds: number;
  backoffMaxSeconds: number;
}

type Decision =
  | { kind: "ack"; duplicate: boolean }
  | { kind: "retry"; reason: string }
  | { kind: "dlq"; reason: string; detail: string };

/**
 * Consumes `wager-transactions.fifo` and delegates to the same use case as HTTP.
 *
 *  - business outcome (PROCESSED/REJECTED/PENDING_REFERENCE) → committed → ack (delete)
 *  - transient failure (DB/lock/connection) → visibility backoff; on the last allowed attempt it is audited
 *    (RETRIES_EXHAUSTED, still recoverable) and dead-lettered
 *  - unexpected error on the last attempt → transaction recorded as FAILED (auditable, terminal), audited, DLQ
 *  - permanent failure (malformed message, idempotency conflict, unknown wallet) → audited + explicit DLQ + delete
 * Every dead-lettered message leaves an audit row (wager_submission_failures) and a WagerSubmissionFailed event;
 * the redrive policy (maxReceiveCount) remains as a safety net for messages that crash the consumer itself.
 *
 * Ack happens strictly after the SQL commit. A crash between commit and ack causes a redelivery, which the
 * persistent inbox recognises (no second effect). Messages of the same MessageGroupId (wallet) inside a batch
 * are processed sequentially to preserve order; different wallets run in parallel.
 */
export class SqsWagerConsumer {
  private running = false;
  private loop: Promise<void> | undefined;
  private poll: AbortController | undefined;
  private readonly inFlight = new Set<Promise<void>>();
  private readonly pending = new Set<Message>();

  constructor(
    private readonly sqs: SqsGateway,
    private readonly processWager: ProcessWagerTransaction,
    private readonly recordFailure: RecordSubmissionFailure,
    private readonly settings: ConsumerSettings,
    private readonly metrics: AppMetrics,
    private readonly logger: AppLogger,
    private readonly faults: FaultInjector,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loop = this.run();
    this.logger.info({ worker: "sqs-consumer", queue: this.sqs.settings.wagerQueueName }, "worker started");
  }

  /**
   * Graceful stop (SIGTERM): stop polling, let in-flight messages finish within `graceMs`, and hand back
   * every message we hold but have not started (visibility = 0) so another instance picks it up immediately.
   */
  async stop(graceMs: number): Promise<void> {
    if (!this.running) return;
    this.running = false;
    this.poll?.abort();
    await Promise.race([Promise.allSettled([...this.inFlight]), Bun.sleep(graceMs)]);
    await Promise.allSettled([...this.pending].map((m) => this.release(m)));
    await this.loop?.catch(() => {});
    this.logger.info({ worker: "sqs-consumer", abandoned: this.inFlight.size }, "worker stopped");
  }

  private async run(): Promise<void> {
    let failures = 0;
    while (this.running) {
      try {
        if (this.inFlight.size >= this.settings.concurrency) {
          await Promise.race(this.inFlight);
          continue;
        }
        this.poll = new AbortController();
        const messages = await this.sqs.receiveWagers(
          this.settings.concurrency - this.inFlight.size,
          this.settings.waitTimeSeconds,
          this.poll.signal,
        );
        failures = 0;
        if (!this.running) {
          await Promise.allSettled(messages.map((m) => this.release(m)));
          break;
        }
        for (const group of groupByMessageGroup(messages)) {
          for (const m of group) this.pending.add(m);
          const task = this.processGroup(group).finally(() => this.inFlight.delete(task));
          this.inFlight.add(task);
        }
      } catch (error) {
        if (!this.running) break;
        failures++;
        this.logger.warn({ err: error, failures }, "SQS receive failed");
        await Bun.sleep(Math.min(1000 * 2 ** failures, 30_000));
      }
    }
  }

  private async processGroup(group: Message[]): Promise<void> {
    for (const message of group) {
      if (!this.running && this.pending.has(message)) continue; // left for release() on shutdown
      this.pending.delete(message);
      await this.handle(message);
    }
  }

  private async handle(message: Message): Promise<void> {
    const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? 1);
    const correlationId = message.MessageAttributes?.correlationId?.StringValue;
    const carrier = Object.fromEntries(
      Object.entries(message.MessageAttributes ?? {}).map(([k, v]) => [k, v.StringValue]),
    );
    await logContext.run({ messageId: message.MessageId, correlationId, channel: "sqs" }, () =>
      withSpan(
        "wager-transactions process",
        {
          "messaging.system": "aws_sqs",
          "messaging.message.id": message.MessageId,
          "messaging.receive_count": receiveCount,
        },
        async (span) => {
          const heartbeat = this.startHeartbeat(message);
          let decision: Decision;
          try {
            decision = await this.decide(message, receiveCount);
          } finally {
            clearInterval(heartbeat);
          }
          span.setAttribute("wagering.decision", decision.kind);
          await this.apply(message, decision, receiveCount);
        },
        SpanKind.CONSUMER,
        extractTraceContext(carrier),
      ),
    );
  }

  private async decide(message: Message, receiveCount: number): Promise<Decision> {
    let raw: unknown;
    try {
      raw = JSON.parse(message.Body ?? "");
    } catch {
      return this.deadLetter(message, undefined, receiveCount, {
        reasonCode: "INVALID_JSON",
        category: SubmissionFailureCategory.Validation,
        retryable: false,
        detail: "body is not valid JSON",
      });
    }
    const result = WagerMessageContract.safeParse(raw);
    if (!result.success) {
      const detail = result.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ");
      return this.deadLetter(message, raw, receiveCount, {
        reasonCode: "INVALID_MESSAGE",
        category: SubmissionFailureCategory.Validation,
        retryable: false,
        detail,
      });
    }
    const parsed = result.data;
    const { data } = parsed;
    logContext.assign({
      messageId: parsed.messageId,
      correlationId: parsed.correlationId ?? logContext.get().correlationId ?? parsed.messageId,
      providerId: data.providerId,
      walletId: data.walletId,
    });
    const { idempotencyKey, ...payload } = data;
    const command: SubmitWagerCommand = {
      ...payload,
      kind: payload.kind as ExternalWagerKind,
      idempotencyKey,
      channel: "sqs",
      correlationId: logContext.get().correlationId ?? parsed.messageId,
      causationId: parsed.messageId,
      inbox: {
        consumerName: this.settings.consumerName,
        // The envelope's business content; the same messageId with a different body is a conflict.
        messageId: parsed.messageId,
        payloadHash: `v1:${sha256Hex(canonicalJson(data as unknown as Json))}`,
      },
    };

    try {
      this.faults.throwIf("consumer-unexpected-error");
      const outcome = await this.processWager.execute(command);
      // Committed. A crash right here is the "died after commit, before ack" scenario.
      await this.faults.hit("after-commit-before-ack");
      return { kind: "ack", duplicate: outcome.duplicateMessage };
    } catch (error) {
      if (error instanceof ApplicationError && error.kind !== "TRANSIENT") {
        // Permanent by nature (malformed, conflict, unknown wallet): audit and dead-letter now.
        return this.deadLetter(message, parsed, receiveCount, classifySubmissionError(error));
      }
      const unexpected = !(error instanceof ApplicationError);
      const reason = unexpected ? "UNEXPECTED_ERROR" : (error as ApplicationError).code;
      if (receiveCount < this.settings.maxReceiveCount) {
        this.logger.warn({ err: error, reason, receiveCount }, "wager message failed; will be retried");
        return { kind: "retry", reason };
      }
      // Last attempt. Transient failures stay recoverable (DLQ redrive / provider resend). An unexpected error
      // that survived every retry is a defect: freeze the submission as FAILED so the provider gets a definitive,
      // auditable answer instead of silence.
      let transactionId: string | undefined;
      if (unexpected) {
        const failed = await this.processWager
          .markFailed(command, FailureCode.InfraPermanentFailure)
          .catch((e) => this.logger.error({ err: e }, "could not mark transaction FAILED"));
        transactionId = failed?.transactionId;
      }
      return this.deadLetter(
        message,
        parsed,
        receiveCount,
        {
          reasonCode: unexpected ? "UNEXPECTED_ERROR" : "RETRIES_EXHAUSTED",
          category: SubmissionFailureCategory.Infrastructure,
          retryable: !unexpected,
          detail: `${reason} after ${receiveCount} attempts: ${error instanceof Error ? error.message : String(error)}`,
        },
        transactionId,
      );
    }
  }

  /** Audits the failure (and its WagerSubmissionFailed event), then dead-letters the message. */
  private async deadLetter(
    message: Message,
    content: unknown,
    receiveCount: number,
    classification: FailureClassification,
    transactionId?: string,
  ): Promise<Decision> {
    const envelope = (content && typeof content === "object" ? content : {}) as Record<string, unknown>;
    const data = (envelope.data && typeof envelope.data === "object" ? envelope.data : {}) as Record<
      string,
      unknown
    >;
    const str = (v: unknown) => (typeof v === "string" ? v : undefined);
    await this.recordFailure.execute({
      channel: "sqs",
      ...classification,
      providerId: str(data.providerId),
      externalTransactionId: str(data.externalTransactionId),
      idempotencyKey: str(data.idempotencyKey),
      walletId: str(data.walletId),
      messageId: str(envelope.messageId) ?? message.MessageId,
      transactionId,
      receiveCount,
    });
    return { kind: "dlq", reason: classification.reasonCode, detail: classification.detail };
  }

  private async apply(message: Message, decision: Decision, receiveCount: number): Promise<void> {
    const handle = message.ReceiptHandle;
    if (!handle) return;
    try {
      switch (decision.kind) {
        case "ack":
          await this.sqs.ack(handle);
          this.metrics.sqsMessage(decision.duplicate ? "duplicate" : "ack");
          return;
        case "dlq":
          await this.sqs.sendToDlq(message, decision.reason, decision.detail);
          await this.sqs.ack(handle);
          this.metrics.sqsMessage("dlq");
          this.logger.error(
            { reason: decision.reason, receiveCount },
            "message dead-lettered (permanent failure)",
          );
          return;
        case "retry": {
          const delay = computeBackoffMs(receiveCount - 1, {
            baseMs: this.settings.backoffBaseSeconds * 1000,
            maxMs: this.settings.backoffMaxSeconds * 1000,
            jitter: 0.3,
          });
          await this.sqs.changeVisibility(handle, delay / 1000);
          this.metrics.sqsMessage("retry");
          this.logger.warn(
            {
              reason: decision.reason,
              receiveCount,
              retryInMs: delay,
              maxReceiveCount: this.settings.maxReceiveCount,
            },
            "message will be retried",
          );
          return;
        }
      }
    } catch (error) {
      // Ack/visibility failed: the message reappears after its visibility timeout and the inbox absorbs it.
      this.logger.warn(
        { err: error, decision: decision.kind },
        "SQS settle call failed; relying on redelivery",
      );
    }
  }

  private startHeartbeat(message: Message): ReturnType<typeof setInterval> {
    const every = Math.max(1, this.settings.visibilityTimeoutSeconds / 2) * 1000;
    return setInterval(() => {
      if (!message.ReceiptHandle) return;
      this.sqs
        .changeVisibility(message.ReceiptHandle, this.settings.visibilityTimeoutSeconds)
        .catch(() => {});
    }, every);
  }

  private async release(message: Message): Promise<void> {
    if (!message.ReceiptHandle) return;
    this.pending.delete(message);
    await this.sqs.changeVisibility(message.ReceiptHandle, 0).catch(() => {});
  }
}

function groupByMessageGroup(messages: Message[]): Message[][] {
  const groups = new Map<string, Message[]>();
  for (const m of messages) {
    const key = m.Attributes?.MessageGroupId ?? m.MessageId ?? "";
    const list = groups.get(key);
    if (list) list.push(m);
    else groups.set(key, [m]);
  }
  return [...groups.values()];
}
