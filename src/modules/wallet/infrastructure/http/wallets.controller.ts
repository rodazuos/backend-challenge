import { Body, Controller, Get, HttpCode, HttpStatus, Param, Post, Query } from "@nestjs/common";
import { parseOrThrow } from "../../../../infrastructure/http/validation";
import { logContext } from "../../../../shared/application/observability";
import {
  CreateWalletContract,
  LedgerQueryContract,
  UuidParam,
} from "../../../wagering/infrastructure/contracts";
import {
  CreateWallet,
  GetWallet,
  type LedgerPage,
  ListWalletLedger,
  ReconcileWallet,
  type ReconciliationReport,
  type WalletView,
} from "../../application/wallet-use-cases";

@Controller("wallets")
export class WalletsController {
  constructor(
    private readonly createWallet: CreateWallet,
    private readonly getWallet: GetWallet,
    private readonly listLedger: ListWalletLedger,
    private readonly reconcileWallet: ReconcileWallet,
  ) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  create(@Body() body: unknown): Promise<WalletView> {
    const input = parseOrThrow(CreateWalletContract, body, "wallet");
    return this.createWallet.execute({ ...input, correlationId: logContext.get().correlationId ?? "n/a" });
  }

  @Get(":walletId")
  get(@Param("walletId") walletId: string): Promise<WalletView> {
    return this.getWallet.execute(parseOrThrow(UuidParam, walletId, "walletId"));
  }

  @Get(":walletId/ledger")
  ledger(@Param("walletId") walletId: string, @Query() query: unknown): Promise<LedgerPage> {
    const id = parseOrThrow(UuidParam, walletId, "walletId");
    const { cursor, limit } = parseOrThrow(LedgerQueryContract, query, "query");
    return this.listLedger.execute(id, cursor, limit);
  }

  /** Read-only check; divergences are reported, logged and counted — never corrected silently. */
  @Post(":walletId/reconciliation")
  @HttpCode(HttpStatus.OK)
  reconcile(@Param("walletId") walletId: string): Promise<ReconciliationReport> {
    return this.reconcileWallet.execute(parseOrThrow(UuidParam, walletId, "walletId"));
  }
}
