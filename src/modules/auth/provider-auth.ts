import {
  type CanActivate,
  type ExecutionContext,
  ForbiddenException,
  Inject,
  Injectable,
  SetMetadata,
  UnauthorizedException,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import type { Request } from "express";

/**
 * Authentication extension point (section 2). Authentication is intentionally NOT implemented:
 * the port and the guard are real, the adapter is a no-op that trusts every caller.
 *
 * Intended production adapter (documented in ARCHITECTURE.md): each game provider is an OIDC client in
 * Keycloak/Zitadel using the client-credentials grant; the adapter validates the bearer JWT against the IdP's
 * JWKS (issuer, audience, expiry) and maps the `provider_id` claim to the principal below.
 */
export interface ProviderPrincipal {
  /** "*" means trusted internal caller (no-op adapter); otherwise the authenticated provider id. */
  providerId: string;
  subject: string;
}

export interface ProviderIdentityPort {
  /** Returns the authenticated principal, or undefined when the request carries no valid credentials. */
  resolve(request: Request): Promise<ProviderPrincipal | undefined>;
}

export const PROVIDER_IDENTITY = Symbol("PROVIDER_IDENTITY");
export const IS_PUBLIC = "auth:isPublic";

/** Marks routes that never require authentication (health, metrics). */
export const Public = () => SetMetadata(IS_PUBLIC, true);

export class NoopProviderIdentity implements ProviderIdentityPort {
  async resolve(): Promise<ProviderPrincipal> {
    return { providerId: "*", subject: "anonymous" };
  }
}

@Injectable()
export class ProviderAuthGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    @Inject(PROVIDER_IDENTITY) private readonly identity: ProviderIdentityPort,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC, [
      context.getHandler(),
      context.getClass(),
    ]);
    if (isPublic) return true;

    const request = context.switchToHttp().getRequest<Request & { principal?: ProviderPrincipal }>();
    const principal = await this.identity.resolve(request);
    if (!principal) throw new UnauthorizedException("missing or invalid credentials");
    request.principal = principal;

    // Authorisation: a provider may only act on its own transactions.
    const bodyProvider = (request.body as { providerId?: unknown } | undefined)?.providerId;
    const paramProvider = request.params?.providerId;
    const claimed = typeof bodyProvider === "string" ? bodyProvider : paramProvider;
    if (principal.providerId !== "*" && claimed !== undefined && claimed !== principal.providerId) {
      throw new ForbiddenException("provider mismatch");
    }
    return true;
  }
}
