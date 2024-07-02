import { Pipeline } from "@upstash/redis";
import { Geo } from "./analytics";

/**
 * EphemeralCache is used to block certain identifiers right away in case they have already exceeded the ratelimit.
 */
export interface EphemeralCache {
  isBlocked: (identifier: string) => { blocked: boolean; reset: number };
  blockUntil: (identifier: string, reset: number) => void;

  set: (key: string, value: number) => void;
  get: (key: string) => number | null;

  incr: (key: string) => number;

  pop: (key: string) => void;
  empty: () => void;

  size: () => number;
}

export type RegionContext = {
  redis: Redis;
  cache?: EphemeralCache,
  scriptHashes: {
    limitHash?: string,
    getRemainingHash?: string,
    resetHash?: string
  },
  cacheScripts: boolean,
};
export type MultiRegionContext = { regionContexts: Omit<RegionContext[], "cache">; cache?: EphemeralCache };

export type RatelimitResponseType = "timeout" | "cacheBlock" | "denyList"

export type Context = RegionContext | MultiRegionContext;
export type RatelimitResponse = {
  /**
   * Whether the request may pass(true) or exceeded the limit(false)
   */
  success: boolean;
  /**
   * Maximum number of requests allowed within a window.
   */
  limit: number;
  /**
   * How many requests the user has left within the current window.
   */
  remaining: number;
  /**
   * Unix timestamp in milliseconds when the limits are reset.
   */
  reset: number;

  /**
   * For the MultiRegion setup we do some synchronizing in the background, after returning the current limit.
   * Or when analytics is enabled, we send the analytics asynchronously after returning the limit.
   * In most case you can simply ignore this.
   *
   * On Vercel Edge or Cloudflare workers, you need to explicitly handle the pending Promise like this:
   *
   * ```ts
   * const { pending } = await ratelimit.limit("id")
   * context.waitUntil(pending)
   * ```
   *
   * See `waitUntil` documentation in
   * [Cloudflare](https://developers.cloudflare.com/workers/runtime-apis/handlers/fetch/#contextwaituntil)
   * and [Vercel](https://vercel.com/docs/functions/edge-middleware/middleware-api#waituntil)
   * for more details.
   * ```
   */
  pending: Promise<unknown>;

  /**
   * Reason behind the result in `success` field.
   * - Is set to "timeout" when request times out
   * - Is set to "cacheBlock" when an identifier is blocked through cache without calling redis because it was
   *    rate limited previously.
   * - Is set to "denyList" when identifier or one of ip/user-agent/country parameters is in deny list. To enable
   *    deny list, see `enableProtection` parameter. To edit the deny list, see the Upstash Ratelimit Dashboard
   *    at https://console.upstash.com/ratelimit.
   * - Is set to undefined if rate limit check had to use Redis. This happens in cases when `success` field in
   *    the response is true. It can also happen the first time sucecss is false.
   */
  reason?: RatelimitResponseType;

  /**
   * The value which was in the deny list if reason: "denyList"
   */
  deniedValue?: DeniedValue
};

export type Algorithm<TContext> = () => {
  limit: (
    ctx: TContext,
    identifier: string,
    rate?: number,
    opts?: {
      cache?: EphemeralCache;
    },
  ) => Promise<RatelimitResponse>;
  getRemaining: (ctx: TContext, identifier: string) => Promise<{
    remaining: number,
    reset: number
  }>;
  resetTokens: (ctx: TContext, identifier: string) => Promise<void>;
};

export type IsDenied = 0 | 1;

export type DeniedValue = string | undefined;
export type DenyListResponse = { deniedValue: DeniedValue, invalidIpDenyList: boolean }

export const DenyListExtension = "denyList" as const
export const IpDenyListKey = "ipDenyList" as const
export const IpDenyListStatusKey = "ipDenyListStatus" as const

export type LimitPayload = [RatelimitResponse, DenyListResponse];
export type LimitOptions = {
  geo?: Geo,
  rate?: number,
  ip?: string,
  userAgent?: string,
  country?: string
}

/**
 * This is all we need from the redis sdk.
 */
export interface Redis {
  sadd: <TData>(key: string, ...members: TData[]) => Promise<number>;

  hset: <TValue>(key: string, obj: { [key: string]: TValue }) => Promise<number>;

  eval: <TArgs extends unknown[], TData = unknown>(
    ...args: [script: string, keys: string[], args: TArgs]
  ) => Promise<TData>;

  evalsha: <TArgs extends unknown[], TData = unknown>(
    ...args: [sha1: string, keys: string[], args: TArgs]
  ) => Promise<TData>;

  scriptLoad: (
    ...args: [script: string]
  ) => Promise<string>;

  smismember: (
    key: string, members: string[]
  ) => Promise<IsDenied[]>;

  multi: () => Pipeline
}
