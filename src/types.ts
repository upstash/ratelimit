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

  reason?: "ratelimit" | "ip-blacklist" | "timeout"
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
  getRemaining: (ctx: TContext, identifier: string) => Promise<number>;
  resetTokens: (ctx: TContext, identifier: string) => Promise<void>;
};

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
}
