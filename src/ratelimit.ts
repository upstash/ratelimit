import { Analytics, type Geo } from "./analytics";
import { Cache } from "./cache";
import type { Algorithm, Context, RatelimitResponse } from "./types";

export class TimeoutError extends Error {
  constructor() {
    super("Timeout");
    this.name = "TimeoutError";
  }
}
export type RatelimitConfig<TContext> = {
  /**
   * The ratelimiter function to use.
   *
   * Choose one of the predefined ones or implement your own.
   * Available algorithms are exposed via static methods:
   * - Ratelimiter.fixedWindow
   * - Ratelimiter.slidingWindow
   * - Ratelimiter.tokenBucket
   */

  limiter: Algorithm<TContext>;

  ctx: TContext;
  /**
   * All keys in redis are prefixed with this.
   *
   * @default `@upstash/ratelimit`
   */
  prefix?: string;

  /**
   * If enabled, the ratelimiter will keep a global cache of identifiers, that have
   * exhausted their ratelimit. In serverless environments this is only possible if
   * you create the ratelimiter instance outside of your handler function. While the
   * function is still hot, the ratelimiter can block requests without having to
   * request data from redis, thus saving time and money.
   *
   * Whenever an identifier has exceeded its limit, the ratelimiter will add it to an
   * internal list together with its reset timestamp. If the same identifier makes a
   * new request before it is reset, we can immediately reject it.
   *
   * Set to `false` to disable.
   *
   * If left undefined, a map is created automatically, but it can only work
   * if the map or the  ratelimit instance is created outside your serverless function handler.
   */
  ephemeralCache?: Map<string, number> | false;

  /**
   * If set, the ratelimiter will allow requests to pass after this many milliseconds.
   *
   * Use this if you want to allow requests in case of network problems
   *
   * @default 5000
   */
  timeout?: number;

  /**
   * If enabled, the ratelimiter will store analytics data in redis, which you can check out at
   * https://console.upstash.com/ratelimit
   *
   * @default false
   */
  analytics?: boolean;
};

/**
 * Ratelimiter using serverless redis from https://upstash.com/
 *
 * @example
 * ```ts
 * const { limit } = new Ratelimit({
 *    redis: Redis.fromEnv(),
 *    limiter: Ratelimit.slidingWindow(
 *      10,     // Allow 10 requests per window of 30 minutes
 *      "30 m", // interval of 30 minutes
 *    ),
 * })
 *
 * ```
 */
export abstract class Ratelimit<TContext extends Context> {
  protected readonly limiter: Algorithm<TContext>;

  protected readonly ctx: TContext;

  protected readonly prefix: string;

  protected readonly timeout: number;

  protected readonly analytics?: Analytics;

  constructor(config: RatelimitConfig<TContext>) {
    this.ctx = config.ctx;
    this.limiter = config.limiter;
    this.timeout = config.timeout ?? 5000;
    this.prefix = config.prefix ?? "@upstash/ratelimit";
    this.analytics = config.analytics
      ? new Analytics({
          redis: Array.isArray(this.ctx.redis) ? this.ctx.redis[0] : this.ctx.redis,
          prefix: this.prefix,
        })
      : undefined;

    if (config.ephemeralCache instanceof Map) {
      this.ctx.cache = new Cache(config.ephemeralCache);
    } else if (typeof config.ephemeralCache === "undefined") {
      this.ctx.cache = new Cache(new Map());
    }
  }

  /**
   * Determine if a request should pass or be rejected based on the identifier and previously chosen ratelimit.
   *
   * Use this if you want to reject all requests that you can not handle right now.
   *
   * @example
   * ```ts
   *  const ratelimit = new Ratelimit({
   *    redis: Redis.fromEnv(),
   *    limiter: Ratelimit.slidingWindow(10, "10 s")
   *  })
   *
   *  const { success } = await ratelimit.limit(id)
   *  if (!success){
   *    return "Nope"
   *  }
   *  return "Yes"
   * ```
   *
   * @param req.rate - The rate at which tokens will be added or consumed from the token bucket. A higher rate allows for more requests to be processed. Defaults to 1 token per interval if not specified.
   *
   * Usage with `req.rate`
   * @example
   * ```ts
   *  const ratelimit = new Ratelimit({
   *    redis: Redis.fromEnv(),
   *    limiter: Ratelimit.slidingWindow(100, "10 s")
   *  })
   *
   *  const { success } = await ratelimit.limit(id, {rate: 10})
   *  if (!success){
   *    return "Nope"
   *  }
   *  return "Yes"
   * ```
   */
  public limit = async (
    identifier: string,
    req?: { geo?: Geo; rate?: number },
  ): Promise<RatelimitResponse> => {
    const key = [this.prefix, identifier].join(":");
    let timeoutId: any = null;
    try {
      const arr: Promise<RatelimitResponse>[] = [this.limiter().limit(this.ctx, key, req?.rate)];
      if (this.timeout > 0) {
        arr.push(
          new Promise((resolve) => {
            timeoutId = setTimeout(() => {
              resolve({
                success: true,
                limit: 0,
                remaining: 0,
                reset: 0,
                pending: Promise.resolve(),
              });
            }, this.timeout);
          }),
        );
      }

      const res = await Promise.race(arr);
      if (this.analytics) {
        try {
          const geo = req ? this.analytics.extractGeo(req) : undefined;
          const analyticsP = this.analytics
            .record({
              identifier,
              time: Date.now(),
              success: res.success,
              ...geo,
            })
            .catch((err) => {
              console.warn("Failed to record analytics", err);
            });
          res.pending = Promise.all([res.pending, analyticsP]);
        } catch (err) {
          console.warn("Failed to record analytics", err);
        }
      }
      return res;
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  };

  /**
   * Block until the request may pass or timeout is reached.
   *
   * This method returns a promise that resolves as soon as the request may be processed
   * or after the timeout has been reached.
   *
   * Use this if you want to delay the request until it is ready to get processed.
   *
   * @example
   * ```ts
   *  const ratelimit = new Ratelimit({
   *    redis: Redis.fromEnv(),
   *    limiter: Ratelimit.slidingWindow(10, "10 s")
   *  })
   *
   *  const { success } = await ratelimit.blockUntilReady(id, 60_000)
   *  if (!success){
   *    return "Nope"
   *  }
   *  return "Yes"
   * ```
   */
  public blockUntilReady = async (
    /**
     * An identifier per user or api.
     * Choose a userID, or api token, or ip address.
     *
     * If you want to limit your api across all users, you can set a constant string.
     */
    identifier: string,
    /**
     * Maximum duration to wait in milliseconds.
     * After this time the request will be denied.
     */
    timeout: number,
  ): Promise<RatelimitResponse> => {
    if (timeout <= 0) {
      throw new Error("timeout must be positive");
    }
    let res: RatelimitResponse;

    const deadline = Date.now() + timeout;
    while (true) {
      res = await this.limit(identifier);
      if (res.success) {
        break;
      }
      if (res.reset === 0) {
        throw new Error("This should not happen");
      }

      const wait = Math.min(res.reset, deadline) - Date.now();
      await new Promise((r) => setTimeout(r, wait));

      if (Date.now() > deadline) {
        break;
      }
    }
    return res!;
  };

  public resetUsedTokens = async (identifier: string) => {
    const script = `
      local pattern = KEYS[1]

      -- Initialize cursor to start from 0
      local cursor = "0"

      repeat
          -- Scan for keys matching the pattern
          local scan_result = redis.call('SCAN', cursor, 'MATCH', pattern)

          -- Extract cursor for the next iteration
          cursor = scan_result[1]

          -- Extract keys from the scan result
          local keys = scan_result[2]

          for i=1, #keys do
          redis.call('DEL', keys[i])
          end

      -- Continue scanning until cursor is 0 (end of keyspace)
      until cursor == "0"
    `;
    const pattern = [this.prefix, identifier, "*"].join(":");

    if (Array.isArray(this.ctx.redis)) {
      for (const db of this.ctx.redis) {
        await db.eval(script, [pattern], [null]);
      }
    } else {
      await this.ctx.redis.eval(script, [pattern], [null]);
    }
  };

  public getRemaining = async (identifier: string) => {
    const pattern = [this.prefix, identifier].join(":");

    return await this.limiter().getRemaining(this.ctx, pattern);
  };
}
