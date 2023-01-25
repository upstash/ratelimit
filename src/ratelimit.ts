import { Cache } from "./cache.ts";
import type { Algorithm, Context, RatelimitResponse } from "./types.ts";
import { Analytics } from "./analytics.ts";
import { config } from "https://deno.land/x/dotenv@v3.2.0/mod.ts";

config({ export: true });

class TimeoutError extends Error {
  constructor() {
    super("timeout reached");
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
   * - Ratelimiter.slidingLogs
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
   * If enabled, the ratelimiter will store analytics data in redis, which you can check out at
   * https://upstash.com/ratelimit
   *
   * @default true
   */
  analytics?: boolean;

  /**
   * If defined, the request will be allowed to pass through after this timeout.
   *
   * Use this if you require low latency at the cost of accuracy.
   */
  timeout?: number;
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
 *    )
 * })
 *
 * ```
 */
export abstract class Ratelimit<TContext extends Context> {
  protected readonly limiter: Algorithm<TContext>;

  protected readonly ctx: TContext;

  protected readonly prefix: string;

  protected readonly analytics?: Analytics;

  protected readonly timeout?: number;

  constructor(config: RatelimitConfig<TContext>) {
    this.ctx = config.ctx;
    this.limiter = config.limiter;
    this.prefix = config.prefix ?? "@upstash/ratelimit";
    this.timeout = config.timeout;

    if (config.analytics !== false) {
      this.analytics = new Analytics({
        redis: Array.isArray(config.ctx.redis)
          ? config.ctx.redis[0]
          : config.ctx.redis,
        prefix: this.prefix,
      });
    }

    if (config.ephemeralCache instanceof Map) {
      this.ctx.cache = new Cache(config.ephemeralCache);
    } else if (typeof config.ephemeralCache === "undefined") {
      this.ctx.cache = new Cache(new Map());
    }
  }

  public getLimit = async (
    identifier: string,
  ): Promise<Omit<RatelimitResponse, "success">> => {
    throw new Error("Not implemented yet");
  };

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
   */
  public limit = async (identifier: string, req?: {
    geo?: {
      city?: string;
      country?: string;
      region?: string;
      latitude?: string;
      longitude?: string;
      ip?: string;
    };
  }): Promise<RatelimitResponse> => {
    const key = [this.prefix, identifier].join(":");
    let timeoutId: number | null = null;
    try {
      if (this.timeout) {
        timeoutId = setTimeout(() => {
          throw new TimeoutError();
        }, this.timeout);
      }

      const res = await this.limiter(this.ctx, key);
      if (this.analytics) {
        res.pending = Promise.all([
          res.pending,
          this.analytics.record({
            time: Date.now(),
            success: res.success,
            identifier,
            ...req?.geo,
          }),
        ]);
      }
      return res;
    } catch (err) {
      if (err instanceof TimeoutError) {
        return {
          success: true,
          limit: 0,
          remaining: 0,
          reset: 0,
          pending: Promise.resolve(),
        };
      }
      throw err;
    } finally {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    }
  };

  /**
   * Block until the request may pass or timeout is reached.
   *
   * This method returns a promsie that resolves as soon as the request may be processed
   * or after the timeoue has been reached.
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
}
