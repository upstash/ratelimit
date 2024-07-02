import { Cache } from "./cache";
import type { Duration } from "./duration";
import { ms } from "./duration";
import { safeEval } from "./hash";
import {
  fixedWindowLimitScript,
  fixedWindowRemainingTokensScript,
  slidingWindowLimitScript,
  slidingWindowRemainingTokensScript,
} from "./lua-scripts/multi";
import { resetScript } from "./lua-scripts/reset";
import { Ratelimit } from "./ratelimit";
import type { Algorithm, RegionContext, MultiRegionContext } from "./types";

import type { Redis } from "./types";

function randomId(): string {
  let result = "";
  const characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  const charactersLength = characters.length;
  for (let i = 0; i < 16; i++) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
  }
  return result;
}

export type MultiRegionRatelimitConfig = {
  /**
   * Instances of `@upstash/redis`
   * @see https://github.com/upstash/upstash-redis#quick-start
   */
  redis: Redis[];
  /**
   * The ratelimiter function to use.
   *
   * Choose one of the predefined ones or implement your own.
   * Available algorithms are exposed via static methods:
   * - MultiRegionRatelimit.fixedWindow
   */
  limiter: Algorithm<MultiRegionContext>;
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
   * if the map or the ratelimit instance is created outside your serverless function handler.
   */
  ephemeralCache?: Map<string, number> | false;

  /**
   * If set, the ratelimiter will allow requests to pass after this many milliseconds.
   *
   * Use this if you want to allow requests in case of network problems
   */
  timeout?: number;

  /**
   * If enabled, the ratelimiter will store analytics data in redis, which you can check out at
   * https://console.upstash.com/ratelimit
   *
   * @default false
   */
  analytics?: boolean;

  /**
   * If enabled, lua scripts will be sent to Redis with SCRIPT LOAD durint the first request.
   * In the subsequent requests, hash of the script will be used to invoke it
   * 
   * @default true
   */
  cacheScripts?: boolean;
};

/**
 * Ratelimiter using serverless redis from https://upstash.com/
 *
 * @example
 * ```ts
 * const { limit } = new MultiRegionRatelimit({
 *    redis: Redis.fromEnv(),
 *    limiter: MultiRegionRatelimit.fixedWindow(
 *      10,     // Allow 10 requests per window of 30 minutes
 *      "30 m", // interval of 30 minutes
 *    )
 * })
 *
 * ```
 */
export class MultiRegionRatelimit extends Ratelimit<MultiRegionContext> {
  /**
   * Create a new Ratelimit instance by providing a `@upstash/redis` instance and the algorithn of your choice.
   */
  constructor(config: MultiRegionRatelimitConfig) {
    super({
      prefix: config.prefix,
      limiter: config.limiter,
      timeout: config.timeout,
      analytics: config.analytics,
      ctx: {
        regionContexts: config.redis.map(redis => ({
          redis: redis,
          scriptHashes: {},
          cacheScripts: config.cacheScripts ?? true,
        })),
        cache: config.ephemeralCache ? new Cache(config.ephemeralCache) : undefined,
      },
    });
  }

  /**
   * Each request inside a fixed time increases a counter.
   * Once the counter reaches the maximum allowed number, all further requests are
   * rejected.
   *
   * **Pro:**
   *
   * - Newer requests are not starved by old ones.
   * - Low storage cost.
   *
   * **Con:**
   *
   * A burst of requests near the boundary of a window can result in a very
   * high request rate because two windows will be filled with requests quickly.
   *
   * @param tokens - How many requests a user can make in each time window.
   * @param window - A fixed timeframe
   */
  static fixedWindow(
    /**
     * How many requests are allowed per window.
     */
    tokens: number,
    /**
     * The duration in which `tokens` requests are allowed.
     */
    window: Duration,
  ): Algorithm<MultiRegionContext> {
    const windowDuration = ms(window);

    return () => ({
      async limit(ctx: MultiRegionContext, identifier: string, rate?: number) {
        if (ctx.cache) {
          const { blocked, reset } = ctx.cache.isBlocked(identifier);
          if (blocked) {
            return {
              success: false,
              limit: tokens,
              remaining: 0,
              reset: reset,
              pending: Promise.resolve(),
              reason: "cacheBlock"
            };
          }
        }

        const requestId = randomId();
        const bucket = Math.floor(Date.now() / windowDuration);
        const key = [identifier, bucket].join(":");
        const incrementBy = rate ? Math.max(1, rate) : 1;

        const dbs: { redis: Redis; request: Promise<string[]> }[] = ctx.regionContexts.map((regionContext) => ({
          redis: regionContext.redis,
          request: safeEval(
            regionContext,
            fixedWindowLimitScript,
            "limitHash",
            [key],
            [requestId, windowDuration, incrementBy],
          ) as Promise<string[]>,
        }));

        // The firstResponse is an array of string at every EVEN indexes and rate at which the tokens are used at every ODD indexes
        const firstResponse = await Promise.any(dbs.map((s) => s.request));

        const usedTokens = firstResponse.reduce((accTokens: number, usedToken, index) => {
          let parsedToken = 0;
          if (index % 2) {
            parsedToken = Number.parseInt(usedToken);
          }

          return accTokens + parsedToken;
        }, 0);

        const remaining = tokens - usedTokens;

        /**
         * If the length between two databases does not match, we sync the two databases
         */
        async function sync() {
          const individualIDs = await Promise.all(dbs.map((s) => s.request));

          const allIDs = Array.from(
            new Set(
              individualIDs
                .flatMap((_) => _)
                .reduce((acc: string[], curr, index) => {
                  if (index % 2 === 0) {
                    acc.push(curr);
                  }
                  return acc;
                }, []),
            ).values(),
          );

          for (const db of dbs) {
            const usedDbTokens = (await db.request).reduce(
              (accTokens: number, usedToken, index) => {
                let parsedToken = 0;
                if (index % 2) {
                  parsedToken = Number.parseInt(usedToken);
                }

                return accTokens + parsedToken;
              },
              0,
            );

            const dbIds = (await db.request).reduce((ids: string[], currentId, index) => {
              if (index % 2 === 0) {
                ids.push(currentId);
              }
              return ids;
            }, []);
            /**
             * If the bucket in this db is already full, it doesn't matter which ids it contains.
             * So we do not have to sync.
             */
            if (usedDbTokens >= tokens) {
              continue;
            }
            const diff = allIDs.filter((id) => !dbIds.includes(id));
            /**
             * Don't waste a request if there is nothing to send
             */
            if (diff.length === 0) {
              continue;
            }

            for (const requestId of diff) {
              await db.redis.hset(key, { [requestId]: incrementBy });
            }
          }
        }

        /**
         * Do not await sync. This should not run in the critical path.
         */

        const success = remaining > 0;
        const reset = (bucket + 1) * windowDuration;

        if (ctx.cache && !success) {
          ctx.cache.blockUntil(identifier, reset);
        }
        return {
          success,
          limit: tokens,
          remaining,
          reset,
          pending: sync(),
        };
      },
      async getRemaining(ctx: MultiRegionContext, identifier: string) {
        const bucket = Math.floor(Date.now() / windowDuration);
        const key = [identifier, bucket].join(":");

        const dbs: { redis: Redis; request: Promise<string[]> }[] = ctx.regionContexts.map((regionContext) => ({
          redis: regionContext.redis,
          request: safeEval(
            regionContext,
            fixedWindowRemainingTokensScript,
            "getRemainingHash",
            [key],
            [null]
          ) as Promise<string[]>,
        }));

        // The firstResponse is an array of string at every EVEN indexes and rate at which the tokens are used at every ODD indexes
        const firstResponse = await Promise.any(dbs.map((s) => s.request));
        const usedTokens = firstResponse.reduce((accTokens: number, usedToken, index) => {
          let parsedToken = 0;
          if (index % 2) {
            parsedToken = Number.parseInt(usedToken);
          }

          return accTokens + parsedToken;
        }, 0);

        return {
          remaining: Math.max(0, tokens - usedTokens),
          reset: (bucket + 1) * windowDuration
        };
      },
      async resetTokens(ctx: MultiRegionContext, identifier: string) {
        const pattern = [identifier, "*"].join(":");
        if (ctx.cache) {
          ctx.cache.pop(identifier)
        }

        await Promise.all(ctx.regionContexts.map((regionContext) => {
          safeEval(
            regionContext,
            resetScript,
            "resetHash",
            [pattern],
            [null]
          );
        }))
      },
    });
  }

  /**
   * Combined approach of `slidingLogs` and `fixedWindow` with lower storage
   * costs than `slidingLogs` and improved boundary behavior by calculating a
   * weighted score between two windows.
   *
   * **Pro:**
   *
   * Good performance allows this to scale to very high loads.
   *
   * **Con:**
   *
   * Nothing major.
   *
   * @param tokens - How many requests a user can make in each time window.
   * @param window - The duration in which the user can max X requests.
   */
  static slidingWindow(
    /**
     * How many requests are allowed per window.
     */
    tokens: number,
    /**
     * The duration in which `tokens` requests are allowed.
     */
    window: Duration,
  ): Algorithm<MultiRegionContext> {
    const windowSize = ms(window);

    const windowDuration = ms(window);

    return () => ({
      async limit(ctx: MultiRegionContext, identifier: string, rate?: number) {
        if (ctx.cache) {
          const { blocked, reset } = ctx.cache.isBlocked(identifier);
          if (blocked) {
            return {
              success: false,
              limit: tokens,
              remaining: 0,
              reset: reset,
              pending: Promise.resolve(),
              reason: "cacheBlock"
            };
          }
        }

        const requestId = randomId();
        const now = Date.now();

        const currentWindow = Math.floor(now / windowSize);
        const currentKey = [identifier, currentWindow].join(":");
        const previousWindow = currentWindow - 1;
        const previousKey = [identifier, previousWindow].join(":");
        const incrementBy = rate ? Math.max(1, rate) : 1;

        const dbs = ctx.regionContexts.map((regionContext) => ({
          redis: regionContext.redis,
          request: safeEval(
            regionContext,
            slidingWindowLimitScript,
            "limitHash",
            [currentKey, previousKey],
            [tokens, now, windowDuration, requestId, incrementBy],
            // lua seems to return `1` for true and `null` for false
          ) as Promise<[string[], string[], 1 | null]>,
        }));

        const percentageInCurrent = (now % windowDuration) / windowDuration;
        const [current, previous, success] = await Promise.any(dbs.map((s) => s.request));

        // in the case of success, the new request is not included in the current array.
        // add it manually
        if (success) {
          current.push(requestId, incrementBy.toString())
        }

        const previousUsedTokens = previous.reduce((accTokens: number, usedToken, index) => {
          let parsedToken = 0;
          if (index % 2) {
            parsedToken = Number.parseInt(usedToken);
          }

          return accTokens + parsedToken;
        }, 0);

        const currentUsedTokens = current.reduce((accTokens: number, usedToken, index) => {
          let parsedToken = 0;
          if (index % 2) {
            parsedToken = Number.parseInt(usedToken);
          }

          return accTokens + parsedToken;
        }, 0);

        const previousPartialUsed = Math.ceil(previousUsedTokens * (1 - percentageInCurrent));

        const usedTokens = previousPartialUsed + currentUsedTokens;

        const remaining = tokens - usedTokens;

        /**
         * If a database differs from the consensus, we sync it
         */
        async function sync() {
          const res = await Promise.all(dbs.map((s) => s.request));

          const allCurrentIds = Array.from(
            new Set(
              res
                .flatMap(([current]) => current)
                .reduce((acc: string[], curr, index) => {
                  if (index % 2 === 0) {
                    acc.push(curr);
                  }
                  return acc;
                }, []),
            ).values(),
          );

          for (const db of dbs) {
            const [current, _previous, _success] = await db.request;
            const dbIds = current.reduce((ids: string[], currentId, index) => {
              if (index % 2 === 0) {
                ids.push(currentId);
              }
              return ids;
            }, []);

            const usedDbTokens = current.reduce((accTokens: number, usedToken, index) => {
              let parsedToken = 0;
              if (index % 2) {
                parsedToken = Number.parseInt(usedToken);
              }

              return accTokens + parsedToken;
            }, 0);
            /**
             * If the bucket in this db is already full, it doesn't matter which ids it contains.
             * So we do not have to sync.
             */
            if (usedDbTokens >= tokens) {
              continue;
            }
            const diff = allCurrentIds.filter((id) => !dbIds.includes(id));
            /**
             * Don't waste a request if there is nothing to send
             */
            if (diff.length === 0) {
              continue;
            }

            for (const requestId of diff) {
              await db.redis.hset(currentKey, { [requestId]: incrementBy });
            }
          }
        }

        // const success = remaining >= 0;
        const reset = (currentWindow + 1) * windowDuration;
        if (ctx.cache && !success) {
          ctx.cache.blockUntil(identifier, reset);
        }
        return {
          success: Boolean(success),
          limit: tokens,
          remaining: Math.max(0, remaining),
          reset,
          pending: sync(),
        };
      },
      async getRemaining(ctx: MultiRegionContext, identifier: string) {
        const now = Date.now();

        const currentWindow = Math.floor(now / windowSize);
        const currentKey = [identifier, currentWindow].join(":");
        const previousWindow = currentWindow - 1;
        const previousKey = [identifier, previousWindow].join(":");

        const dbs = ctx.regionContexts.map((regionContext) => ({
          redis: regionContext.redis,
          request: safeEval(
            regionContext,
            slidingWindowRemainingTokensScript,
            "getRemainingHash",
            [currentKey, previousKey],
            [now, windowSize],
            // lua seems to return `1` for true and `null` for false
          ) as Promise<number>,
        }));

        const usedTokens = await Promise.any(dbs.map((s) => s.request));
        return {
          remaining: Math.max(0, tokens - usedTokens),
          reset: (currentWindow + 1) * windowSize
        };
      },
      async resetTokens(ctx: MultiRegionContext, identifier: string) {
        const pattern = [identifier, "*"].join(":");
        if (ctx.cache) {
          ctx.cache.pop(identifier)
        }

        
        await Promise.all(ctx.regionContexts.map((regionContext) => {
          safeEval(
            regionContext,
            resetScript,
            "resetHash",
            [pattern],
            [null]
          );
        }))
      },
    });
  }
}
