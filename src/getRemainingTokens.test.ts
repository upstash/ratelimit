import { describe, expect, test } from "bun:test";
import { Redis } from "@upstash/redis";
import { MultiRegionRatelimit } from "./multi";
import type { Ratelimit } from "./ratelimit";
import { RegionRatelimit } from "./single";
import { Algorithm, Context, MultiRegionContext, RegionContext } from "./types";

const limit = 10;
const refillRate = 10;
const windowString = "30s";

function run<TContext extends Context>(builder: Ratelimit<TContext>) {
    const id = crypto.randomUUID();

    describe("getRemainingTokens", () => {
        test("get remaining tokens", async () => {
            for (let i = 0; i < 10; i++) {
                await builder.limit(id);
                const remaining = await builder.getRemaining(id);
                expect(remaining).toBe(limit - i - 1);
            }
        }, 20000);
    });
}

function newRegion(limiter: Algorithm<RegionContext>): Ratelimit<RegionContext> {
    return new RegionRatelimit({
        prefix: crypto.randomUUID(),
        redis: Redis.fromEnv(),
        limiter,
    });
}

function newMultiRegion(limiter: Algorithm<MultiRegionContext>): Ratelimit<MultiRegionContext> {
    function ensureEnv(key: string): string {
        const value = process.env[key];
        if (!value) {
            throw new Error(`Environment variable ${key} not found`);
        }
        return value;
    }

    return new MultiRegionRatelimit({
        prefix: crypto.randomUUID(),
        redis: [
            new Redis({
                url: ensureEnv("EU2_UPSTASH_REDIS_REST_URL"),
                token: ensureEnv("EU2_UPSTASH_REDIS_REST_TOKEN"),
            }),
            new Redis({
                url: ensureEnv("APN_UPSTASH_REDIS_REST_URL"),
                token: ensureEnv("APN_UPSTASH_REDIS_REST_TOKEN"),
            }),
            new Redis({
                url: ensureEnv("US1_UPSTASH_REDIS_REST_URL"),
                token: ensureEnv("US1_UPSTASH_REDIS_REST_TOKEN"),
            }),
        ],
        limiter,
    });
}

describe("fixedWindow", () => {
    describe("region", () => run(newRegion(RegionRatelimit.fixedWindow(limit, windowString))));

    describe("multiRegion", () =>
        run(newMultiRegion(MultiRegionRatelimit.fixedWindow(limit, windowString))));
});
describe("slidingWindow", () => {
    describe("region", () => run(newRegion(RegionRatelimit.slidingWindow(limit, windowString))));
    describe("multiRegion", () =>
        run(newMultiRegion(MultiRegionRatelimit.slidingWindow(limit, windowString))));
});

describe("tokenBucket", () => {
    describe("region", () =>
        run(newRegion(RegionRatelimit.tokenBucket(refillRate, windowString, limit))));
});

describe("cachedFixedWindow", () => {
    describe("region", () => run(newRegion(RegionRatelimit.cachedFixedWindow(limit, windowString))));
});
