import { Algorithm } from ".";
import type { Duration } from "./duration";
import { MultiRegionRatelimit } from "./multi";
import { Ratelimit } from "./ratelimit";
import { RegionRatelimit } from "./single";
import { TestHarness } from "./test_utils";
import type { Context, MultiRegionContext, RegionContext } from "./types";
import { describe, expect, jest, test } from "@jest/globals";
import { Redis } from "@upstash/redis";
// hack to make printing work in jest
import { log } from "console";
import crypto from "node:crypto";

jest.useRealTimers();
type TestCase = {
  // allowed per second
  rate: number;
  /**
   * Multiplier for rate
   *
   * rate = 10, load = 0.5 -> attack rate will be 5
   */
  load: number;
};
const attackDuration = 60;
const window = 5;
const windowString: Duration = `${window} s`;

const testcases: TestCase[] = [];

for (const rate of [10, 100]) {
  for (const load of [0.5, 1.0, 1.5]) {
    testcases.push({ load, rate });
  }
}

function run<TContext extends Context>(builder: (tc: TestCase) => Ratelimit<TContext>) {
  for (const tc of testcases) {
    const name = `${tc.rate.toString().padStart(4, " ")}/s - Load: ${(tc.load * 100)
      .toString()
      .padStart(3, " ")}% -> Sending ${(tc.rate * tc.load).toString().padStart(4, " ")}req/s`;
    const ratelimit = builder(tc);
    const isMultiRegion = ratelimit instanceof MultiRegionRatelimit;
    const tolerance = isMultiRegion ? 0.5 : 0.1;
    describe(name, () => {
      test(name, async () => {
        log(name);
        const harness = new TestHarness(ratelimit);
        await harness.attack(tc.rate * tc.load, attackDuration).catch((e) => {
          console.error(e);
        });

        expect(harness.metrics.success).toBeLessThanOrEqual(((attackDuration * tc.rate) / window) * (1 + tolerance));
        expect(harness.metrics.success).toBeGreaterThanOrEqual(((attackDuration * tc.rate) / window) * (1 - tolerance));
      });
    });
  }
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
        token: ensureEnv("EU2_UPSTASH_REDIS_REST_TOKEN")!,
      }),
      new Redis({
        url: ensureEnv("APN_UPSTASH_REDIS_REST_URL")!,
        token: ensureEnv("APN_UPSTASH_REDIS_REST_TOKEN")!,
      }),
      new Redis({
        url: ensureEnv("US1_UPSTASH_REDIS_REST_URL")!,
        token: ensureEnv("US1_UPSTASH_REDIS_REST_TOKEN")!,
      }),
    ],
    limiter,
  });
}

function newRegion(limiter: Algorithm<RegionContext>): Ratelimit<RegionContext> {
  return new RegionRatelimit({
    prefix: crypto.randomUUID(),
    redis: Redis.fromEnv(),
    limiter,
  });
}

describe("timeout", () => {
  test("pass after timeout", async () => {
    const r = new RegionRatelimit({
      prefix: crypto.randomUUID(),
      // @ts-ignore - I just want to test the timeout
      redis: {
        ...Redis.fromEnv(),
        eval: () => new Promise((r) => setTimeout(r, 2000)),
      },
      limiter: RegionRatelimit.fixedWindow(1, "1 s"),
      timeout: 1000,
    });
    const start = Date.now();
    const res = await r.limit("id");
    const duration = Date.now() - start;
    expect(res.success).toBe(true);
    expect(res.limit).toBe(0);
    expect(res.remaining).toBe(0);
    expect(res.reset).toBe(0);
    expect(duration).toBeCloseTo(1000, 100);

    // stop the test from leaking
    await new Promise((r) => setTimeout(r, 5000));
  });
});

describe("fixedWindow", () => {
  describe("region", () => run((tc) => newRegion(RegionRatelimit.fixedWindow(tc.rate, windowString))));

  describe("multiRegion", () => run((tc) => newMultiRegion(MultiRegionRatelimit.fixedWindow(tc.rate, windowString))));
});
describe("slidingWindow", () => {
  describe("region", () => run((tc) => newRegion(RegionRatelimit.slidingWindow(tc.rate, windowString))));
  describe("multiRegion", () => run((tc) => newMultiRegion(MultiRegionRatelimit.slidingWindow(tc.rate, windowString))));
});

describe("tokenBucket", () => {
  describe("region", () => run((tc) => newRegion(RegionRatelimit.tokenBucket(tc.rate, windowString, tc.rate))));
});
