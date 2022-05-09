import { Ratelimit } from "./ratelimit.ts";
import { Context } from "./types.ts";

type Metrics = {
  requests: number;
  success: number;
  rejected: number;
};
export class TestHarness<TContext extends Context> {
  /**
   * Used as prefix for redis keys
   */
  public readonly id: string;

  private readonly ratelimit: Ratelimit<TContext>;
  public metrics: Metrics;

  public latencies: Record<string, { start: number; end: number }> = {};

  constructor(ratelimit: Ratelimit<TContext>) {
    this.ratelimit = ratelimit;
    this.id = crypto.randomUUID();
    this.metrics = {
      requests: 0,
      success: 0,
      rejected: 0,
    };
  }

  /**
   * @param rate - req per second
   * @param duration - duration in seconds
   */
  public async attack(rate: number, duration: number): Promise<void> {
    const promises: Promise<{ success: boolean }>[] = [];

    for (let i = 0; i < duration; i++) {
      for (let r = 0; r < rate; r++) {
        this.metrics.requests++;
        const id = crypto.randomUUID();
        this.latencies[id] = { start: Date.now(), end: -1 };
        promises.push(
          this.ratelimit.limit(this.id).then((res) => {
            this.latencies[id].end = Date.now();
            return res;
          }),
        );
      }
      await new Promise((r) => setTimeout(r, 1000));
    }

    await Promise.all(
      promises.map(async (p) => {
        const { success } = await p;
        if (success) {
          this.metrics.success++;
        } else {
          this.metrics.rejected++;
        }
      }),
    );
  }
}
