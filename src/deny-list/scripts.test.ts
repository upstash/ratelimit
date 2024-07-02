import { Redis } from "@upstash/redis";
import { beforeEach, describe, expect, test } from "bun:test";
import { DenyListExtension, IpDenyListStatusKey, IsDenied } from "../types";
import { checkDenyListScript } from "./scripts";
import { disableIpDenyList, updateIpDenyList } from "./ip-deny-list";

describe("should manage state correctly", async () => {
  const redis = Redis.fromEnv();
  const prefix = `test-script-prefix`;

  const allDenyListsKey = [prefix, DenyListExtension, "all"].join(":");
  const ipDenyListStatusKey = [prefix, IpDenyListStatusKey].join(":");

  beforeEach(async () => {
    await redis.flushdb()
    await redis.sadd(
      allDenyListsKey, "foo", "bar")
  });

  test("should return status: -2 initially", async () => {
    const [isMember, status] = await redis.eval(
      checkDenyListScript,
      [allDenyListsKey, ipDenyListStatusKey],
      ["whale", "foo", "bar", "zed"]
    ) as [IsDenied[], number];

    expect(isMember).toEqual([0, 1, 1, 0])
    expect(status).toBe(-2)
  })

  test("should return status: -1 when disabled", async () => {
    await disableIpDenyList(redis, prefix);
    const [isMember, status] = await redis.eval(
      checkDenyListScript,
      [allDenyListsKey, ipDenyListStatusKey],
      ["whale", "foo", "bar", "zed"]
    ) as [IsDenied[], number];

    expect(isMember).toEqual([0, 1, 1, 0])
    expect(status).toBe(-1)
  })

  test("should return status: number after update", async () => {
    await updateIpDenyList(redis, prefix, 8);
    const [isMember, status] = await redis.eval(
      checkDenyListScript,
      [allDenyListsKey, ipDenyListStatusKey],
      ["foo", "whale", "bar", "zed"]
    ) as [IsDenied[], number];

    expect(isMember).toEqual([1, 0, 1, 0])
    expect(status).toBeGreaterThan(1000)
  })

  test("should return status: -1 after update and disable", async () => {
    await updateIpDenyList(redis, prefix, 8);
    await disableIpDenyList(redis, prefix);
    const [isMember, status] = await redis.eval(
      checkDenyListScript,
      [allDenyListsKey, ipDenyListStatusKey],
      ["foo", "whale", "bar", "zed"]
    ) as [IsDenied[], number];

    expect(isMember).toEqual([1, 0, 1, 0])
    expect(status).toBe(-1)
  })

  test("should only make one of two consecutive requests update deny list", async () => {

    // running the eval script consecutively when the deny list needs
    // to be updated. Only one will update the ip list. It will be
    // given 30 seconds before its turn expires. Until then, other requests
    // will continue using the old ip deny list
    const response = await Promise.all([
      redis.eval(
        checkDenyListScript,
        [allDenyListsKey, ipDenyListStatusKey],
        ["foo", "whale", "bar", "zed"]
      ) as Promise<[IsDenied[], number]>,
      redis.eval(
        checkDenyListScript,
        [allDenyListsKey, ipDenyListStatusKey],
        ["foo", "whale", "bar", "zed"]
      ) as Promise<[IsDenied[], number]>
    ]);

    // first request is told that there is no valid ip list (ttl: -2),
    // hence it will update the ip deny list
    expect(response[0]).toEqual([[1, 0, 1, 0], -2])

    // second request is told that there is already a valid ip list
    // with ttl 30.
    expect(response[1]).toEqual([[1, 0, 1, 0], 30])

    const state = await redis.get(ipDenyListStatusKey)
    expect(state).toBe("pending")
  })
})