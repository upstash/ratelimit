import { Ratelimit } from "../../../src";
import kv from "@vercel/kv";
import { Inter } from "next/font/google";
import { headers } from "next/headers";
import Image from "next/image";
import Link from "next/link";

const ratelimit = new Ratelimit({
  redis: kv,
  limiter: Ratelimit.fixedWindow(10, "30s"),
});

export default async function Home() {
  const ip = headers().get("x-forwarded-for");
  const { success, limit, reset } = await ratelimit.limit(ip ?? "anonymous011");

  const remaining = await ratelimit.getRemaining(ip ?? "anonymous011");

  return (
    <main className="flex flex-col items-center justify-between min-h-screen p-24">
      <div className="z-10 items-center justify-between w-full max-w-5xl font-mono text-sm lg:flex">
        <p className="fixed top-0 left-0 flex justify-center w-full pt-8 pb-6 border-b border-gray-300 bg-gradient-to-b from-zinc-200 backdrop-blur-2xl dark:border-neutral-800 dark:bg-zinc-800/30 dark:from-inherit lg:static lg:w-auto lg:rounded-xl lg:border lg:bg-gray-200 lg:p-4 lg:dark:bg-zinc-800/30">
          Check out the source at&nbsp;
          <Link
            href="https://github.com/upstash/ratelimit/tree/main/examples/with-vercel-kv"
            className="font-mono font-bold"
          >
            github.com/upstash/ratelimit
          </Link>
        </p>
      </div>

      <div className="relative text-4xl lg:text-7xl font-semibold text-center flex place-items-center before:absolute before:h-[300px] before:w-[480px] before:-translate-x-1/2 before:rounded-full  before:content-[''] after:absolute after:-z-20 after:h-[180px] after:w-[240px] after:translate-x-1/3 ">
        {success ? (
          <>
            @upstash/ratelimit
            <br />+
            <br />
            Vercel KV
          </>
        ) : (
          <>
            You have reached the limit,
            <br />
            please come back later
          </>
        )}
      </div>

      <div className="grid mb-32 text-center lg:mb-0 lg:grid-cols-4 lg:text-left">
        <div className="px-5 py-4 transition-colors border border-transparent rounded-lg group hover:border-gray-300 hover:bg-gray-100 hover:dark:border-neutral-700 hover:dark:bg-neutral-800/30">
          <h2 className={"mb-3 text-2xl font-semibold"}>Success</h2>
          <p className={"m-0 max-w-[30ch] text-sm opacity-50"}>
            {success.toString()}
          </p>
        </div>

        <div className="px-5 py-4 transition-colors border border-transparent rounded-lg group hover:border-gray-300 hover:bg-gray-100 hover:dark:border-neutral-700 hover:dark:bg-neutral-800 hover:dark:bg-opacity-30">
          <h2 className={"mb-3 text-2xl font-semibold"}>Limit </h2>
          <p className={"m-0 max-w-[30ch] text-sm"}>{limit}</p>
        </div>

        <div className="px-5 py-4 transition-colors border border-transparent rounded-lg group hover:border-gray-300 hover:bg-gray-100 hover:dark:border-neutral-700 hover:dark:bg-neutral-800/30">
          <h2 className={"mb-3 text-2xl font-semibold"}>Remaining </h2>
          <p className={"m-0 max-w-[30ch] text-sm opacity-50"}>{remaining}</p>
        </div>

        <div className="px-5 py-4 transition-colors border border-transparent rounded-lg group hover:border-gray-300 hover:bg-gray-100 hover:dark:border-neutral-700 hover:dark:bg-neutral-800/30">
          <h2 className={"mb-3 text-2xl font-semibold"}>Reset</h2>
          <p className={"m-0 max-w-[30ch] text-sm opacity-50"}>
            {new Date(reset).toUTCString()}
          </p>
        </div>
      </div>
    </main>
  );
}
