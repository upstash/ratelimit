import type { NextPage } from "next";
import { useRouter } from "next/router";
import { useEffect, useState } from "react";

const Home: NextPage = () => {
  const [response, setResponse] = useState<Record<string, unknown> | null>(null);

  const router = useRouter();
  useEffect(() => {}, []);

  const generate = async () => {
    const res = await fetch("/api/hello");

    if (res.ok) {
      setResponse({
        status: res.status,
        body: await res.json(),
        headers: {
          "X-RateLimit-Limit": res.headers.get("X-RateLimit-Limit"),
          "X-RateLimit-Remaining": res.headers.get("X-RateLimit-Remaining"),
          "X-RateLimit-Reset": res.headers.get("X-RateLimit-Reset"),
        },
      });
    } else {
      setResponse(null);

      alert("Ratelimit reached, try again later");
    }
  };
  return (
    <>
      <main>
        <header>
          <h1 className="text-4xl font-bold">
            Welcome to <span className="text-primary-500">@upstash/ratelimit</span>
          </h1>

          <p className="mt-4">
            This is an example of how to ratelimit your nextjs app at the edge using Vercel Edge and
            Upstash Redis
          </p>

          <p className="mt-4">
            Click the button below to make a request, that will be ratelimited by your IP.
          </p>
        </header>

        <hr className="my-10" />

        <div className="grid grid-cols-1 gap-6">
          <div className="flex justify-center">
            <button onClick={generate}>Make a request</button>
          </div>

          {response ? <pre>{JSON.stringify(response, null, 2)}</pre> : null}
        </div>
      </main>
    </>
  );
};

export default Home;
