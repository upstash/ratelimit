import type { NextApiRequest, NextApiResponse } from "next";

export default function handler(
  _req: NextApiRequest,
  res: NextApiResponse,
) {
  return res.json({ hello: "world" });
}
