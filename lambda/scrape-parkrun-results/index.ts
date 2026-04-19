import type { IncomingHttpHeaders } from "node:http";
import * as https from "node:https";
import { URL } from "node:url";

const DEFAULT_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36";

function requestHeaders(ua: string, referer?: string): Record<string, string> {
  const r = referer?.trim();
  const cookie = process.env.PARKRUN_COOKIE?.trim();
  const h: Record<string, string> = {
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    Priority: "u=0, i",
    "Sec-CH-UA":
      '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": r ? "same-origin" : "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": ua,
  };
  if (cookie) h.Cookie = cookie;
  if (r) h.Referer = r;
  return h;
}

function get(
  urlStr: string,
  referer?: string,
): Promise<{ status: number; headers: IncomingHttpHeaders; body: Buffer }> {
  const ua = process.env.HTTP_USER_AGENT?.trim() || DEFAULT_UA;
  const u = new URL(urlStr);
  return new Promise((resolve, reject) => {
    https
      .request(
        {
          hostname: u.hostname,
          port: u.port || 443,
          path: `${u.pathname}${u.search}`,
          method: "GET",
          headers: requestHeaders(ua, referer),
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (c: Buffer) => chunks.push(c));
          res.on("end", () =>
            resolve({
              status: res.statusCode ?? 0,
              headers: res.headers,
              body: Buffer.concat(chunks),
            }),
          );
        },
      )
      .on("error", reject)
      .end();
  });
}

type Event = { url?: string };

export async function handler(event: Event) {
  const start =
    (typeof event?.url === "string" && event.url.trim()) ||
    process.env.RESULTS_PAGE_URL?.trim();
  if (!start) throw new Error('Set RESULTS_PAGE_URL or pass { "url": "…" }');

  let url = start;
  let referer: string | undefined;

  for (let hop = 0; hop < 6; hop++) {
    const { status, headers, body } = await get(url, referer);
    const text = body.toString("utf8");

    console.log("--- HTTP response ---");
    console.log("requestUrl:", url);
    console.log("status:", status);
    console.log("headers:", JSON.stringify(headers, null, 2));
    console.log("body:", text);

    const loc = headers.location;
    const next =
      typeof loc === "string" ? loc : Array.isArray(loc) ? loc[0] : undefined;
    if (status >= 300 && status < 400 && next) {
      referer = url;
      url = new URL(next, url).href;
      continue;
    }

    return { status, finalUrl: url, bodyLength: body.length };
  }

  throw new Error("Too many redirects");
}
