/**
 * Fetches a parkrun event results HTML page, parses the finishers table, and
 * writes one DynamoDB item per finisher. Respect parkrun's terms of use; this
 * is intended for personal/authorised aggregation only.
 */
import { BatchWriteItemCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import type { IncomingHttpHeaders } from "node:http";
import * as http2 from "node:http2";
import * as https from "node:https";
import { URL } from "node:url";
import { load } from "cheerio";

/** Match a normal desktop Chrome request; some CDNs return 405/403 to bare or “bot” agents. */
const DEFAULT_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

const MAX_REDIRECTS = 5;

function normalizeResultsUrl(url: string): string {
  const t = url.trim();
  return t.endsWith("/") ? t : `${t}/`;
}

function commonBrowserHeaders(ua: string, referer?: string): Record<string, string> {
  const h: Record<string, string> = {
    "User-Agent": ua,
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "identity",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": referer ? "same-origin" : "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
  };
  if (referer) {
    h.Referer = referer;
  }
  return h;
}

async function httpsGetOnce(
  urlStr: string,
  referer: string | undefined,
): Promise<{ statusCode: number; headers: IncomingHttpHeaders; body: Buffer }> {
  const ua = process.env.HTTP_USER_AGENT?.trim() || DEFAULT_UA;
  const u = new URL(urlStr);
  const headers = commonBrowserHeaders(ua, referer);
  // Do not set `Host` — Node sets it from `hostname`. A duplicate Host breaks some edges (HTTP 405).

  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: u.hostname,
        port: u.port || 443,
        path: `${u.pathname}${u.search}`,
        method: "GET",
        headers,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () => {
          resolve({
            statusCode: res.statusCode ?? 0,
            headers: res.headers,
            body: Buffer.concat(chunks),
          });
        });
      },
    );
    req.on("error", reject);
    req.end();
  });
}

async function http2GetOnce(
  urlStr: string,
  referer: string | undefined,
): Promise<{ statusCode: number; headers: IncomingHttpHeaders; body: Buffer }> {
  const ua = process.env.HTTP_USER_AGENT?.trim() || DEFAULT_UA;
  const u = new URL(urlStr);
  const h = commonBrowserHeaders(ua, referer);

  const session = http2.connect(`https://${u.hostname}`);
  return new Promise((resolve, reject) => {
    session.on("error", reject);
    const reqHeaders: http2.OutgoingHttpHeaders = {
      ":method": "GET",
      ":path": `${u.pathname}${u.search}`,
      ":scheme": "https",
      ":authority": u.host,
      ...h,
    };
    const req = session.request(reqHeaders);
    const chunks: Buffer[] = [];
    req.on("response", (headers) => {
      const status = Number(headers[":status"] ?? 0);
      req.on("data", (c: Buffer) => chunks.push(c));
      req.on("end", () => {
        session.close();
        resolve({
          statusCode: status,
          headers: headers as IncomingHttpHeaders,
          body: Buffer.concat(chunks),
        });
      });
    });
    req.on("error", (e) => {
      session.destroy();
      reject(e);
    });
    req.end();
  });
}

type FetchMode = "https" | "http2";

async function fetchOne(
  urlStr: string,
  referer: string | undefined,
  mode: FetchMode,
): Promise<{ statusCode: number; headers: IncomingHttpHeaders; body: Buffer }> {
  return mode === "http2"
    ? http2GetOnce(urlStr, referer)
    : httpsGetOnce(urlStr, referer);
}

/**
 * GET the results HTML with redirect handling. Tries HTTP/1.1 then HTTP/2 and
 * omits duplicate Host (which can produce HTTP 405 on some CDNs).
 */
async function fetchResultsHtml(
  startUrl: string,
): Promise<{ finalUrl: string; html: string }> {
  let current = normalizeResultsUrl(startUrl);
  let lastReferer: string | undefined;

  for (let hop = 0; hop <= MAX_REDIRECTS; hop++) {
    let statusCode = 0;
    let headers: IncomingHttpHeaders = {};
    let body = Buffer.alloc(0);

    let lastErr: Error | undefined;
    let gotUsableResponse = false;

    for (const mode of ["https", "http2"] as const) {
      try {
        const r = await fetchOne(current, lastReferer, mode);
        statusCode = r.statusCode;
        headers = r.headers;
        body = Buffer.from(r.body);
        lastErr = undefined;

        const location = headers.location;
        const loc =
          typeof location === "string"
            ? location
            : Array.isArray(location)
              ? location[0]
              : undefined;
        const isRedirect =
          statusCode >= 300 && statusCode < 400 && Boolean(loc);

        if (statusCode === 200 || isRedirect) {
          gotUsableResponse = true;
          break;
        }

        lastErr = new Error(
          `HTTP ${statusCode} from ${mode} (url: ${current})`,
        );
      } catch (e) {
        lastErr = e instanceof Error ? e : new Error(String(e));
      }
    }

    if (!gotUsableResponse && lastErr) {
      throw lastErr;
    }

    const location = headers.location;
    const loc =
      typeof location === "string"
        ? location
        : Array.isArray(location)
          ? location[0]
          : undefined;
    if (statusCode >= 300 && statusCode < 400 && loc) {
      lastReferer = current;
      current = new URL(loc, current).href;
      continue;
    }

    if (statusCode !== 200) {
      throw new Error(
        `Failed to fetch results page: HTTP ${statusCode} (url: ${current}). ` +
          `If this persists from Lambda only, parkrun may be blocking datacenter IPs; ` +
          `invoke with { "htmlBase64": "<saved page as base64>" } to skip HTTP fetch.`,
      );
    }

    return { finalUrl: current, html: body.toString("utf8") };
  }

  throw new Error("Too many redirects when fetching results page");
}

type ScrapeEvent = {
  /** When set, skip HTTP and parse this HTML (base64). Use if parkrun blocks Lambda IPs. */
  htmlBase64?: string;
  url?: string;
};

type Finisher = {
  position: number;
  runnerName: string;
  time: string;
  ageGroup: string;
  gender: string;
  club: string;
};

function parseResultsUrl(url: string): { eventSlug: string; date: string } {
  const m = url.match(
    /parkrun\.org\.uk\/([^/]+)\/results\/(\d{4}-\d{2}-\d{2})\/?/i,
  );
  if (!m) {
    throw new Error(
      `Could not parse event slug and date from URL (expected …/EVENT/results/YYYY-MM-DD/): ${url}`,
    );
  }
  return { eventSlug: m[1].toLowerCase(), date: m[2] };
}

function parseFinishers(html: string): Finisher[] {
  const $ = load(html);
  const out: Finisher[] = [];

  $("table.js-ResultsTable tbody tr.Results-table-row").each((_, el) => {
    const tr = $(el);
    const position = Number.parseInt(tr.attr("data-position") ?? "", 10);
    const runnerName = (tr.attr("data-name") ?? "").trim();
    const time = tr
      .find("td.Results-table-td--time .compact")
      .first()
      .text()
      .trim();

    if (!Number.isFinite(position) || position < 1 || !runnerName || !time) {
      return;
    }

    out.push({
      position,
      runnerName,
      time,
      ageGroup: (tr.attr("data-agegroup") ?? "").trim(),
      gender: (tr.attr("data-gender") ?? "").trim(),
      club: (tr.attr("data-club") ?? "").trim(),
    });
  });

  return out.sort((a, b) => a.position - b.position);
}

function chunk<T>(arr: T[], size: number): T[][] {
  const res: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    res.push(arr.slice(i, i + size));
  }
  return res;
}

export async function handler(rawEvent: ScrapeEvent | Record<string, unknown>) {
  const tableName = process.env.TABLE_NAME;
  if (!tableName) {
    throw new Error("TABLE_NAME environment variable is required");
  }

  const payload =
    rawEvent && typeof rawEvent === "object" ? (rawEvent as ScrapeEvent) : {};

  const htmlFromPayload =
    typeof payload.htmlBase64 === "string" &&
    payload.htmlBase64.trim().length > 0
      ? Buffer.from(payload.htmlBase64, "base64").toString("utf8")
      : undefined;

  const url =
    (typeof payload.url === "string" ? payload.url : undefined) ??
    process.env.RESULTS_PAGE_URL;

  if (!url?.trim() && !htmlFromPayload) {
    throw new Error(
      "Set RESULTS_PAGE_URL on the function or pass { \"url\": \"…\" } (and optionally { \"htmlBase64\": \"…\" }) in the invocation payload",
    );
  }

  if (htmlFromPayload && !url?.trim()) {
    throw new Error(
      "When passing htmlBase64 you must also provide url (in the payload or RESULTS_PAGE_URL) so event slug and date can be parsed for DynamoDB keys.",
    );
  }

  const normalizedStart = normalizeResultsUrl(url!.trim());
  const { eventSlug, date } = parseResultsUrl(normalizedStart);
  const scrapedAt = new Date().toISOString();
  const pk = `PARKRUN_EVENT#${eventSlug}#${date}`;

  let finalUrl: string;
  let html: string;
  if (htmlFromPayload) {
    finalUrl = normalizedStart;
    html = htmlFromPayload;
  } else {
    const fetched = await fetchResultsHtml(normalizedStart);
    finalUrl = fetched.finalUrl;
    html = fetched.html;
  }

  const finishers = parseFinishers(html);

  if (finishers.length === 0) {
    throw new Error(
      "No finisher rows found (selector table.js-ResultsTable tbody tr.Results-table-row). The page layout may have changed.",
    );
  }

  const client = new DynamoDBClient({});

  const items = finishers.map((f) =>
    marshall({
      pk,
      sk: `FINISHER#${String(f.position).padStart(5, "0")}`,
      entityType: "PARKRUN_EVENT_RESULT",
      eventSlug,
      eventDate: date,
      position: f.position,
      runnerName: f.runnerName,
      time: f.time,
      ageGroup: f.ageGroup,
      gender: f.gender,
      club: f.club,
      sourceUrl: finalUrl,
      scrapedAt,
    }),
  );

  let written = 0;
  for (const batch of chunk(items, 25)) {
    const resp = await client.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [tableName]: batch.map((Item) => ({ PutRequest: { Item } })),
        },
      }),
    );
    const unprocessed = resp.UnprocessedItems?.[tableName]?.length ?? 0;
    if (unprocessed > 0) {
      throw new Error(
        `DynamoDB left ${unprocessed} unprocessed writes (throttle). Retry the Lambda.`,
      );
    }
    written += batch.length;
  }

  return {
    ok: true,
    pk,
    finishersWritten: written,
    sourceUrl: finalUrl,
    usedHtmlPayload: Boolean(htmlFromPayload),
  };
}
