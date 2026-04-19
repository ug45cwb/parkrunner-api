/**
 * Fetches a parkrun event results HTML page, parses the finishers table, and
 * writes one DynamoDB item per finisher. Respect parkrun's terms of use; this
 * is intended for personal/authorised aggregation only.
 */
import { BatchWriteItemCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import type { IncomingHttpHeaders } from "node:http";
import * as https from "node:https";
import { URL } from "node:url";
import { load } from "cheerio";

/** Match a normal desktop Chrome request; some CDNs return 405/403 to bare or “bot” agents. */
const DEFAULT_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36";

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
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    Priority: "u=0, i",
    "Sec-CH-UA":
      '"Chromium";v="147", "Google Chrome";v="147", "Not_A Brand";v="24"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"',
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

/**
 * GET a page over plain HTTPS GET with redirect handling.
 */
async function fetchHtmlWithRedirects(
  startUrl: string,
  referer?: string,
): Promise<{ finalUrl: string; html: string }> {
  let current = normalizeResultsUrl(startUrl);
  let lastReferer = referer;

  for (let hop = 0; hop <= MAX_REDIRECTS; hop++) {
    const r = await httpsGetOnce(current, lastReferer);
    const statusCode = r.statusCode;
    const headers = r.headers;
    const body = Buffer.from(r.body);

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
        `Failed to fetch page: HTTP ${statusCode} (url: ${current}). ` +
          `If this persists from Lambda only, parkrun may be blocking datacenter IPs; ` +
          `invoke with { "htmlBase64": "<saved page as base64>" } to skip HTTP fetch.`,
      );
    }

    return { finalUrl: current, html: body.toString("utf8") };
  }

  throw new Error("Too many redirects when fetching results page");
}

function getHistoryUrlForEvent(eventSlug: string): string {
  return `https://www.parkrun.org.uk/${eventSlug}/results/`;
}

function findDatedResultsUrlFromHistory(
  historyHtml: string,
  historyUrl: string,
  eventSlug: string,
  date: string,
): string | undefined {
  const $ = load(historyHtml);
  const targetPath = `/${eventSlug}/results/${date}/`;

  for (const el of $("a[href]").toArray()) {
    const href = $(el).attr("href");
    if (!href) {
      continue;
    }

    const absolute = normalizeResultsUrl(new URL(href, historyUrl).href);
    const parsed = new URL(absolute);
    if (parsed.pathname.toLowerCase() === targetPath.toLowerCase()) {
      return absolute;
    }
  }

  return undefined;
}

async function fetchResultsHtml(
  startUrl: string,
): Promise<{ finalUrl: string; html: string }> {
  const normalizedStart = normalizeResultsUrl(startUrl);
  const { eventSlug, date } = parseResultsUrl(normalizedStart);
  const historyUrl = getHistoryUrlForEvent(eventSlug);
  const history = await fetchHtmlWithRedirects(historyUrl);
  const resolvedUrl =
    findDatedResultsUrlFromHistory(
      history.html,
      history.finalUrl,
      eventSlug,
      date,
    ) ?? normalizedStart;

  if (resolvedUrl !== normalizedStart) {
    console.log(
      `Resolved dated results URL via history flow: ${normalizedStart} -> ${resolvedUrl}`,
    );
  } else {
    console.log(
      `History flow did not expose ${normalizedStart}; falling back to direct dated URL fetch.`,
    );
  }

  return fetchHtmlWithRedirects(resolvedUrl, history.finalUrl);
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

  console.log("Fetched results HTML before parsing:");
  console.log(html);

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
