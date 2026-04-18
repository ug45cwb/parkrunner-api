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
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

const MAX_REDIRECTS = 5;

function normalizeResultsUrl(url: string): string {
  const t = url.trim();
  return t.endsWith("/") ? t : `${t}/`;
}

/**
 * Plain HTTPS GET (no global fetch): avoids Undici/fetch quirks in Lambda that
 * can surface as HTTP 405 against some front-ends.
 */
async function fetchResultsHtml(
  startUrl: string,
): Promise<{ finalUrl: string; html: string }> {
  const ua = process.env.HTTP_USER_AGENT?.trim() || DEFAULT_UA;
  let current = normalizeResultsUrl(startUrl);

  for (let hop = 0; hop <= MAX_REDIRECTS; hop++) {
    const u = new URL(current);
    const referer = `${u.origin}/`;

    const { statusCode, headers, body } = await new Promise<{
      statusCode: number;
      headers: IncomingHttpHeaders;
      body: Buffer;
    }>((resolve, reject) => {
      const req = https.request(
        {
          hostname: u.hostname,
          port: u.port || 443,
          path: `${u.pathname}${u.search}`,
          method: "GET",
          headers: {
            Host: u.hostname,
            "User-Agent": ua,
            Accept:
              "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
            "Accept-Encoding": "identity",
            Referer: referer,
            Connection: "close",
            "Upgrade-Insecure-Requests": "1",
          },
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

    const location = headers.location;
    const loc =
      typeof location === "string"
        ? location
        : Array.isArray(location)
          ? location[0]
          : undefined;
    if (statusCode >= 300 && statusCode < 400 && loc) {
      current = new URL(loc, current).href;
      continue;
    }

    if (statusCode !== 200) {
      throw new Error(
        `Failed to fetch results page: HTTP ${statusCode} (url: ${current})`,
      );
    }

    return { finalUrl: current, html: body.toString("utf8") };
  }

  throw new Error("Too many redirects when fetching results page");
}

type ScrapeEvent = { url?: string };

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

  const url =
    (rawEvent && typeof rawEvent === "object" && typeof rawEvent.url === "string"
      ? rawEvent.url
      : undefined) ?? process.env.RESULTS_PAGE_URL;

  if (!url?.trim()) {
    throw new Error(
      "Set RESULTS_PAGE_URL on the function or pass { \"url\": \"…\" } in the invocation payload",
    );
  }

  const normalizedStart = normalizeResultsUrl(url.trim());
  const { eventSlug, date } = parseResultsUrl(normalizedStart);
  const scrapedAt = new Date().toISOString();
  const pk = `PARKRUN_EVENT#${eventSlug}#${date}`;

  const { finalUrl, html } = await fetchResultsHtml(normalizedStart);
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
  };
}
