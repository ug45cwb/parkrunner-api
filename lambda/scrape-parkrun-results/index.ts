/**
 * Fetches a parkrun event results HTML page, parses the finishers table, and
 * writes one DynamoDB item per finisher. Respect parkrun's terms of use; this
 * is intended for personal/authorised aggregation only.
 */
import { BatchWriteItemCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import { load } from "cheerio";

const DEFAULT_UA =
  "Mozilla/5.0 (compatible; parkrun-hub/1.0; +https://github.com/parkrun-hub) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

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

  const { eventSlug, date } = parseResultsUrl(url.trim());
  const scrapedAt = new Date().toISOString();
  const pk = `PARKRUN_EVENT#${eventSlug}#${date}`;

  const res = await fetch(url.trim(), {
    headers: {
      "User-Agent": process.env.HTTP_USER_AGENT?.trim() || DEFAULT_UA,
      Accept: "text/html,application/xhtml+xml",
      "Accept-Language": "en-GB,en;q=0.9",
    },
  });

  if (!res.ok) {
    throw new Error(`Failed to fetch results page: HTTP ${res.status}`);
  }

  const html = await res.text();
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
      sourceUrl: url.trim(),
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
    sourceUrl: url.trim(),
  };
}
