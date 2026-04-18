import { DynamoDBClient, QueryCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";

/** Fields this handler reads from API Gateway proxy integration (no `aws-lambda` package). */
type LeaderboardEvent = {
  pathParameters?: Record<string, string> | null;
};

type LeaderboardResponse = {
  statusCode: number;
  headers: Record<string, string>;
  body: string;
};

const client = new DynamoDBClient({});

const PERIOD_MAP: Record<string, string> = {
  "this-week": "THIS_WEEK",
  "this-year": "THIS_YEAR",
  "all-time": "ALL_TIME",
};

const jsonHeaders = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*",
};

type Row = { rank: number; name: string; time: string; event: string };

async function queryBoard(tableName: string, periodKey: string): Promise<Row[]> {
  const pk = `LEADERBOARD#${periodKey}`;
  const res = await client.send(
    new QueryCommand({
      TableName: tableName,
      KeyConditionExpression: "pk = :pk",
      ExpressionAttributeValues: {
        ":pk": { S: pk },
      },
    }),
  );

  const rows = (res.Items ?? []).map((item) => {
    const m = unmarshall(item) as {
      rank: number;
      runnerName: string;
      eventName: string;
      time: string;
    };
    return {
      rank: m.rank,
      name: m.runnerName,
      time: m.time,
      event: m.eventName,
    };
  });
  rows.sort((a, b) => a.rank - b.rank);
  return rows;
}

export async function handler(
  event: LeaderboardEvent,
): Promise<LeaderboardResponse> {
  const tableName = process.env.TABLE_NAME;
  if (!tableName) {
    return {
      statusCode: 500,
      headers: jsonHeaders,
      body: JSON.stringify({ message: "TABLE_NAME is not set" }),
    };
  }

  try {
    const periodParam = event.pathParameters?.period;

    if (periodParam) {
      const periodKey = PERIOD_MAP[periodParam];
      if (!periodKey) {
        return {
          statusCode: 400,
          headers: jsonHeaders,
          body: JSON.stringify({
            message: "Invalid period",
            allowed: Object.keys(PERIOD_MAP),
          }),
        };
      }
      const rows = await queryBoard(tableName, periodKey);
      return {
        statusCode: 200,
        headers: jsonHeaders,
        body: JSON.stringify({ period: periodParam, rows }),
      };
    }

    const out: Record<string, Row[]> = {};
    for (const [urlKey, dynamoPeriod] of Object.entries(PERIOD_MAP)) {
      out[urlKey] = await queryBoard(tableName, dynamoPeriod);
    }
    return {
      statusCode: 200,
      headers: jsonHeaders,
      body: JSON.stringify(out),
    };
  } catch (err) {
    console.error(err);
    return {
      statusCode: 500,
      headers: jsonHeaders,
      body: JSON.stringify({
        message: "Failed to read leaderboards",
        error: err instanceof Error ? err.message : String(err),
      }),
    };
  }
}
