/**
 * Sample leaderboard rows for DynamoDB. Aligns with the UI sample in parkrun-hub-ui
 * (`LEADERBOARD#…` + `RANK#…`). Bump SEED_REVISION after changing rows so deploy re-seeds.
 */
export const SEED_REVISION = 1;

const THIS_WEEK = [
  { rank: 1, name: "Sam Okoro", time: "14:52", event: "Bushy Park" },
  { rank: 2, name: "Elena Vasquez", time: "15:01", event: "Parque de Valdebebas" },
  { rank: 3, name: "Jonas Lind", time: "15:08", event: "Tøyen" },
  { rank: 4, name: "Priya Nair", time: "15:14", event: "Cannon Hill" },
  { rank: 5, name: "Marcus Webb", time: "15:19", event: "Albert Park" },
  { rank: 6, name: "Hannah Cole", time: "15:22", event: "Edinburgh" },
  { rank: 7, name: "Diego Ferreira", time: "15:26", event: "Ibirapuera" },
  { rank: 8, name: "Amelia Grant", time: "15:31", event: "North Beach" },
  { rank: 9, name: "Chen Wei", time: "15:33", event: "Centennial Park" },
  { rank: 10, name: "Noah Brooks", time: "15:37", event: "Crissy Field" },
] as const;

const THIS_YEAR = [
  { rank: 1, name: "Sam Okoro", time: "14:41", event: "Bushy Park" },
  { rank: 2, name: "Yuki Tanaka", time: "14:55", event: "Odaiba" },
  { rank: 3, name: "Elena Vasquez", time: "14:58", event: "Valencia" },
  { rank: 4, name: "Tom Brennan", time: "15:03", event: "St Anne's" },
  { rank: 5, name: "Jonas Lind", time: "15:05", event: "Tøyen" },
  { rank: 6, name: "Zara Ahmed", time: "15:11", event: "Roundhay" },
  { rank: 7, name: "Priya Nair", time: "15:12", event: "Cannon Hill" },
  { rank: 8, name: "Luca Romano", time: "15:18", event: "Villa Borghese" },
  { rank: 9, name: "Marcus Webb", time: "15:19", event: "Albert Park" },
  { rank: 10, name: "Hannah Cole", time: "15:21", event: "Holyrood" },
] as const;

const ALL_TIME = [
  { rank: 1, name: "Alex Mercer", time: "14:12", event: "Bushy Park" },
  { rank: 2, name: "Riley Stone", time: "14:28", event: "Congleton" },
  { rank: 3, name: "Jordan Blake", time: "14:35", event: "Cardiff" },
  { rank: 4, name: "Casey Frost", time: "14:39", event: "Cardiff" },
  { rank: 5, name: "Sam Okoro", time: "14:41", event: "Bushy Park" },
  { rank: 6, name: "Morgan Dale", time: "14:44", event: "Congleton" },
  { rank: 7, name: "Yuki Tanaka", time: "14:55", event: "Odaiba" },
  { rank: 8, name: "Elena Vasquez", time: "14:58", event: "Valencia" },
  { rank: 9, name: "Tom Brennan", time: "15:03", event: "St Anne's" },
  { rank: 10, name: "Jonas Lind", time: "15:05", event: "Tøyen" },
] as const;

export type SeedLeaderboardPeriod = "THIS_WEEK" | "THIS_YEAR" | "ALL_TIME";

export type SeedRow = {
  pk: string;
  sk: string;
  period: SeedLeaderboardPeriod;
  rank: number;
  runnerName: string;
  eventName: string;
  time: string;
};

function buildRows(
  period: SeedLeaderboardPeriod,
  rows: readonly { rank: number; name: string; time: string; event: string }[],
): SeedRow[] {
  return rows.map((r) => ({
    pk: `LEADERBOARD#${period}`,
    sk: `RANK#${String(r.rank).padStart(2, "0")}`,
    period,
    rank: r.rank,
    runnerName: r.name,
    eventName: r.event,
    time: r.time,
  }));
}

export const SEED_ROWS: SeedRow[] = [
  ...buildRows("THIS_WEEK", THIS_WEEK),
  ...buildRows("THIS_YEAR", THIS_YEAR),
  ...buildRows("ALL_TIME", ALL_TIME),
];
