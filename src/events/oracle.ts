import { combineParsers, parseAddress, parseI129, parseU64 } from "../parse";
import type { GetParserType } from "../parse";

export const parseSnapshot = combineParsers({
  block_timestamp: { index: 0, parser: parseU64 },
  tick_cumulative: { index: 1, parser: parseI129 },
});
export type Snapshot = GetParserType<typeof parseSnapshot>;

export const parseSnapshotEvent = combineParsers({
  token0: { index: 0, parser: parseAddress },
  token1: { index: 1, parser: parseAddress },
  index: { index: 2, parser: parseU64 },
  snapshot: { index: 3, parser: parseSnapshot },
});
export type SnapshotEvent = GetParserType<typeof parseSnapshotEvent>;
