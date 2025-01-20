import {
  backwardsCompatibleParserAdditionalArgument,
  combineParsers,
  parseAddress,
  parseU64,
} from "../parse";
import { parseBounds, parsePoolKey } from "./core";
import type { GetParserType } from "../parse";

export const parseLegacyPositionMintedEvent = combineParsers({
  id: { index: 0, parser: parseU64 },
  pool_key: { index: 1, parser: parsePoolKey },
  bounds: { index: 2, parser: parseBounds },
  referrer: {
    index: 3,
    parser: backwardsCompatibleParserAdditionalArgument(parseAddress),
  },
});
export type LegacyPositionMintedEvent = GetParserType<
  typeof parseLegacyPositionMintedEvent
>;

export const parsePositionMintedWithReferrerEvent = combineParsers({
  id: { index: 0, parser: parseU64 },
  referrer: { index: 1, parser: parseAddress },
});
export type PositionMintedWithReferrer = GetParserType<
  typeof parsePositionMintedWithReferrerEvent
>;
