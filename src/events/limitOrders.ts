import {
  combineParsers,
  parseAddress,
  parseFelt252,
  parseI129,
  parseU128,
} from "../parse";
import type { GetParserType } from "../parse";

export const parseLimitOrderKey = combineParsers({
  token0: { index: 0, parser: parseAddress },
  token1: { index: 1, parser: parseAddress },
  tick: { index: 2, parser: parseI129 },
});
export type LimitOrderKey = GetParserType<typeof parseLimitOrderKey>;

export const parseOrderPlaced = combineParsers({
  owner: { index: 0, parser: parseAddress },
  salt: { index: 1, parser: parseFelt252 },
  order_key: { index: 2, parser: parseLimitOrderKey },
  liquidity: { index: 3, parser: parseU128 },
  amount: { index: 4, parser: parseU128 },
});
export type OrderPlacedEvent = GetParserType<typeof parseOrderPlaced>;

export const parseOrderClosed = combineParsers({
  owner: { index: 0, parser: parseAddress },
  salt: { index: 1, parser: parseFelt252 },
  order_key: { index: 2, parser: parseLimitOrderKey },
  amount0: { index: 3, parser: parseU128 },
  amount1: { index: 4, parser: parseU128 },
});
export type OrderClosedEvent = GetParserType<typeof parseOrderClosed>;
