import {
  combineParsers,
  GetParserType,
  parseAddress,
  parseFelt252,
  parseI129,
  parseU128,
  parseU64,
} from "../parse";
import { parseDelta } from "./core";

export const parseOrderKey = combineParsers({
  sell_token: { index: 0, parser: parseAddress },
  buy_token: { index: 1, parser: parseAddress },
  fee: { index: 2, parser: parseU128 },
  start_time: { index: 3, parser: parseU64 },
  end_time: { index: 4, parser: parseU64 },
});
export type OrderKey = GetParserType<typeof parseOrderKey>;

export const parseOrderUpdated = combineParsers({
  owner: { index: 0, parser: parseAddress },
  salt: { index: 1, parser: parseFelt252 },
  order_key: { index: 2, parser: parseOrderKey },
  sale_rate_delta: { index: 3, parser: parseI129 },
});
export type OrderUpdatedEvent = GetParserType<typeof parseOrderUpdated>;

export const parseOrderProceedsWithdrawn = combineParsers({
  owner: { index: 0, parser: parseAddress },
  salt: { index: 1, parser: parseFelt252 },
  order_key: { index: 2, parser: parseOrderKey },
  amount: { index: 3, parser: parseU128 },
});
export type OrderProceedsWithdrawnEvent = GetParserType<typeof parseOrderProceedsWithdrawn>;

export const parseStateKey = combineParsers({
  token0: { index: 0, parser: parseAddress },
  token1: { index: 1, parser: parseAddress },
  fee: { index: 2, parser: parseU128 },
});
export type StateKey = GetParserType<typeof parseStateKey>;

export const parseVirtualOrdersExecuted = combineParsers({
  key: { index: 0, parser: parseStateKey },
  token0_sale_rate: { index: 1, parser: parseU128 },
  token1_sale_rate: { index: 2, parser: parseU128 },
  twamm_delta: { index: 3, parser: parseDelta },
});
export type VirtualOrdersExecutedEvent = GetParserType<typeof parseVirtualOrdersExecuted>;