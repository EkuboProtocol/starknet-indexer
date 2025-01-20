import {
  combineParsers,
  parseAddress,
  parseByteArray,
  parseFelt252,
  parseU128,
  parseU8,
} from "../parse";
import type { GetParserType } from "../parse";

export const parseRegistrationEvent = combineParsers({
  address: { index: 0, parser: parseAddress },
  name: { index: 1, parser: parseFelt252 },
  symbol: { index: 2, parser: parseFelt252 },
  decimals: { index: 2, parser: parseU8 },
  total_supply: { index: 2, parser: parseU128 },
});
export type TokenRegistrationEvent = GetParserType<
  typeof parseRegistrationEvent
>;

export const parseRegistrationEventV3 = combineParsers({
  address: { index: 0, parser: parseAddress },
  name: { index: 1, parser: parseByteArray },
  symbol: { index: 2, parser: parseByteArray },
  decimals: { index: 2, parser: parseU8 },
  total_supply: { index: 2, parser: parseU128 },
});
export type TokenRegistrationEventV3 = GetParserType<
  typeof parseRegistrationEventV3
>;
