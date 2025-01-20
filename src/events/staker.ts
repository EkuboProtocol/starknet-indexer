import { combineParsers, parseAddress, parseU128 } from "../parse";
import type { GetParserType } from "../parse";

export const parseStakedEvent = combineParsers({
  from: { index: 0, parser: parseAddress },
  amount: { index: 1, parser: parseU128 },
  delegate: { index: 2, parser: parseAddress },
});
export type StakedEvent = GetParserType<typeof parseStakedEvent>;

export const parseWithdrawnEvent = combineParsers({
  from: { index: 0, parser: parseAddress },
  delegate: { index: 1, parser: parseAddress },
  to: { index: 2, parser: parseAddress },
  amount: { index: 3, parser: parseU128 },
});
export type WithdrawnEvent = GetParserType<typeof parseWithdrawnEvent>;
