import {
    combineParsers,
    parseAddress,
    parseI129,
    parseU128,
    parseU256,
  } from "../parse";
  import type { GetParserType } from "../parse";
  
  export const parsePoolKey = combineParsers({
    token0: { index: 0, parser: parseAddress },
    token1: { index: 1, parser: parseAddress },
    fee: { index: 2, parser: parseU128 },
    tick_spacing: { index: 3, parser: parseU128 },
    extension: { index: 4, parser: parseAddress },
  });
  export type PoolKey = GetParserType<typeof parsePoolKey>;
  
  export const parseLiquidityUpdated = combineParsers({
    pool_key: { index: 0, parser: parsePoolKey },
    sender: { index: 1, parser: parseAddress },
    liquidity_factor: { index: 2, parser: parseI129 },
    shares: { index: 3, parser: parseU256 },
    amount0: { index: 4, parser: parseI129 },
    amount1: { index: 5, parser: parseI129 },
    protocol_fees0: { index: 6, parser: parseU128 },
    protocol_fees1: { index: 7, parser: parseU128 },
  });
  export type LiquidityUpdatedEvent = GetParserType<typeof parseLiquidityUpdated>;