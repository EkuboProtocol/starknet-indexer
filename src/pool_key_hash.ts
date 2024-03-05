import { PoolKey } from "./events/core";
import { OrderKey } from "./events/twamm";
import { pedersen_from_hex } from "pedersen-fast";
import { num } from "starknet";

const KEY_HASH_CACHE: { [key: string]: bigint } = {};

function computeCacheKey(pool_key: PoolKey): string {
  return `${pool_key.token0}-${pool_key.token1}-${pool_key.fee}-${pool_key.tick_spacing}-${pool_key.extension}`;
}

export function populateCache(
  values: { pool_key: PoolKey; hash: bigint }[]
): void {
  values.forEach(
    ({ pool_key, hash }) => (KEY_HASH_CACHE[computeCacheKey(pool_key)] = hash)
  );
}

export function computePoolKeyHash(pool_key: PoolKey): bigint {
  const cacheKey = computeCacheKey(pool_key);
  return (
    KEY_HASH_CACHE[cacheKey] ??
    (KEY_HASH_CACHE[cacheKey] = BigInt(
      pedersen_from_hex(
        pedersen_from_hex(
          pedersen_from_hex(
            num.toHex(pool_key.token0),
            num.toHex(pool_key.token1)
          ),
          pedersen_from_hex(
            num.toHex(pool_key.fee),
            num.toHex(pool_key.tick_spacing)
          )
        ),
        num.toHex(pool_key.extension)
      )
    ))
  );
}

export function computeTWAMMOrderKeyHash(order_key: OrderKey): bigint {
  // todo: maybe cache?
  return BigInt(
      pedersen_from_hex(
        pedersen_from_hex(
          pedersen_from_hex(
            num.toHex(order_key.sell_token),
            num.toHex(order_key.buy_token)
          ),
          pedersen_from_hex(
            num.toHex(order_key.fee),
            num.toHex(order_key.start_time)
          )
        ),
        num.toHex(order_key.end_time)
      )
    );
}