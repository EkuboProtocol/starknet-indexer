import { PoolKey } from "./events/core";
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

export function computeKeyHash(pool_key: PoolKey): bigint {
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