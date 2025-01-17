import type { PoolKey } from "./events/core";
import { createHash } from "crypto";

const KEY_HASH_CACHE: { [key: string]: bigint } = {};

function computeCacheKey(pool_key: PoolKey): string {
  return `${pool_key.token0}-${pool_key.token1}-${pool_key.fee}-${pool_key.tick_spacing}-${pool_key.extension}`;
}

export function populateCache(
  values: { pool_key: PoolKey; hash: bigint }[],
): void {
  values.forEach(
    ({ pool_key, hash }) => (KEY_HASH_CACHE[computeCacheKey(pool_key)] = hash),
  );
}

// instead uses the node crypto api: https://nodejs.org/api/crypto.html#hashdigestencoding
export function computeKeyHash(pool_key: PoolKey): bigint {
  const cacheKey = computeCacheKey(pool_key);
  return (
    KEY_HASH_CACHE[cacheKey] ??
    (KEY_HASH_CACHE[cacheKey] = BigInt(
      `0x${createHash("sha256")
        .update(pool_key.token0.toString(16), "hex")
        .update(pool_key.token1.toString(16), "hex")
        .update(pool_key.fee.toString(16), "hex")
        .update(pool_key.tick_spacing.toString(16), "hex")
        .update(pool_key.extension.toString(16), "hex")
        .digest("hex")}`,
    ))
  );
}
