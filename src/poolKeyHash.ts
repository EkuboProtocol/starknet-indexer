import type { PoolKey } from "./events/core";
import { createHash } from "crypto";

// instead uses the node crypto api: https://nodejs.org/api/crypto.html#hashdigestencoding
export function computeKeyHash(pool_key: PoolKey): bigint {
  return BigInt(
    `0x${createHash("sha256")
      .update(pool_key.token0.toString(16).padStart(64, "0"), "hex")
      .update(pool_key.token1.toString(16).padStart(64, "0"), "hex")
      .update(pool_key.fee.toString(16).padStart(32, "0"), "hex")
      .update(pool_key.tick_spacing.toString(16).padStart(32, "0"), "hex")
      .update(pool_key.extension.toString(16).padStart(64, "0"), "hex")
      .digest("hex")}`,
  );
}
