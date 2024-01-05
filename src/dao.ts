import { Client, PoolClient } from "pg";
import { pedersen_from_hex } from "pedersen-fast";
import { EventKey } from "./processor";
import {
  FeesAccumulatedEvent,
  FeesPaidEvent,
  FeesWithdrawnEvent,
  PoolInitializationEvent,
  PoolKey,
  PositionFeesCollectedEvent,
  PositionUpdatedEvent,
  SwappedEvent,
  TokenRegistrationEvent,
} from "./events/core";
import {
  DepositEvent,
  PositionMintedEvent,
  WithdrawEvent,
} from "./events/positions";
import { TransferEvent } from "./events/nft";

function toHex(x: bigint): string {
  return `0x${x.toString(16)}`;
}

const KEY_HASH_CACHE: { [key: string]: bigint } = {};

function computeKeyHash(pool_key: PoolKey): bigint {
  const cacheKey = `${pool_key.token0}-${pool_key.token1}-${pool_key.fee}-${pool_key.tick_spacing}-${pool_key.extension}`;
  return (
    KEY_HASH_CACHE[cacheKey] ??
    (KEY_HASH_CACHE[cacheKey] = BigInt(
      pedersen_from_hex(
        pedersen_from_hex(
          pedersen_from_hex(toHex(pool_key.token0), toHex(pool_key.token1)),
          pedersen_from_hex(toHex(pool_key.fee), toHex(pool_key.tick_spacing))
        ),
        toHex(pool_key.extension)
      )
    ))
  );
}

// Data access object that manages inserts/deletes
export class DAO {
  private pg: Client | PoolClient;

  constructor(pg: Client | PoolClient) {
    this.pg = pg;
  }

  public async beginTransaction(): Promise<void> {
    await this.pg.query("BEGIN");
  }

  public async commitTransaction(): Promise<void> {
    await this.pg.query("COMMIT");
  }

  public async initializeSchema() {
    await this.beginTransaction();
    await this.initSchema();
    const cursor = await this.loadCursor();
    // we need to clear anything that was potentially inserted as pending before starting
    if (cursor) {
      await this.deleteOldBlockNumbers(BigInt(cursor.orderKey) + 1n);
    }
    await this.commitTransaction();
    return cursor;
  }

  private async initSchema(): Promise<void> {
    await this.pg.query(`
        CREATE TABLE IF NOT EXISTS cursor
        (
            id           INT         NOT NULL UNIQUE CHECK (id = 1), -- only one row.
            order_key    NUMERIC     NOT NULL,
            unique_key   TEXT        NOT NULL,
            last_updated timestamptz NOT NULL
        );

        CREATE TABLE IF NOT EXISTS blocks
        (
            -- int4 blocks represents over a thousand years at 12 second blocks
            number   int4        NOT NULL PRIMARY KEY,
            hash     NUMERIC     NOT NULL,
            time     timestamptz NOT NULL,
            inserted timestamptz NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_blocks_time ON blocks USING btree (time);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_blocks_hash ON blocks USING btree (hash);

        CREATE TABLE IF NOT EXISTS pool_keys
        (
            key_hash     NUMERIC NOT NULL PRIMARY KEY,
            token0       NUMERIC NOT NULL,
            token1       NUMERIC NOT NULL,
            fee          NUMERIC NOT NULL,
            tick_spacing INT     NOT NULL,
            extension    NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pool_keys_token0 ON pool_keys USING btree (token0);
        CREATE INDEX IF NOT EXISTS idx_pool_keys_token1 ON pool_keys USING btree (token1);
        CREATE INDEX IF NOT EXISTS idx_pool_keys_token0_token1 ON pool_keys USING btree (token0, token1);
        CREATE INDEX IF NOT EXISTS idx_pool_keys_extension ON pool_keys USING btree (extension);

        -- all events reference an event id which contains the metadata of the event
        CREATE TABLE IF NOT EXISTS event_keys
        (
            id                int8 GENERATED ALWAYS AS (block_number * 4294967296 + transaction_index * 65536 + event_index) STORED PRIMARY KEY,
            transaction_hash  NUMERIC NOT NULL,
            block_number      int4    NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int2    NOT NULL,
            event_index       int2    NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_event_keys_block_number_transaction_index_event_index ON event_keys USING btree (block_number, transaction_index, event_index);
        CREATE INDEX IF NOT EXISTS idx_event_keys_transaction_hash ON event_keys USING btree (transaction_hash);

        CREATE TABLE IF NOT EXISTS position_minted
        (
            event_id      int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            token_id      int8    NOT NULL,
            lower_bound   int4    NOT NULL,
            upper_bound   int4    NOT NULL,

            referrer      NUMERIC,

            pool_key_hash NUMERIC NOT NULL REFERENCES pool_keys (key_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_position_minted_pool_key_hash ON position_minted (pool_key_hash);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_position_minted_token_id ON position_minted (token_id);

        CREATE TABLE IF NOT EXISTS position_deposit
        (
            event_id      int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            token_id      int8    NOT NULL,
            lower_bound   int4    NOT NULL,
            upper_bound   int4    NOT NULL,

            pool_key_hash NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            liquidity     NUMERIC NOT NULL,
            delta0        NUMERIC NOT NULL,
            delta1        NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_position_deposit_pool_key_hash ON position_deposit (pool_key_hash);
        CREATE INDEX IF NOT EXISTS idx_position_deposit_token_id ON position_deposit (token_id);

        CREATE TABLE IF NOT EXISTS position_withdraw
        (
            event_id      int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            token_id      int8    NOT NULL,
            lower_bound   int4    NOT NULL,
            upper_bound   int4    NOT NULL,

            pool_key_hash NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            collect_fees  bool    NOT NULL,

            liquidity     NUMERIC NOT NULL,
            delta0        NUMERIC NOT NULL,
            delta1        NUMERIC NOT NULL,

            recipient     NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_position_withdraw_pool_key_hash ON position_withdraw (pool_key_hash);
        CREATE INDEX IF NOT EXISTS idx_position_withdraw_token_id ON position_withdraw (token_id);

        CREATE TABLE IF NOT EXISTS position_transfers
        (
            event_id     int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            token_id     int8    NOT NULL,
            from_address NUMERIC NOT NULL,
            to_address   NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_position_transfers_token_id_from_to ON position_transfers (token_id, from_address, to_address);

        CREATE TABLE IF NOT EXISTS position_updates
        (
            event_id        int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            locker          NUMERIC NOT NULL,

            pool_key_hash   NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            salt            NUMERIC NOT NULL,
            lower_bound     int4    NOT NULL,
            upper_bound     int4    NOT NULL,

            liquidity_delta NUMERIC NOT NULL,
            delta0          NUMERIC NOT NULL,
            delta1          NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_position_updates_pool_key_hash_event_id ON position_updates USING btree (pool_key_hash, event_id);

        CREATE TABLE IF NOT EXISTS position_fees_collected
        (
            event_id      int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            pool_key_hash NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            owner         NUMERIC NOT NULL,
            salt          NUMERIC NOT NULL,
            lower_bound   int4    NOT NULL,
            upper_bound   int4    NOT NULL,

            delta0        NUMERIC NOT NULL,
            delta1        NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_position_fees_collected_pool_key_hash ON position_fees_collected (pool_key_hash);

        CREATE TABLE IF NOT EXISTS protocol_fees_withdrawn
        (
            event_id  int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            recipient NUMERIC NOT NULL,
            token     NUMERIC NOT NULL,
            amount    NUMERIC NOT NULL
        );


        CREATE TABLE IF NOT EXISTS protocol_fees_paid
        (
            event_id      int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            pool_key_hash NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            owner         NUMERIC NOT NULL,
            salt          NUMERIC NOT NULL,
            lower_bound   int4    NOT NULL,
            upper_bound   int4    NOT NULL,

            delta0        NUMERIC NOT NULL,
            delta1        NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_protocol_fees_paid_pool_key_hash ON protocol_fees_paid (pool_key_hash);

        CREATE TABLE IF NOT EXISTS fees_accumulated
        (
            event_id      int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            pool_key_hash NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            amount0       NUMERIC NOT NULL,
            amount1       NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_fees_accumulated_pool_key_hash ON fees_accumulated (pool_key_hash);

        CREATE TABLE IF NOT EXISTS pool_initializations
        (
            event_id      int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            pool_key_hash NUMERIC  NOT NULL REFERENCES pool_keys (key_hash),

            tick          int4     NOT NULL,
            sqrt_ratio    NUMERIC  NOT NULL,
            call_points   SMALLINT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pool_initializations_pool_key_hash ON pool_initializations (pool_key_hash);


        CREATE TABLE IF NOT EXISTS swaps
        (
            event_id         int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            locker           NUMERIC NOT NULL,
            pool_key_hash    NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            delta0           NUMERIC NOT NULL,
            delta1           NUMERIC NOT NULL,

            sqrt_ratio_after NUMERIC NOT NULL,
            tick_after       int4    NOT NULL,
            liquidity_after  NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_swaps_pool_key_hash_event_id ON swaps USING btree (pool_key_hash, event_id);


        CREATE TABLE IF NOT EXISTS token_registrations
        (
            event_id     int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            address      NUMERIC NOT NULL,

            name         NUMERIC NOT NULL,
            symbol       NUMERIC NOT NULL,
            decimals     INT     NOT NULL,
            total_supply NUMERIC NOT NULL
        );

        CREATE OR REPLACE VIEW pool_states_view AS
        (
        WITH lss AS (SELECT key_hash,
                            COALESCE(last_swap.event_id, pi.event_id)           AS last_swap_event_id,
                            COALESCE(last_swap.sqrt_ratio_after, pi.sqrt_ratio) AS sqrt_ratio,
                            COALESCE(last_swap.tick_after, pi.tick)             AS tick,
                            COALESCE(last_swap.liquidity_after, 0)              AS liquidity_last
                     FROM pool_keys
                              LEFT JOIN LATERAL (
                         SELECT event_id, sqrt_ratio_after, tick_after, liquidity_after
                         FROM swaps
                         WHERE pool_keys.key_hash = swaps.pool_key_hash
                         ORDER BY event_id DESC
                         LIMIT 1
                         ) AS last_swap ON TRUE
                              LEFT JOIN LATERAL (
                         SELECT event_id, sqrt_ratio, tick
                         FROM pool_initializations
                         WHERE pool_initializations.pool_key_hash = pool_keys.key_hash
                         ORDER BY event_id DESC
                         LIMIT 1
                         ) AS pi ON TRUE),
             pl AS (SELECT key_hash,
                           (SELECT event_id
                            FROM position_updates
                            WHERE key_hash = position_updates.pool_key_hash
                            ORDER BY event_id DESC
                            LIMIT 1)                                   AS last_update_event_id,
                           (COALESCE(liquidity_last, 0) + COALESCE((SELECT SUM(liquidity_delta)
                                                                    FROM position_updates AS pu
                                                                    WHERE lss.last_swap_event_id < pu.event_id
                                                                      AND pu.pool_key_hash = lss.key_hash
                                                                      AND lss.tick BETWEEN pu.lower_bound AND (pu.upper_bound - 1)),
                                                                   0)) AS liquidity
                    FROM lss)
        SELECT lss.key_hash                                              AS pool_key_hash,
               sqrt_ratio,
               tick,
               liquidity,
               GREATEST(lss.last_swap_event_id, pl.last_update_event_id) AS last_event_id,
               pl.last_update_event_id                                   AS last_liquidity_update_event_id
        FROM lss
                 JOIN pl ON lss.key_hash = pl.key_hash
            );

        CREATE MATERIALIZED VIEW IF NOT EXISTS pool_states_materialized AS
        (
        SELECT pool_key_hash, last_event_id, last_liquidity_update_event_id, sqrt_ratio, liquidity, tick
        FROM pool_states_view);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_pool_states_materialized_pool_key_hash ON pool_states_materialized USING btree (pool_key_hash);


        CREATE OR REPLACE VIEW volume_by_token_by_hour_by_key_hash_view AS
        (
        SELECT DATE_TRUNC('hour', blocks.time)                        AS hour,
               key_hash,
               (CASE WHEN delta0 >= 0 THEN token0 ELSE token1 END)       token,
               SUM(CASE WHEN delta0 >= 0 THEN delta0 ELSE delta1 END) AS volume,
               SUM(FLOOR(((CASE WHEN delta0 >= 0 THEN delta0 ELSE delta1 END) * fee) /
                         340282366920938463463374607431768211456))    AS fees
        FROM swaps
                 JOIN pool_keys ON swaps.pool_key_hash = pool_keys.key_hash
                 JOIN event_keys ON swaps.event_id = event_keys.id
                 JOIN blocks ON event_keys.block_number = blocks.number
        GROUP BY hour, key_hash, token
            );

        CREATE MATERIALIZED VIEW IF NOT EXISTS volume_by_token_by_hour_by_key_hash_materialized AS
        (
        SELECT hour, key_hash, token, volume, fees
        FROM volume_by_token_by_hour_by_key_hash_view);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_volume_by_token_by_hour_by_hour_key_hash_materialized_token ON volume_by_token_by_hour_by_key_hash_materialized USING btree (key_hash, hour, token);

        CREATE OR REPLACE VIEW tvl_delta_by_token_by_hour_by_key_hash_view AS
        (
        WITH grouped_pool_key_hash_deltas AS (SELECT pool_key_hash,
                                                     DATE_TRUNC('hour', blocks.time) AS hour,
                                                     SUM(delta0)                     AS delta0,
                                                     SUM(delta1)                     AS delta1
                                              FROM swaps
                                                       JOIN event_keys ON swaps.event_id = event_keys.id
                                                       JOIN blocks ON event_keys.block_number = blocks.number
                                              GROUP BY pool_key_hash, hour

                                              UNION ALL

                                              SELECT pool_key_hash,
                                                     DATE_TRUNC('hour', blocks.time) AS hour,
                                                     SUM(delta0)                     AS delta0,
                                                     SUM(delta1)                     AS delta1
                                              FROM position_updates
                                                       JOIN event_keys ON position_updates.event_id = event_keys.id
                                                       JOIN blocks ON event_keys.block_number = blocks.number
                                              GROUP BY pool_key_hash, hour

                                              UNION ALL

                                              SELECT pool_key_hash,
                                                     DATE_TRUNC('hour', blocks.time) AS hour,
                                                     SUM(delta0)                     AS delta0,
                                                     SUM(delta1)                     AS delta1
                                              FROM position_fees_collected
                                                       JOIN event_keys ON position_fees_collected.event_id = event_keys.id
                                                       JOIN blocks ON event_keys.block_number = blocks.number
                                              GROUP BY pool_key_hash, hour

                                              UNION ALL

                                              SELECT pool_key_hash,
                                                     DATE_TRUNC('hour', blocks.time) AS hour,
                                                     SUM(delta0)                     AS delta0,
                                                     SUM(delta1)                     AS delta1
                                              FROM protocol_fees_paid
                                                       JOIN event_keys ON protocol_fees_paid.event_id = event_keys.id
                                                       JOIN blocks ON event_keys.block_number = blocks.number
                                              GROUP BY pool_key_hash, hour

                                              UNION ALL

                                              SELECT pool_key_hash,
                                                     DATE_TRUNC('hour', blocks.time) AS hour,
                                                     SUM(amount0)                    AS delta0,
                                                     SUM(amount1)                    AS delta1
                                              FROM fees_accumulated
                                                       JOIN event_keys ON fees_accumulated.event_id = event_keys.id
                                                       JOIN blocks ON event_keys.block_number = blocks.number
                                              GROUP BY pool_key_hash, hour),
             token_deltas AS (SELECT pool_key_hash, hour, pool_keys.token0 AS token, SUM(delta0) AS delta
                              FROM grouped_pool_key_hash_deltas
                                       JOIN pool_keys
                                            ON pool_keys.key_hash = grouped_pool_key_hash_deltas.pool_key_hash
                              GROUP BY pool_key_hash, hour, pool_keys.token0

                              UNION ALL

                              SELECT pool_key_hash, hour, pool_keys.token1 AS token, SUM(delta1) AS delta
                              FROM grouped_pool_key_hash_deltas
                                       JOIN pool_keys
                                            ON pool_keys.key_hash = grouped_pool_key_hash_deltas.pool_key_hash
                              GROUP BY pool_key_hash, hour, pool_keys.token1)
        SELECT token,
               pool_key_hash AS key_hash,
               hour,
               SUM(delta)    AS delta
        FROM token_deltas
        GROUP BY token, key_hash, hour
            );

        CREATE MATERIALIZED VIEW IF NOT EXISTS tvl_delta_by_token_by_hour_by_key_hash_materialized AS
        (
        SELECT token, key_hash, hour, delta
        FROM tvl_delta_by_token_by_hour_by_key_hash_view);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_tvl_delta_by_token_by_hour_by_key_hash_token_hour_key_hash ON tvl_delta_by_token_by_hour_by_key_hash_materialized USING btree (key_hash, hour, token);

        CREATE OR REPLACE VIEW per_pool_per_tick_liquidity_view AS
        (
        WITH all_tick_deltas AS (SELECT pool_key_hash,
                                        lower_bound AS       tick,
                                        SUM(liquidity_delta) net_liquidity_delta
                                 FROM position_updates
                                 GROUP BY pool_key_hash, lower_bound
                                 UNION ALL
                                 SELECT pool_key_hash,
                                        upper_bound AS        tick,
                                        SUM(-liquidity_delta) net_liquidity_delta
                                 FROM position_updates
                                 GROUP BY pool_key_hash, upper_bound),
             summed AS (SELECT pool_key_hash,
                               tick,
                               SUM(net_liquidity_delta) AS net_liquidity_delta_diff
                        FROM all_tick_deltas
                        GROUP BY pool_key_hash, tick)
        SELECT pool_key_hash, tick, net_liquidity_delta_diff
        FROM summed
        WHERE net_liquidity_delta_diff != 0
        ORDER BY tick);

        CREATE MATERIALIZED VIEW IF NOT EXISTS per_pool_per_tick_liquidity_materialized AS
        (
        SELECT pool_key_hash, tick, net_liquidity_delta_diff
        FROM per_pool_per_tick_liquidity_view);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_per_pool_per_tick_liquidity_pool_key_hash_tick ON per_pool_per_tick_liquidity_materialized USING btree (pool_key_hash, tick);

        CREATE OR REPLACE VIEW pair_vwap_preimages_view AS
        (
        SELECT date_bin('15 minutes', blocks.time, '2000-1-1') AS timestamp_start,
               token0,
               token1,
               SUM(delta1 * delta1)                            AS total,
               SUM(ABS(delta0 * delta1))                       AS k_volume
        FROM swaps
                 JOIN event_keys ON swaps.event_id = event_keys.id
                 JOIN blocks ON event_keys.block_number = blocks.number
                 JOIN pool_keys ON swaps.pool_key_hash = pool_keys.key_hash
        GROUP BY token0, token1, timestamp_start
            );

        CREATE MATERIALIZED VIEW IF NOT EXISTS pair_vwap_preimages_materialized AS
        (
        SELECT timestamp_start, token0, token1, total, k_volume
        FROM pair_vwap_preimages_view);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_pair_vwap_preimages_materialized_token0_token1_timestamp ON pair_vwap_preimages_materialized USING btree (token0, token1, timestamp_start);

        CREATE OR REPLACE VIEW leaderboard_view AS
        (
        WITH all_tokens AS (SELECT token0 AS token FROM pool_keys UNION DISTINCT SELECT token1 FROM pool_keys),

             pair_swap_counts AS (SELECT pk.token0, pk.token1, COUNT(1) AS swap_count
                                  FROM swaps AS s
                                           JOIN event_keys AS ek ON s.event_id = ek.id
                                           JOIN blocks AS b ON ek.block_number = b.number
                                           JOIN pool_keys AS pk ON s.pool_key_hash = pk.key_hash
                                  WHERE b.time >= (NOW() - INTERVAL '1 month')
                                  GROUP BY pk.token0, pk.token1),

             swap_counts_as_t0 AS (SELECT token0 AS token, SUM(swap_count) AS swap_count
                                   FROM pair_swap_counts
                                   GROUP BY token0),

             swap_counts_as_t1 AS (SELECT token1 AS token, SUM(swap_count) AS swap_count
                                   FROM pair_swap_counts
                                   GROUP BY token1),

             -- all the tokens and the respective total number of swaps for each token
             all_tokens_with_swap_counts AS (SELECT at.token                                                  AS token,
                                                    (COALESCE(s0.swap_count, 0) + COALESCE(s1.swap_count, 0)) AS swap_count
                                             FROM all_tokens AS at
                                                      LEFT JOIN
                                                  swap_counts_as_t0 AS s0 ON at.token = s0.token
                                                      LEFT JOIN
                                                  swap_counts_as_t1 AS s1 ON at.token = s1.token),

             -- this boost allows users to earn more points by depositing liquidity in pools that are heavily utilized
             pair_points_boost AS (SELECT token0,
                                          token1,
                                          1::NUMERIC AS multiplier
                                   -- todo: decide on these factors
                                   -- 0.1 + 9.9 * LN(1 + (EXP(1) - 1) * swap_count / (SUM(swap_count) OVER ())) multiplier
                                   FROM pair_swap_counts),

             fee_to_discount_factor AS (SELECT DISTINCT fee,
                                                        1 - SQRT(fee / 340282366920938463463374607431768211456) AS fee_discount
                                        FROM pool_keys),

             -- we compute the VWAP price in eth per token over the last month for each token we will consider
             token_points_rates AS
                 (SELECT token,
                         (CASE
                              WHEN swap_count < 3000 THEN 0
                              WHEN token =
                                   2087021424722619777119509474943472645767659996348769578120564519014510906823
                                  THEN 1
                              WHEN token <
                                   2087021424722619777119509474943472645767659996348769578120564519014510906823
                                  THEN (SELECT SUM(delta1 * delta1) / SUM(ABS(delta0 * delta1))
                                        FROM swaps
                                                 JOIN pool_keys ON swaps.pool_key_hash = pool_keys.key_hash
                                                 JOIN event_keys ON swaps.event_id = event_keys.id
                                                 JOIN blocks ON event_keys.block_number = blocks.number
                                        WHERE token0 = token
                                          AND token1 =
                                              2087021424722619777119509474943472645767659996348769578120564519014510906823
                                          AND blocks.time >= NOW() - INTERVAL '1 month')
                              ELSE
                                  (SELECT SUM(ABS(delta0 * delta1)) / SUM(delta1 * delta1)
                                   FROM swaps
                                            JOIN pool_keys ON swaps.pool_key_hash = pool_keys.key_hash
                                            JOIN event_keys ON swaps.event_id = event_keys.id
                                            JOIN blocks ON event_keys.block_number = blocks.number
                                   WHERE token0 =
                                         2087021424722619777119509474943472645767659996348769578120564519014510906823
                                     AND token1 = token
                                     AND blocks.time >= NOW() - INTERVAL '1 month')
                             END) AS rate
                  FROM all_tokens_with_swap_counts),

             position_multipliers AS (SELECT pm.token_id AS token_id,
                                             2 *
                                             EXP(GREATEST((pmb.time::DATE - '2023-09-14'::DATE), 0) * -0.01) +
                                             1           AS multiplier
                                      FROM position_minted AS pm
                                               JOIN event_keys ON pm.event_id = event_keys.id
                                               JOIN blocks AS pmb ON event_keys.block_number = pmb.number),

             points_from_mints AS (SELECT pmb.time                              AS points_earned_timestamp,
                                          (SELECT to_address
                                           FROM position_transfers AS pt
                                           WHERE pt.token_id = pm.token_id
                                           ORDER BY pt.event_id
                                           LIMIT 1)                             AS collector,
                                          pm.referrer                           AS referrer,
                                          (2000 * multipliers.multiplier)::int8 AS points
                                   FROM position_minted AS pm
                                            JOIN event_keys AS pmek ON pm.event_id = pmek.id
                                            JOIN position_deposit AS pd
                                                 ON pm.token_id = pd.token_id
                                                     -- this means only a single deposit per mint
                                                     AND pd.event_id = pm.event_id + 4
                                                     -- this means non-zero deposit of in range
                                                     AND pd.delta0 != 0 AND pd.delta1 != 0
                                            JOIN position_multipliers AS multipliers
                                                 ON pm.token_id = multipliers.token_id
                                            JOIN blocks AS pmb ON pmek.block_number = pmb.number),

             position_from_withdrawal_fees_paid AS (SELECT pfpb.time                  AS points_earned_timestamp,
                                                           (SELECT to_address
                                                            FROM position_transfers AS pt
                                                            WHERE pt.token_id = pfp.salt::int8
                                                              AND pt.event_id <
                                                                  pfp.event_id
                                                            ORDER BY pt.event_id DESC
                                                            LIMIT 1)                  AS collector,
                                                           pm.referrer                AS referrer,
                                                           FLOOR(ABS(
                                                                         (pfp.delta0 * tp0.rate * fd.fee_discount) +
                                                                         (pfp.delta1 * tp1.rate * fd.fee_discount)
                                                                 ) * multipliers.multiplier *
                                                                 COALESCE(ppb.multiplier, 1) /
                                                                 1e12::NUMERIC)::int8 AS points
                                                    FROM protocol_fees_paid AS pfp
                                                             JOIN event_keys AS pfek ON pfp.event_id = pfek.id
                                                             JOIN blocks AS pfpb ON pfek.block_number = pfpb.number
                                                        -- todo: prevent points from being earned if the deposit/withdraw is a different pool key hash than the mint
                                                        -- AND pfp.pool_key_hash = pm.pool_key_hash
                                                             JOIN position_minted AS pm ON pfp.salt::BIGINT = pm.token_id
                                                             JOIN position_multipliers AS multipliers
                                                                  ON pm.token_id = multipliers.token_id
                                                             JOIN pool_keys AS pk ON pfp.pool_key_hash = pk.key_hash
                                                             LEFT JOIN pair_points_boost AS ppb
                                                                       ON pk.token0 = ppb.token0 AND pk.token1 = ppb.token1
                                                             JOIN token_points_rates AS tp0 ON tp0.token = pk.token0
                                                             JOIN token_points_rates AS tp1 ON tp1.token = pk.token1
                                                             JOIN fee_to_discount_factor AS fd ON pk.fee = fd.fee),

             points_from_fees AS (SELECT pfb.time                   AS points_earned_timestamp,
                                         (SELECT to_address
                                          FROM position_transfers AS pt
                                          WHERE pt.token_id = pf.salt::int8
                                            AND pt.event_id < pf.event_id
                                          ORDER BY pt.event_id DESC
                                          LIMIT 1)                  AS collector,
                                         pm.referrer                AS referrer,
                                         FLOOR(ABS(SUM(
                                                 (pf.delta0 * tp0.rate * fd.fee_discount) +
                                                 (pf.delta1 * tp1.rate * fd.fee_discount)
                                                   )) * multipliers.multiplier * COALESCE(ppb.multiplier, 1) /
                                               1e12::NUMERIC)::int8 AS points
                                  FROM position_minted AS pm
                                           JOIN position_fees_collected AS pf ON pm.token_id = pf.salt::int8
                                      -- todo: prevent points from being earned if the deposit/withdraw is a different pool key hash than the mint
                                      -- AND pfp.pool_key_hash = pm.pool_key_hash
                                           JOIN position_multipliers AS multipliers
                                                ON pm.token_id = multipliers.token_id
                                           JOIN event_keys AS pmek ON pm.event_id = pmek.id
                                           JOIN blocks AS pmb ON pmek.block_number = pmb.number
                                           JOIN event_keys AS pfek ON pf.event_id = pfek.id
                                           JOIN blocks AS pfb ON pfek.block_number = pfb.number
                                           JOIN pool_keys AS pk ON pf.pool_key_hash = pk.key_hash
                                           LEFT JOIN pair_points_boost AS ppb
                                                     ON pk.token0 = ppb.token0 AND pk.token1 = ppb.token1
                                           JOIN fee_to_discount_factor AS fd ON pk.fee = fd.fee
                                           JOIN token_points_rates AS tp0 ON tp0.token = pk.token0
                                           JOIN token_points_rates AS tp1 ON tp1.token = pk.token1
                                  GROUP BY pfb.time, multipliers.multiplier, ppb.multiplier, collector, referrer),

             points_by_collector_with_referrer AS (SELECT points_earned_timestamp, collector, referrer, points
                                                   FROM points_from_fees
                                                   UNION ALL
                                                   SELECT points_earned_timestamp, collector, referrer, points
                                                   FROM points_from_mints
                                                   UNION ALL
                                                   SELECT points_earned_timestamp, collector, referrer, points
                                                   FROM position_from_withdrawal_fees_paid)
        SELECT date_bin(INTERVAL '1 day', points_earned_timestamp, '2000-01-01') AS points_earned_day,
               collector,
               referrer,
               SUM(points)                                                       AS points
        FROM points_by_collector_with_referrer
        GROUP BY points_earned_day, collector, referrer
            );

        CREATE TABLE IF NOT EXISTS leaderboard
        (
            points_earned_day timestamptz NOT NULL,
            collector         NUMERIC     NOT NULL,
            referrer          NUMERIC     NOT NULL,
            points            BIGINT      NOT NULL,
            PRIMARY KEY (points_earned_day, collector, referrer)
        );
    `);
  }

  public async refreshAnalyticalMaterializedViews() {
    await this.pg.query(`
      REFRESH MATERIALIZED VIEW CONCURRENTLY volume_by_token_by_hour_by_key_hash_materialized;
      REFRESH MATERIALIZED VIEW CONCURRENTLY tvl_delta_by_token_by_hour_by_key_hash_materialized;
      REFRESH MATERIALIZED VIEW CONCURRENTLY pair_vwap_preimages_materialized;
    `);
  }

  public async refreshOperationalMaterializedView() {
    await this.pg.query(`
      REFRESH MATERIALIZED VIEW CONCURRENTLY per_pool_per_tick_liquidity_materialized;
      REFRESH MATERIALIZED VIEW CONCURRENTLY pool_states_materialized;
    `);
  }

  private async loadCursor(): Promise<{
    orderKey: string;
    uniqueKey: string;
  } | null> {
    const { rows } = await this.pg.query({
      text: `SELECT order_key, unique_key
                   FROM cursor
                   WHERE id = 1;`,
    });
    if (rows.length === 1) {
      const { order_key, unique_key } = rows[0];

      return {
        orderKey: order_key,
        uniqueKey: unique_key,
      };
    } else {
      return null;
    }
  }

  public async writeCursor(cursor: { orderKey: string; uniqueKey: string }) {
    await this.pg.query({
      text: `
                INSERT INTO cursor (id, order_key, unique_key, last_updated)
                VALUES (1, $1, $2, NOW())
                ON CONFLICT (id) DO UPDATE SET order_key    = $1,
                                               unique_key   = $2,
                                               last_updated = NOW();
            `,
      values: [BigInt(cursor.orderKey), cursor.uniqueKey],
    });
  }

  public async insertBlock({
    number,
    hash,
    time,
  }: {
    number: bigint;
    hash: bigint;
    time: Date;
  }) {
    await this.pg.query({
      text: `
                INSERT INTO blocks (number, hash, time)
                VALUES ($1, $2, $3);
            `,
      values: [number, hash, time],
    });
  }

  private async insertPoolKeyHash(pool_key: PoolKey) {
    const key_hash = computeKeyHash(pool_key);

    await this.pg.query({
      text: `
                INSERT INTO pool_keys (key_hash,
                                       token0,
                                       token1,
                                       fee,
                                       tick_spacing,
                                       extension)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT DO NOTHING;
            `,
      values: [
        key_hash,
        BigInt(pool_key.token0),
        BigInt(pool_key.token1),
        pool_key.fee,
        pool_key.tick_spacing,
        BigInt(pool_key.extension),
      ],
    });
    return key_hash;
  }

  public async insertPositionMinted(
    minted: PositionMintedEvent,
    key: EventKey
  ) {
    const pool_key_hash = await this.insertPoolKeyHash(minted.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO position_minted
                (event_id,
                 token_id,
                 lower_bound,
                 upper_bound,
                 referrer,
                 pool_key_hash)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7, $8, $9);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        minted.id,
        minted.bounds.lower,
        minted.bounds.upper,
        // we treat 0n as null so queries don't have to filter by both
        minted.referrer !== 0n ? minted.referrer : null,
        pool_key_hash,
      ],
    });
  }

  public async insertPositionDeposit(deposit: DepositEvent, key: EventKey) {
    const pool_key_hash = await this.insertPoolKeyHash(deposit.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO position_deposit
                (event_id,
                 token_id,
                 lower_bound,
                 upper_bound,
                 pool_key_hash,
                 liquidity,
                 delta0,
                 delta1)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7, $8, $9, $10, $11);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        deposit.id,
        deposit.bounds.lower,
        deposit.bounds.upper,
        pool_key_hash,
        deposit.liquidity,
        deposit.delta.amount0,
        deposit.delta.amount1,
      ],
    });
  }

  public async insertPositionWithdraw(withdraw: WithdrawEvent, key: EventKey) {
    const pool_key_hash = await this.insertPoolKeyHash(withdraw.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO position_withdraw
                (event_id,
                 token_id,
                 lower_bound,
                 upper_bound,
                 pool_key_hash,
                 collect_fees,
                 liquidity,
                 delta0,
                 delta1,
                 recipient)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7, $8, $9, $10, $11, $12, $13);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        withdraw.id,
        withdraw.bounds.lower,
        withdraw.bounds.upper,
        pool_key_hash,
        withdraw.collect_fees,
        withdraw.liquidity,
        withdraw.delta.amount0,
        withdraw.delta.amount1,
        withdraw.recipient,
      ],
    });
  }

  public async insertPositionTransferEvent(
    transfer: TransferEvent,
    key: EventKey
  ) {
    // The `*` operator is the PostgreSQL range intersection operator.
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO position_transfers
                (event_id,
                 token_id,
                 from_address,
                 to_address)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        transfer.id,
        transfer.from,
        transfer.to,
      ],
    });
  }

  public async insertPositionUpdatedEvent(
    event: PositionUpdatedEvent,
    key: EventKey
  ) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO position_updates
                (event_id,
                 locker,
                 pool_key_hash,
                 salt,
                 lower_bound,
                 upper_bound,
                 liquidity_delta,
                 delta0,
                 delta1)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7, $8, $9, $10, $11, $12);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        event.locker,

        pool_key_hash,

        event.params.salt,
        event.params.bounds.lower,
        event.params.bounds.upper,

        event.params.liquidity_delta,
        event.delta.amount0,
        event.delta.amount1,
      ],
    });
  }

  public async insertPositionFeesCollectedEvent(
    event: PositionFeesCollectedEvent,
    key: EventKey
  ) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO position_fees_collected
                (event_id,
                 pool_key_hash,
                 owner,
                 salt,
                 lower_bound,
                 upper_bound,
                 delta0,
                 delta1)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7, $8, $9, $10, $11);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        pool_key_hash,

        event.position_key.owner,
        event.position_key.salt,
        event.position_key.bounds.lower,
        event.position_key.bounds.upper,

        event.delta.amount0,
        event.delta.amount1,
      ],
    });
  }

  public async insertInitializationEvent(
    event: PoolInitializationEvent,
    key: EventKey
  ) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO pool_initializations
                (event_id,
                 pool_key_hash,
                 tick,
                 sqrt_ratio,
                 call_points)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7, $8);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        pool_key_hash,

        event.tick,
        event.sqrt_ratio,
        event.call_points,
      ],
    });
  }

  public async insertProtocolFeesWithdrawn(
    event: FeesWithdrawnEvent,
    key: EventKey
  ) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO protocol_fees_withdrawn
                (event_id,
                 recipient,
                 token,
                 amount)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        event.recipient,
        event.token,
        event.amount,
      ],
    });
  }

  public async insertProtocolFeesPaid(event: FeesPaidEvent, key: EventKey) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO protocol_fees_paid
                (event_id,
                 pool_key_hash,
                 owner,
                 salt,
                 lower_bound,
                 upper_bound,
                 delta0,
                 delta1)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7, $8, $9, $10, $11);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        pool_key_hash,

        event.position_key.owner,
        event.position_key.salt,
        event.position_key.bounds.lower,
        event.position_key.bounds.upper,

        event.delta.amount0,
        event.delta.amount1,
      ],
    });
  }

  public async insertFeesAccumulatedEvent(
    event: FeesAccumulatedEvent,
    key: EventKey
  ) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO fees_accumulated
                (event_id,
                 pool_key_hash,
                 amount0,
                 amount1)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        pool_key_hash,

        event.amount0,
        event.amount1,
      ],
    });
  }

  public async insertRegistration(
    event: TokenRegistrationEvent,
    key: EventKey
  ) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO token_registrations
                (event_id,
                 address,
                 decimals,
                 name,
                 symbol,
                 total_supply)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7, $8, $9);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        event.address,
        event.decimals,
        event.name,
        event.symbol,
        event.total_supply,
      ],
    });
  }

  public async insertSwappedEvent(event: SwappedEvent, key: EventKey) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                        VALUES ($1, $2, $3, $4)
                        RETURNING id)
                INSERT
                INTO swaps
                (event_id,
                 locker,
                 pool_key_hash,
                 delta0,
                 delta1,
                 sqrt_ratio_after,
                 tick_after,
                 liquidity_after)
                VALUES ((SELECT id FROM inserted_event), $5, $6, $7, $8, $9, $10, $11);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        event.locker,
        pool_key_hash,

        event.delta.amount0,
        event.delta.amount1,
        event.sqrt_ratio_after,
        event.tick_after,
        event.liquidity_after,
      ],
    });
  }

  public async deleteOldBlockNumbers(invalidatedBlockNumber: bigint) {
    const { rowCount } = await this.pg.query({
      text: `
                DELETE
                FROM blocks
                WHERE number >= $1;
            `,
      values: [invalidatedBlockNumber],
    });
    return rowCount;
  }
}
