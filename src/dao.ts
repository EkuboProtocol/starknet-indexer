import { Client, PoolClient } from "pg";
import { EventKey, eventKeyToId, ParsedEventWithKey } from "./processor";
import {
  FeesAccumulatedEvent,
  PoolInitializationEvent,
  PoolKey,
  PositionFeesCollectedEvent,
  PositionUpdatedEvent,
  ProtocolFeesPaidEvent,
  ProtocolFeesWithdrawnEvent,
  SwappedEvent,
  TokenRegistrationEvent,
} from "./events/core";
import { TransferEvent } from "./events/nft";
import { 
  computeKeyHash,
  populateCache
} from "./pool_key_hash";
import { PositionMintedWithReferrer } from "./events/positions";
import {
  OrderKey,
  OrderUpdatedEvent,
  OrderProceedsWithdrawnEvent,
  VirtualOrdersExecutedEvent,
} from "./events/twamm";

const ETH_TOKEN_ADDRESS =
  2087021424722619777119509474943472645767659996348769578120564519014510906823n;

const MAX_TICK_SPACING = 354892;

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
    await this.createSchema();
    const cursor = await this.loadCursor();
    // we need to clear anything that was potentially inserted as pending before starting
    if (cursor) {
      await this.deleteOldBlockNumbers(Number(cursor.orderKey) + 1);
    }
    await this.populatePoolKeyCache();
    await this.commitTransaction();
    return cursor;
  }

  private async populatePoolKeyCache() {
    const { rows } = await this.pg.query<{
      key_hash: string;
      token0: string;
      token1: string;
      fee: string;
      tick_spacing: number;
      extension: number;
    }>(`
            SELECT key_hash, token0, token1, fee, tick_spacing, extension
            FROM pool_keys
        `);
    populateCache(
      rows.map(({ token0, token1, key_hash, fee, extension, tick_spacing }) => {
        return {
          pool_key: {
            token0: BigInt(token0),
            token1: BigInt(token1),
            fee: BigInt(fee),
            tick_spacing: BigInt(tick_spacing),
            extension: BigInt(extension),
          },
          hash: BigInt(key_hash),
        };
      })
    );
  }

  private async createSchema(): Promise<void> {
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

        CREATE TABLE IF NOT EXISTS transactions
        (
            transaction_hash NUMERIC NOT NULL PRIMARY KEY,
            sender           NUMERIC NOT NULL
        );

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
        CREATE INDEX IF NOT EXISTS idx_position_updates_locker_salt ON position_updates USING btree (locker, salt);
        CREATE INDEX IF NOT EXISTS idx_position_updates_salt ON position_updates USING btree (salt);

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
        CREATE INDEX IF NOT EXISTS idx_position_fees_collected_salt ON position_fees_collected USING btree (salt);


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
        CREATE INDEX IF NOT EXISTS idx_protocol_fees_paid_salt ON protocol_fees_paid USING btree (salt);

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

        CREATE TABLE IF NOT EXISTS position_minted_with_referrer
        (
            event_id int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            token_id int8    NOT NULL,
            referrer NUMERIC NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_position_minted_with_referrer_token_id ON position_minted_with_referrer USING btree (token_id);

        CREATE TABLE IF NOT EXISTS token_registrations
        (
            event_id     int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            address      NUMERIC NOT NULL,

            name         NUMERIC NOT NULL,
            symbol       NUMERIC NOT NULL,
            decimals     INT     NOT NULL,
            total_supply NUMERIC NOT NULL
        );

        CREATE TABLE IF NOT EXISTS account_class_hashes
        (
            address    NUMERIC NOT NULL PRIMARY KEY,
            class_hash NUMERIC NOT NULL
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

        CREATE TABLE IF NOT EXISTS hourly_volume_by_token
        (
            key_hash   NUMERIC,
            hour       timestamptz,
            token      NUMERIC,
            volume     NUMERIC,
            fees       NUMERIC,
            swap_count NUMERIC,
            PRIMARY KEY (key_hash, hour, token)
        );

        CREATE TABLE IF NOT EXISTS hourly_tvl_delta_by_token
        (
            key_hash NUMERIC,
            hour     timestamptz,
            token    NUMERIC,
            delta    NUMERIC,
            PRIMARY KEY (key_hash, hour, token)
        );

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

        CREATE TABLE IF NOT EXISTS leaderboard
        (
            collector NUMERIC  NOT NULL,
            token_id  BIGINT   NOT NULL,
            category  SMALLINT NOT NULL,
            points    BIGINT   NOT NULL,
            PRIMARY KEY (collector, category, token_id)
        );

        CREATE MATERIALIZED VIEW IF NOT EXISTS leaderboard_materialized_view AS
        (
        WITH earned_points AS (SELECT collector,
                                      SUM(CASE
                                              WHEN class_hash IN (
                                                                  0x01a736d6ed154502257f02b1ccdf4d9d1089f80811cd6acad48e6b6a9d1f2003,
                                                                  0x029927c8af6bccf3f6fda035981e765a7bdbf18a2dc0d630494f8758aa908e2b,
                                                                  0x025ec026985a3bf9d0cc1fe17326b245dfdc3ff89b8fde106542a3ea56c5a918,
                                                                  0x071c3c99f5cf76fc19945d4b8b7d34c7c5528f22730d56192b50c6bbfd338a64,
                                                                  0x0737ee2f87ce571a58c6c8da558ec18a07ceb64a6172d5ec46171fbc80077a48,
                                                                  0x06e150953b26271a740bf2b6e9bca17cc52c68d765f761295de51ceb8526ee72
                                                  ) AND referrer =
                                                        0x064d28d1d1d53a0b5de12e3678699bc9ba32c1cb19ce1c048578581ebb7f8396
                                                  THEN FLOOR(points * 1.2)
                                              ELSE points END) AS points
                               FROM leaderboard
                                        LEFT JOIN position_minted_with_referrer AS pmwr
                                                  ON pmwr.token_id = leaderboard.token_id
                                        LEFT JOIN account_class_hashes
                                                  ON leaderboard.collector = account_class_hashes.address
                               GROUP BY collector),
             referral_points AS (SELECT referrer AS collector, SUM(points / 5) AS points
                                 FROM leaderboard
                                          JOIN position_minted_with_referrer AS pmwr
                                               ON pmwr.token_id = leaderboard.token_id
                                 WHERE referrer != 0
                                 GROUP BY referrer),
             collectors_with_scores
                 AS (SELECT COALESCE(earned_points.collector, referral_points.collector)            AS collector,
                            COALESCE(earned_points.points, 0)                                       AS earned_points,
                            COALESCE(referral_points.points, 0)                                     AS referral_points,
                            COALESCE(earned_points.points, 0) + COALESCE(referral_points.points, 0) AS total_points
                     FROM earned_points
                              FULL OUTER JOIN referral_points ON earned_points.collector = referral_points.collector)
        SELECT collector,
               ROW_NUMBER() OVER (ORDER BY total_points DESC) AS rank,
               earned_points,
               referral_points,
               total_points
        FROM collectors_with_scores
        WHERE total_points != 0
            );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_leaderboard_materialized_view_collector ON leaderboard_materialized_view USING btree (collector);

        CREATE TABLE IF NOT EXISTS twamm_order_updates
        (
            event_id          int8 NOT NULL PRIMARY KEY REFERENCES event_keys (id) ON DELETE CASCADE,

            key_hash          NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            owner             NUMERIC NOT NULL,
            salt              NUMERIC NOT NULL,
            sale_rate_delta0  NUMERIC NOT NULL,
            sale_rate_delta1  NUMERIC NOT NULL,
            start_time        timestamptz NOT NULL,
            end_time          timestamptz NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_twamm_order_updates_key_hash_event_id ON twamm_order_updates USING btree (key_hash, event_id);
        CREATE INDEX IF NOT EXISTS idx_twamm_order_updates_key_hash_time ON twamm_order_updates USING btree (key_hash, start_time, end_time);
        CREATE INDEX IF NOT EXISTS idx_twamm_order_updates_owner_salt ON twamm_order_updates USING btree (owner, salt);
        CREATE INDEX IF NOT EXISTS idx_twamm_order_updates_salt ON twamm_order_updates USING btree (salt);
 
        CREATE TABLE IF NOT EXISTS twamm_proceeds_withdrawals
        (
            event_id    int8 NOT NULL PRIMARY KEY REFERENCES event_keys (id) ON DELETE CASCADE,

            key_hash    NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            owner         NUMERIC NOT NULL,
            salt          NUMERIC NOT NULL,
            amount0       NUMERIC NOT NULL,
            amount1       NUMERIC NOT NULL,
            start_time    timestamptz NOT NULL,
            end_time      timestamptz NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_twamm_proceeds_withdrawals_key_hash_event_id ON twamm_proceeds_withdrawals USING btree (key_hash, event_id);
        CREATE INDEX IF NOT EXISTS idx_twamm_proceeds_withdrawals_key_hash_time ON twamm_proceeds_withdrawals USING btree (key_hash, start_time, end_time);
        CREATE INDEX IF NOT EXISTS idx_twamm_proceeds_withdrawals_owner_salt ON twamm_proceeds_withdrawals USING btree (owner, salt);
        CREATE INDEX IF NOT EXISTS idx_twamm_proceeds_withdrawals_salt ON twamm_proceeds_withdrawals USING btree (salt);
 
        CREATE TABLE IF NOT EXISTS twamm_virtual_order_executions
        (
            event_id          int8 NOT NULL PRIMARY KEY REFERENCES event_keys (id) ON DELETE CASCADE,
            
            key_hash          NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            token0_sale_rate  NUMERIC NOT NULL,
            token1_sale_rate  NUMERIC NOT NULL,
            delta0            NUMERIC NOT NULL,
            delta1            NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_twamm_virtual_order_executions_pool_key_hash_event_id ON twamm_virtual_order_executions USING btree (key_hash, event_id DESC);

        CREATE OR REPLACE VIEW twamm_pool_states_view AS (
          WITH lvoe AS (
              SELECT 
                  key_hash,
                  number as block_number,
                  COALESCE(last_virtual_order_execution.token0_sale_rate, 0) AS token0_sale_rate,
                  COALESCE(last_virtual_order_execution.token1_sale_rate, 0) AS token1_sale_rate,
                  block_time 
              FROM
                  pool_keys
                  INNER JOIN LATERAL (
                      SELECT 
                          event_id, 
                          token0_sale_rate, 
                          token1_sale_rate
                      FROM 
                          twamm_virtual_order_executions
                      WHERE 
                          pool_keys.key_hash = twamm_virtual_order_executions.key_hash
                      ORDER BY 
                          event_id DESC
                      LIMIT 1
                  ) AS last_virtual_order_execution ON TRUE
                  LEFT JOIN LATERAL (
                      SELECT
                          number,
                          time as block_time
                      FROM
                          blocks
                      WHERE
                          (
                          SELECT
                              block_number
                          FROM
                              event_keys
                          WHERE
                              id = last_virtual_order_execution.event_id
                          LIMIT
                              1
                          ) = number
                      LIMIT
                          1
                  ) AS block on TRUE
          ),
          ou AS (
              SELECT 
                  lvoe.key_hash,
                  SUM(tou.sale_rate_delta0) AS sale_rate_delta0,
                  SUM(tou.sale_rate_delta1) AS sale_rate_delta1
              FROM 
                  lvoe
                  INNER JOIN (
                      SELECT 
                          key_hash,
                          sale_rate_delta0,
                          sale_rate_delta1,
                          block_number,
                          end_time
                      FROM twamm_order_updates 
                      LEFT JOIN event_keys on event_id = id
                  ) AS tou ON
                      lvoe.key_hash = tou.key_hash AND
                      lvoe.block_number = tou.block_number AND
                      end_time > lvoe.block_time
              GROUP BY lvoe.key_hash
          )
          SELECT
              lvoe.key_hash,
              lvoe.block_time,
              lvoe.token0_sale_rate + COALESCE(ou.sale_rate_delta0, 0) AS token0_sale_rate,
              lvoe.token1_sale_rate + COALESCE(ou.sale_rate_delta1, 0) AS token1_sale_rate
          FROM
              lvoe
              LEFT JOIN ou ON lvoe.key_hash = ou.key_hash
        );

        CREATE MATERIALIZED VIEW IF NOT EXISTS twamm_pool_states_materialized AS
          (
            SELECT 
              key_hash,
              block_time,
              token0_sale_rate,
              token1_sale_rate
            FROM
              twamm_pool_states_view
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_twamm_pool_states_materialized_key_hash ON twamm_pool_states_materialized USING btree (key_hash);
    `);
  }

  public async refreshAnalyticalTables({ since }: { since: Date }) {
    await Promise.all([
      this.pg.query({
        text: `
            INSERT INTO hourly_volume_by_token
                (SELECT swaps.pool_key_hash                                                      AS   key_hash,
                        DATE_TRUNC('hour', blocks.time)                                          AS   hour,
                        (CASE WHEN swaps.delta0 >= 0 THEN pool_keys.token0 ELSE pool_keys.token1 END) token,
                        SUM(CASE WHEN swaps.delta0 >= 0 THEN swaps.delta0 ELSE swaps.delta1 END) AS   volume,
                        SUM(FLOOR(((CASE WHEN delta0 >= 0 THEN swaps.delta0 ELSE swaps.delta1 END) * pool_keys.fee) /
                                  340282366920938463463374607431768211456))                      AS   fees,
                        COUNT(1)                                                                 AS   swap_count
                 FROM swaps
                          JOIN pool_keys ON swaps.pool_key_hash = pool_keys.key_hash
                          JOIN event_keys ON swaps.event_id = event_keys.id
                          JOIN blocks ON event_keys.block_number = blocks.number
                 WHERE DATE_TRUNC('hour', blocks.time) >= DATE_TRUNC('hour', $1::timestamptz)
                 GROUP BY hour, swaps.pool_key_hash, token)
            ON CONFLICT (key_hash, hour, token)
                DO UPDATE SET volume     = excluded.volume,
                              fees       = excluded.fees,
                              swap_count = excluded.swap_count;
        `,
        values: [since],
      }),

      this.pg.query({
        text: `
            INSERT INTO hourly_tvl_delta_by_token
                (WITH first_event_id AS (SELECT id
                                         FROM event_keys AS ek
                                                  JOIN blocks AS b ON ek.block_number = b.number
                                         WHERE b.time >= DATE_TRUNC('hour', $1::timestamptz)
                                         LIMIT 1),
                      grouped_pool_key_hash_deltas AS (SELECT pool_key_hash,
                                                              DATE_TRUNC('hour', blocks.time) AS hour,
                                                              SUM(delta0)                     AS delta0,
                                                              SUM(delta1)                     AS delta1
                                                       FROM swaps
                                                                JOIN event_keys ON swaps.event_id = event_keys.id
                                                                JOIN blocks ON event_keys.block_number = blocks.number
                                                       WHERE event_id >= (SELECT id FROM first_event_id)
                                                       GROUP BY pool_key_hash, DATE_TRUNC('hour', blocks.time)

                                                       UNION ALL

                                                       SELECT pool_key_hash,
                                                              DATE_TRUNC('hour', blocks.time) AS hour,
                                                              SUM(delta0)                     AS delta0,
                                                              SUM(delta1)                     AS delta1
                                                       FROM position_updates
                                                                JOIN event_keys ON position_updates.event_id = event_keys.id
                                                                JOIN blocks ON event_keys.block_number = blocks.number
                                                       WHERE event_id >= (SELECT id FROM first_event_id)
                                                       GROUP BY pool_key_hash, DATE_TRUNC('hour', blocks.time)

                                                       UNION ALL

                                                       SELECT pool_key_hash,
                                                              DATE_TRUNC('hour', blocks.time) AS hour,
                                                              SUM(delta0)                     AS delta0,
                                                              SUM(delta1)                     AS delta1
                                                       FROM position_fees_collected
                                                                JOIN event_keys ON position_fees_collected.event_id = event_keys.id
                                                                JOIN blocks ON event_keys.block_number = blocks.number
                                                       WHERE event_id >= (SELECT id FROM first_event_id)
                                                       GROUP BY pool_key_hash, DATE_TRUNC('hour', blocks.time)

                                                       UNION ALL

                                                       SELECT pool_key_hash,
                                                              DATE_TRUNC('hour', blocks.time) AS hour,
                                                              SUM(delta0)                     AS delta0,
                                                              SUM(delta1)                     AS delta1
                                                       FROM protocol_fees_paid
                                                                JOIN event_keys ON protocol_fees_paid.event_id = event_keys.id
                                                                JOIN blocks ON event_keys.block_number = blocks.number
                                                       WHERE event_id >= (SELECT id FROM first_event_id)
                                                       GROUP BY pool_key_hash, DATE_TRUNC('hour', blocks.time)

                                                       UNION ALL

                                                       SELECT pool_key_hash,
                                                              DATE_TRUNC('hour', blocks.time) AS hour,
                                                              SUM(amount0)                    AS delta0,
                                                              SUM(amount1)                    AS delta1
                                                       FROM fees_accumulated
                                                                JOIN event_keys ON fees_accumulated.event_id = event_keys.id
                                                                JOIN blocks ON event_keys.block_number = blocks.number
                                                       WHERE event_id >= (SELECT id FROM first_event_id)
                                                       GROUP BY pool_key_hash, DATE_TRUNC('hour', blocks.time)),
                      token_deltas AS (SELECT pool_key_hash,
                                              grouped_pool_key_hash_deltas.hour,
                                              pool_keys.token0 AS token,
                                              SUM(delta0)      AS delta
                                       FROM grouped_pool_key_hash_deltas
                                                JOIN pool_keys
                                                     ON pool_keys.key_hash = grouped_pool_key_hash_deltas.pool_key_hash
                                       GROUP BY pool_key_hash, grouped_pool_key_hash_deltas.hour, pool_keys.token0

                                       UNION ALL

                                       SELECT pool_key_hash,
                                              grouped_pool_key_hash_deltas.hour,
                                              pool_keys.token1 AS token,
                                              SUM(delta1)      AS delta
                                       FROM grouped_pool_key_hash_deltas
                                                JOIN pool_keys
                                                     ON pool_keys.key_hash = grouped_pool_key_hash_deltas.pool_key_hash
                                       GROUP BY pool_key_hash, grouped_pool_key_hash_deltas.hour, pool_keys.token1)
                 SELECT pool_key_hash AS key_hash,
                        token_deltas.hour,
                        token_deltas.token,
                        SUM(delta)    AS delta
                 FROM token_deltas
                 GROUP BY token_deltas.pool_key_hash, token_deltas.hour, token_deltas.token)
            ON CONFLICT (key_hash, hour, token)
                DO UPDATE SET delta = excluded.delta;
        `,
        values: [since],
      }),
    ]);
  }

  public async refreshOperationalMaterializedView() {
    await this.pg.query(`
      REFRESH MATERIALIZED VIEW CONCURRENTLY per_pool_per_tick_liquidity_materialized;
      REFRESH MATERIALIZED VIEW CONCURRENTLY pool_states_materialized;
      REFRESH MATERIALIZED VIEW CONCURRENTLY twamm_pool_states_materialized;
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
      text: `INSERT INTO blocks (number, hash, time)
                   VALUES ($1, $2, $3);`,
      values: [number, hash, time],
    });
  }

  private async batchInsertFakeEventKeys(keys: EventKey[]) {
    await this.pg.query({
      text: `
                INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                SELECT *
                FROM UNNEST($1::INT[], $2::SMALLINT[], $3::SMALLINT[], $4::NUMERIC[])
            `,
      values: [
        keys.map((k) => k.blockNumber),
        keys.map((k) => k.transactionIndex),
        keys.map((k) => k.eventIndex),
        Array(keys.length).fill(0),
      ],
    });
  }

  public async batchInsertFakeFeeEvents(
    tableName: "position_fees_collected" | "protocol_fees_paid",
    owner: BigInt,
    events:
      | ParsedEventWithKey<PositionFeesCollectedEvent>[]
      | ParsedEventWithKey<ProtocolFeesPaidEvent>[]
  ) {
    await this.batchInsertFakeEventKeys(events.map((e) => e.key));
    await this.pg.query({
      text: `
                INSERT INTO ${tableName} (event_id, pool_key_hash, owner, salt, lower_bound, upper_bound, delta0,
                                          delta1)
                SELECT *
                FROM UNNEST($1::BIGINT[], $2::NUMERIC[], $3::NUMERIC[], $4::NUMERIC[], $5::INT[], $6::INT[],
                            $7::NUMERIC[], $8::NUMERIC[])
            `,
      values: [
        events.map((e) => eventKeyToId(e.key)),
        events.map((e) => computeKeyHash(e.parsed.pool_key)),
        events.map(() => owner),
        events.map((e) => e.parsed.position_key.salt),
        events.map((e) => e.parsed.position_key.bounds.lower),
        events.map((e) => e.parsed.position_key.bounds.upper),
        events.map((e) => e.parsed.delta.amount0),
        events.map((e) => e.parsed.delta.amount1),
      ],
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

  public async insertPositionMintedWithReferrerEvent(
    minted: PositionMintedWithReferrer,
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
                INTO position_minted_with_referrer
                (event_id,
                 token_id,
                 referrer)
                VALUES ((SELECT id FROM inserted_event), $5, $6)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        minted.id,
        minted.referrer,
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
      name: "insert-position-fees-collected",
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
    event: ProtocolFeesWithdrawnEvent,
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

  public async insertProtocolFeesPaid(
    event: ProtocolFeesPaidEvent,
    key: EventKey
  ) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      name: "insert-protocol-fees-paid",
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

  public async refreshLeaderboard(maxEventIdExclusive: bigint) {
    await this.pg.query(`
        DELETE
        FROM leaderboard
        WHERE TRUE;
        INSERT INTO leaderboard(WITH all_tokens AS (SELECT token0 AS token
                                                    FROM pool_keys
                                                    UNION
                                                    DISTINCT
                                                    SELECT token1
                                                    FROM pool_keys),

                                     pair_swap_counts_by_day
                                         AS (SELECT s.pool_key_hash,
                                                    date_bin(INTERVAL '1 day', b.time, '2000-01-01') AS day,
                                                    COUNT(DISTINCT transaction_hash)                 AS swap_count
                                             FROM swaps AS s
                                                      JOIN event_keys AS ek ON s.event_id = ek.id
                                                      JOIN blocks AS b ON ek.block_number = b.number
                                             WHERE s.event_id < ${maxEventIdExclusive}
                                               AND s.delta0 != 0
                                               AND s.delta1 != 0
                                             GROUP BY s.pool_key_hash, day),

                                     pool_key_num_depositors_multiplier AS (SELECT pool_key_hash,
                                                                                   (4::NUMERIC / (1 + EXP(-0.001 * COUNT(DISTINCT pt.to_address)))) - 2 AS multiplier
                                                                            FROM position_transfers AS pt
                                                                                     JOIN position_updates AS pu ON pt.token_id::NUMERIC = pu.salt
                                                                            WHERE pt.from_address = 0
                                                                            GROUP BY pu.pool_key_hash),

                                     swap_counts_as_t0 AS (SELECT token0 AS token, SUM(swap_count) AS swap_count
                                                           FROM pair_swap_counts_by_day AS pscbd
                                                                    JOIN pool_keys AS pk ON pscbd.pool_key_hash = pk.key_hash
                                                           WHERE day >= (NOW() - INTERVAL '30 days')
                                                           GROUP BY token0),

                                     swap_counts_as_t1 AS (SELECT token1 AS token, SUM(swap_count) AS swap_count
                                                           FROM pair_swap_counts_by_day AS pscbd
                                                                    JOIN pool_keys AS pk ON pscbd.pool_key_hash = pk.key_hash
                                                           WHERE day >= (NOW() - INTERVAL '30 days')
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
                                     pair_swap_points_boost AS (SELECT pool_key_hash,
                                                                       ((20::NUMERIC / (1 + EXP(-0.0001 * SUM(swap_count)))) - 10) AS multiplier
                                                                FROM pair_swap_counts_by_day
                                                                GROUP BY pool_key_hash),

                                     fee_to_discount_factor AS (SELECT DISTINCT fee,
                                                                                1 - SQRT(fee / 340282366920938463463374607431768211456) AS fee_discount
                                                                FROM pool_keys),

                                     -- we compute the VWAP price in eth per token over the last month for each token we will consider
                                     token_points_rates AS
                                         (SELECT token,
                                                 (CASE
                                                      WHEN swap_count < 4000 THEN 0
                                                      WHEN token =
                                                           ${ETH_TOKEN_ADDRESS}
                                                          THEN 1
                                                      WHEN token <
                                                           ${ETH_TOKEN_ADDRESS}
                                                          THEN (SELECT SUM(delta1 * delta1) / SUM(ABS(delta0 * delta1))
                                                                FROM swaps
                                                                         JOIN pool_keys ON swaps.pool_key_hash = pool_keys.key_hash
                                                                         JOIN event_keys ON swaps.event_id = event_keys.id
                                                                         JOIN blocks ON event_keys.block_number = blocks.number
                                                                WHERE token0 = token
                                                                  AND token1 =
                                                                      ${ETH_TOKEN_ADDRESS}
                                                                  AND blocks.time >= NOW() - INTERVAL '1 month'
                                                                  AND swaps.event_id < ${maxEventIdExclusive})
                                                      ELSE
                                                          (SELECT SUM(ABS(delta0 * delta1)) / SUM(delta1 * delta1)
                                                           FROM swaps
                                                                    JOIN pool_keys ON swaps.pool_key_hash = pool_keys.key_hash
                                                                    JOIN event_keys ON swaps.event_id = event_keys.id
                                                                    JOIN blocks ON event_keys.block_number = blocks.number
                                                           WHERE token0 =
                                                                 ${ETH_TOKEN_ADDRESS}
                                                             AND token1 = token
                                                             AND blocks.time >= NOW() - INTERVAL '1 month'
                                                             AND swaps.event_id < ${maxEventIdExclusive})
                                                     END) AS rate
                                          FROM all_tokens_with_swap_counts),

                                     position_multipliers AS (SELECT pt.token_id AS token_id,
                                                                     2 *
                                                                     EXP(GREATEST((pmb.time::DATE - '2023-09-14'::DATE), 0) * -0.01) +
                                                                     1           AS multiplier
                                                              FROM position_transfers AS pt
                                                                       JOIN event_keys ON pt.event_id = event_keys.id
                                                                       JOIN blocks AS pmb ON event_keys.block_number = pmb.number
                                                              WHERE pt.from_address = 0),

                                     points_from_mints AS (SELECT pt.token_id                    AS token_id,
                                                                  to_address                     AS collector,
                                                                  ((CASE
                                                                        WHEN EXISTS (SELECT 1
                                                                                     FROM position_updates AS pu
                                                                                     WHERE pu.salt = pt.token_id::NUMERIC
                                                                                       AND pu.delta0 != 0
                                                                                       AND pu.delta1 != 0) THEN 2000
                                                                        ELSE 0 END) *
                                                                   multipliers.multiplier)::int8 AS points
                                                           FROM position_transfers AS pt
                                                                    JOIN event_keys AS ptek ON pt.event_id = ptek.id
                                                                    JOIN position_multipliers AS multipliers
                                                                         ON pt.token_id = multipliers.token_id
                                                                    JOIN blocks AS ptb ON ptek.block_number = ptb.number
                                                           WHERE pt.from_address = 0
                                                             AND pt.event_id < ${maxEventIdExclusive}),

                                     points_from_withdrawal_fees_paid AS (SELECT multipliers.token_id       AS token_id,
                                                                                 (SELECT to_address
                                                                                  FROM position_transfers AS pt
                                                                                  WHERE pfp.salt::BIGINT = pt.token_id
                                                                                    AND pt.event_id < pfp.event_id
                                                                                  ORDER BY pt.event_id DESC
                                                                                  LIMIT 1)                  AS collector,
                                                                                 FLOOR(ABS(
                                                                                               (pfp.delta0 * tp0.rate * fd.fee_discount) +
                                                                                               (pfp.delta1 * tp1.rate * fd.fee_discount)
                                                                                       ) * multipliers.multiplier /
                                                                                       1e12::NUMERIC)::int8 AS points
                                                                          FROM position_multipliers AS multipliers
                                                                                   JOIN protocol_fees_paid AS pfp
                                                                                        ON pfp.salt =
                                                                                           multipliers.token_id::NUMERIC AND
                                                                                           pfp.event_id <
                                                                                           ${maxEventIdExclusive}
                                                                                   JOIN event_keys AS pfpek ON pfp.event_id = pfpek.id
                                                                                   JOIN blocks AS pfpb
                                                                                        ON pfpek.block_number = pfpb.number
                                                                                   JOIN pool_keys AS pk ON pfp.pool_key_hash = pk.key_hash
                                                                                   JOIN token_points_rates AS tp0 ON tp0.token = pk.token0
                                                                                   JOIN token_points_rates AS tp1 ON tp1.token = pk.token1
                                                                                   JOIN fee_to_discount_factor AS fd ON pk.fee = fd.fee),

                                     points_from_fees AS (SELECT position_id_multiplier.token_id AS token_id,
                                                                 (SELECT to_address
                                                                  FROM position_transfers AS pt
                                                                  WHERE pt.token_id = pfc.salt::BIGINT
                                                                    AND pt.event_id < pfc.event_id
                                                                  ORDER BY pt.event_id DESC
                                                                  LIMIT 1)                       AS collector,
                                                                 FLOOR(ABS(
                                                                               (pfc.delta0 * tp0.rate * fd.fee_discount) +
                                                                               (pfc.delta1 * tp1.rate * fd.fee_discount)
                                                                       ) * position_id_multiplier.multiplier *
                                                                       ppb.multiplier *
                                                                       ppd.multiplier /
                                                                       1e12::NUMERIC)::int8      AS points
                                                          FROM position_multipliers AS position_id_multiplier
                                                                   JOIN position_fees_collected AS pfc
                                                                        ON pfc.salt =
                                                                           position_id_multiplier.token_id::NUMERIC AND
                                                                           pfc.event_id <
                                                                           ${maxEventIdExclusive}
                                                                   JOIN event_keys AS pfek ON pfc.event_id = pfek.id
                                                                   JOIN blocks AS pfb ON pfek.block_number = pfb.number
                                                                   JOIN pool_keys AS pk ON pfc.pool_key_hash = pk.key_hash
                                                                   JOIN pair_swap_points_boost AS ppb
                                                                        ON pfc.pool_key_hash = ppb.pool_key_hash
                                                                   JOIN pool_key_num_depositors_multiplier AS ppd
                                                                        ON pfc.pool_key_hash = ppd.pool_key_hash
                                                                   JOIN fee_to_discount_factor AS fd ON pk.fee = fd.fee
                                                                   JOIN token_points_rates AS tp0 ON tp0.token = pk.token0
                                                                   JOIN token_points_rates AS tp1 ON tp1.token = pk.token1),

                                     points_by_collector_and_token_id
                                         AS (SELECT token_id, collector, 0 AS category, points
                                             FROM points_from_fees
                                             UNION ALL
                                             SELECT token_id, collector, 1 AS category, points
                                             FROM points_from_mints
                                             UNION ALL
                                             SELECT token_id, collector, 2 AS category, points
                                             FROM points_from_withdrawal_fees_paid)

                                SELECT collector, token_id, category, SUM(points) AS points
                                FROM points_by_collector_and_token_id
                                GROUP BY collector, token_id, category
                                ORDER BY points DESC);
    `);

    await this.pg.query(
      `REFRESH MATERIALIZED VIEW CONCURRENTLY leaderboard_materialized_view;`
    );
  }

  public deleteFakeEvents(blockNumber: number) {
    return this.pg.query({
      text: `DELETE
                   FROM event_keys
                   WHERE block_number = $1
                     AND transaction_hash = 0`,
      values: [blockNumber],
    });
  }

  /**
   * Deletes all the blocks equal to or greater than the given block number, cascades to all the other tables.
   * @param invalidatedBlockNumber the block number for which data in the database should be removed
   */
  public async deleteOldBlockNumbers(invalidatedBlockNumber: number) {
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

  public async insertAccountClassHashes(
    items: {
      account: string;
      class_hash: string;
    }[]
  ) {
    await this.pg.query({
      text: `INSERT INTO account_class_hashes (address, class_hash)
             SELECT *
             FROM UNNEST($1::NUMERIC[], $2::NUMERIC[])
             ON CONFLICT (address)
                 DO UPDATE SET class_hash = excluded.class_hash;`,
      values: [items.map((i) => i.account), items.map((i) => i.class_hash)],
    });
  }

  public async writeTransactionSenders(
    transactionSenders: [transactionHash: string, sender: string][]
  ) {
    if (transactionSenders.length > 0) {
      await this.pg.query({
        text: `INSERT INTO transactions (transaction_hash, sender)
               SELECT *
               FROM UNNEST($1::NUMERIC[], $2::NUMERIC[])
               ON CONFLICT DO NOTHING`,
        values: [
          transactionSenders.map(([hash]) => hash),
          transactionSenders.map(([, sender]) => sender),
        ],
      });
    }
  }

  public async insertTWAMMOrderUpdatedEvent(
    order_updated: OrderUpdatedEvent,
    key: EventKey
  ) {
    const { order_key } = order_updated;

    const key_hash = await this.insertPoolKeyHash(this.orderKeyToPoolKey(order_key));

    const [sale_rate_delta0, sale_rate_delta1] = order_key.sell_token > order_key.buy_token ?
      [0, order_updated.sale_rate_delta] : [order_updated.sale_rate_delta, 0]

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys 
                        (block_number, transaction_index, event_index, transaction_hash)
                    VALUES 
                        ($1, $2, $3, $4)
                    RETURNING id
                )
                INSERT INTO twamm_order_updates
                    (
                        event_id,
                        key_hash,
                        owner,
                        salt,
                        sale_rate_delta0,
                        sale_rate_delta1,
                        start_time,
                        end_time
                    )
                VALUES 
                    (
                        (SELECT id FROM inserted_event), $5, $6, $7, $8, $9, $10, $11
                    );
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        key_hash,

        BigInt(order_updated.owner),
        order_updated.salt,
        sale_rate_delta0,
        sale_rate_delta1,
        new Date(
          Number(order_key.start_time * 1000n)
        ),
        new Date(
          Number(order_key.end_time * 1000n)
        ),
      ],
    });
  }

  public async insertTWAMMOrderProceedsWithdrawnEvent(
    order_proceeds_withdrawn: OrderProceedsWithdrawnEvent,
    key: EventKey
  ) {
    const { order_key } = order_proceeds_withdrawn;

    const key_hash = await this.insertPoolKeyHash(this.orderKeyToPoolKey(order_key));

    const [amount0, amount1] = order_key.sell_token > order_key.buy_token ?
      [0, order_proceeds_withdrawn.amount] : [order_proceeds_withdrawn.amount, 0]

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash)
                    VALUES ($1, $2, $3, $4)
                    RETURNING id
                )
                INSERT INTO twamm_proceeds_withdrawals
                    (event_id, key_hash, owner, salt, amount0, amount1, start_time, end_time)
                VALUES
                    ((SELECT id FROM inserted_event), $5, $6, $7, $8, $9, $10, $11);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        key_hash,

        BigInt(order_proceeds_withdrawn.owner),
        order_proceeds_withdrawn.salt,
        amount0,
        amount1,
        new Date(
          Number(order_key.start_time * 1000n)
        ),
        new Date(
          Number(order_key.end_time * 1000n)
        )
      ],
    });
  }

  public async insertTWAMMVirtualOrdersExecutedEvent(
    virtual_orders_executed: VirtualOrdersExecutedEvent,
    key: EventKey
  ) {
    let { key: state_key } = virtual_orders_executed;

    const key_hash = await this.insertPoolKeyHash({
      token0: state_key.token0,
      token1: state_key.token1,
      fee: state_key.fee,
      tick_spacing: BigInt(MAX_TICK_SPACING),
      extension: BigInt(process.env.TWAMM_ADDRESS)
    });

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys 
                        (block_number, transaction_index, event_index, transaction_hash)
                    VALUES 
                        ($1, $2, $3, $4)
                    RETURNING id
                )
                INSERT INTO twamm_virtual_order_executions
                    (event_id, key_hash, token0_sale_rate, token1_sale_rate, delta0, delta1)
                VALUES
                    ((SELECT id FROM inserted_event), $5, $6, $7, $8, $9);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        key_hash,
        virtual_orders_executed.token0_sale_rate,
        virtual_orders_executed.token1_sale_rate,
        virtual_orders_executed.twamm_delta.amount0,
        virtual_orders_executed.twamm_delta.amount1
      ],
    });
  }

  private orderKeyToPoolKey(order_key: OrderKey): PoolKey {
    const [token0, token1]: [bigint, bigint] = order_key.buy_token > order_key.sell_token ? 
      [order_key.sell_token, order_key.buy_token] :
      [order_key.buy_token, order_key.sell_token];

    return {
      token0,
      token1,
      fee: order_key.fee,
      tick_spacing: BigInt(MAX_TICK_SPACING),
      extension: BigInt(process.env.TWAMM_ADDRESS)
    };
  }

}
