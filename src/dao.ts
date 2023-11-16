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
            id         INT     NOT NULL UNIQUE CHECK (id = 1), -- only one row.
            order_key  NUMERIC NOT NULL,
            unique_key TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS blocks
        (
            number    int8      NOT NULL PRIMARY KEY,
            hash      NUMERIC   NOT NULL,
            timestamp TIMESTAMP NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks USING btree (timestamp);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_blocks_hash ON blocks USING btree (hash);

        CREATE TABLE IF NOT EXISTS pool_keys
        (
            key_hash     NUMERIC NOT NULL PRIMARY KEY,
            token0       NUMERIC NOT NULL,
            token1       NUMERIC NOT NULL,
            fee          NUMERIC NOT NULL,
            tick_spacing NUMERIC NOT NULL,
            extension    NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pool_keys_token0 ON pool_keys USING btree (token0);
        CREATE INDEX IF NOT EXISTS idx_pool_keys_token1 ON pool_keys USING btree (token1);
        CREATE INDEX IF NOT EXISTS idx_pool_keys_token0_token1 ON pool_keys USING btree (token0, token1);
        CREATE INDEX IF NOT EXISTS idx_pool_keys_extension ON pool_keys USING btree (extension);

        CREATE TABLE IF NOT EXISTS position_minted
        (
            block_number      int8    NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int4    NOT NULL,
            event_index       int4    NOT NULL,

            transaction_hash  NUMERIC NOT NULL,

            token_id          int8    NOT NULL,
            lower_bound       int4    NOT NULL,
            upper_bound       int4    NOT NULL,

            pool_key_hash     NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            PRIMARY KEY (block_number, transaction_index, event_index)
        );
        CREATE INDEX IF NOT EXISTS idx_position_minted_pool_key_hash ON position_minted (pool_key_hash);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_position_minted_token_id ON position_minted (token_id);

        CREATE TABLE IF NOT EXISTS position_deposit
        (
            block_number      int8    NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int4    NOT NULL,
            event_index       int4    NOT NULL,

            transaction_hash  NUMERIC NOT NULL,

            token_id          int8    NOT NULL,
            lower_bound       int4    NOT NULL,
            upper_bound       int4    NOT NULL,

            pool_key_hash     NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            liquidity         NUMERIC NOT NULL,
            delta0            NUMERIC NOT NULL,
            delta1            NUMERIC NOT NULL,

            PRIMARY KEY (block_number, transaction_index, event_index)
        );
        CREATE INDEX IF NOT EXISTS idx_position_deposit_pool_key_hash ON position_deposit (pool_key_hash);
        CREATE INDEX IF NOT EXISTS idx_position_deposit_token_id ON position_deposit (token_id);

        CREATE TABLE IF NOT EXISTS position_withdraw
        (
            block_number      int8    NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int4    NOT NULL,
            event_index       int4    NOT NULL,

            transaction_hash  NUMERIC NOT NULL,

            token_id          int8    NOT NULL,
            lower_bound       int4    NOT NULL,
            upper_bound       int4    NOT NULL,

            pool_key_hash     NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            collect_fees      bool    NOT NULL,

            liquidity         NUMERIC NOT NULL,
            delta0            NUMERIC NOT NULL,
            delta1            NUMERIC NOT NULL,

            recipient         NUMERIC NOT NULL,

            PRIMARY KEY (block_number, transaction_index, event_index)
        );
        CREATE INDEX IF NOT EXISTS idx_position_withdraw_pool_key_hash ON position_withdraw (pool_key_hash);
        CREATE INDEX IF NOT EXISTS idx_position_withdraw_token_id ON position_withdraw (token_id);

        CREATE TABLE IF NOT EXISTS position_transfers
        (
            block_number      int8    NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int4    NOT NULL,
            event_index       int4    NOT NULL,

            transaction_hash  NUMERIC NOT NULL,

            token_id          int8    NOT NULL,
            from_address      NUMERIC NOT NULL,
            to_address        NUMERIC NOT NULL,

            PRIMARY KEY (block_number, transaction_index, event_index)
        );
        CREATE INDEX IF NOT EXISTS idx_position_transfers_token_id ON position_transfers (token_id);
        CREATE INDEX IF NOT EXISTS idx_position_transfers_from_address ON position_transfers (from_address);
        CREATE INDEX IF NOT EXISTS idx_position_transfers_to_address ON position_transfers (to_address);

        CREATE TABLE IF NOT EXISTS position_updates
        (
            block_number      int8    NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int4    NOT NULL,
            event_index       int4    NOT NULL,

            transaction_hash  NUMERIC NOT NULL,

            locker            NUMERIC NOT NULL,

            pool_key_hash     NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            salt              NUMERIC NOT NULL,
            lower_bound       int4    NOT NULL,
            upper_bound       int4    NOT NULL,

            liquidity_delta   NUMERIC NOT NULL,
            delta0            NUMERIC NOT NULL,
            delta1            NUMERIC NOT NULL,

            PRIMARY KEY (block_number, transaction_index, event_index)
        );
        CREATE INDEX IF NOT EXISTS idx_position_updates_pool_key_hash ON position_updates (pool_key_hash);
        CREATE INDEX IF NOT EXISTS idx_position_updates_pool_key_hash_sorted_events ON position_updates USING btree (pool_key_hash, block_number, transaction_index, event_index);

        CREATE TABLE IF NOT EXISTS position_fees_collected
        (
            block_number      int8    NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int4    NOT NULL,
            event_index       int4    NOT NULL,

            transaction_hash  NUMERIC NOT NULL,

            pool_key_hash     NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            owner             NUMERIC NOT NULL,
            salt              NUMERIC NOT NULL,
            lower_bound       int4    NOT NULL,
            upper_bound       int4    NOT NULL,

            delta0            NUMERIC NOT NULL,
            delta1            NUMERIC NOT NULL,

            PRIMARY KEY (block_number, transaction_index, event_index)
        );
        CREATE INDEX IF NOT EXISTS idx_position_fees_collected_pool_key_hash ON position_fees_collected (pool_key_hash);

        CREATE TABLE IF NOT EXISTS protocol_fees_withdrawn
        (
            block_number      int8    NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int4    NOT NULL,
            event_index       int4    NOT NULL,

            transaction_hash  NUMERIC NOT NULL,

            recipient         NUMERIC NOT NULL,
            token             NUMERIC NOT NULL,
            amount            NUMERIC NOT NULL,

            PRIMARY KEY (block_number, transaction_index, event_index)
        );


        CREATE TABLE IF NOT EXISTS protocol_fees_paid
        (
            block_number      int8    NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int4    NOT NULL,
            event_index       int4    NOT NULL,

            transaction_hash  NUMERIC NOT NULL,

            pool_key_hash     NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            owner             NUMERIC NOT NULL,
            salt              NUMERIC NOT NULL,
            lower_bound       int4    NOT NULL,
            upper_bound       int4    NOT NULL,

            delta0            NUMERIC NOT NULL,
            delta1            NUMERIC NOT NULL,

            PRIMARY KEY (block_number, transaction_index, event_index)
        );
        CREATE INDEX IF NOT EXISTS idx_protocol_fees_paid_pool_key_hash ON protocol_fees_paid (pool_key_hash);

        CREATE TABLE IF NOT EXISTS fees_accumulated
        (
            block_number      int8    NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int4    NOT NULL,
            event_index       int4    NOT NULL,

            transaction_hash  NUMERIC NOT NULL,

            pool_key_hash     NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            amount0           NUMERIC NOT NULL,
            amount1           NUMERIC NOT NULL,

            PRIMARY KEY (block_number, transaction_index, event_index)
        );
        CREATE INDEX IF NOT EXISTS idx_fees_accumulated_pool_key_hash ON fees_accumulated (pool_key_hash);

        CREATE TABLE IF NOT EXISTS pool_initializations
        (
            block_number      int8     NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int4     NOT NULL,
            event_index       int4     NOT NULL,

            transaction_hash  NUMERIC  NOT NULL,

            pool_key_hash     NUMERIC  NOT NULL REFERENCES pool_keys (key_hash),

            tick              int4     NOT NULL,
            sqrt_ratio        NUMERIC  NOT NULL,
            call_points       SMALLINT NOT NULL,

            PRIMARY KEY (block_number, transaction_index, event_index)
        );
        CREATE INDEX IF NOT EXISTS idx_pool_initializations_pool_key_hash ON pool_initializations (pool_key_hash);


        CREATE TABLE IF NOT EXISTS swaps
        (
            block_number      int8    NOT NULL REFERENCES blocks (number) ON DELETE CASCADE,
            transaction_index int4    NOT NULL,
            event_index       int4    NOT NULL,

            transaction_hash  NUMERIC NOT NULL,

            locker            NUMERIC NOT NULL,
            pool_key_hash     NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            delta0            NUMERIC NOT NULL,
            delta1            NUMERIC NOT NULL,

            sqrt_ratio_after  NUMERIC NOT NULL,
            tick_after        int4    NOT NULL,
            liquidity_after   NUMERIC NOT NULL,

            PRIMARY KEY (block_number, transaction_index, event_index)
        );
        CREATE INDEX IF NOT EXISTS idx_swaps_pool_key_hash ON swaps (pool_key_hash);
        CREATE INDEX IF NOT EXISTS idx_swaps_pool_key_hash_sorted_events ON swaps USING btree (pool_key_hash, block_number, transaction_index, event_index);

        CREATE MATERIALIZED VIEW IF NOT EXISTS volume_by_token_by_hour_by_key_hash AS
        (
        SELECT DATE_TRUNC('hour', blocks.timestamp)                   AS hour,
               key_hash,
               (CASE WHEN delta0 >= 0 THEN token0 ELSE token1 END)       token,
               SUM(CASE WHEN delta0 >= 0 THEN delta0 ELSE delta1 END) AS volume,
               SUM(FLOOR(((CASE WHEN delta0 >= 0 THEN delta0 ELSE delta1 END) * fee) /
                         340282366920938463463374607431768211456))    AS fees
        FROM swaps
                 JOIN pool_keys ON swaps.pool_key_hash = pool_keys.key_hash
                 JOIN blocks ON swaps.block_number = blocks.number
        GROUP BY hour, key_hash, token
            );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_volume_by_token_by_hour_by_hour_key_hash_token ON volume_by_token_by_hour_by_key_hash USING btree (key_hash, hour, token);

        CREATE MATERIALIZED VIEW IF NOT EXISTS tvl_delta_by_token_by_hour_by_key_hash AS
        (
        WITH token_deltas AS (SELECT token0                               AS token,
                                     key_hash,
                                     position_updates.delta0              AS delta,
                                     DATE_TRUNC('hour', blocks.timestamp) AS hour
                              FROM position_updates
                                       INNER JOIN
                                   pool_keys
                                   ON pool_keys.key_hash = position_updates.pool_key_hash
                                       INNER JOIN blocks
                                                  ON position_updates.block_number = blocks.number
                              UNION ALL
                              SELECT pool_keys.token1                     AS token,
                                     key_hash,
                                     position_updates.delta1              AS delta,
                                     DATE_TRUNC('hour', blocks.timestamp) AS hour
                              FROM position_updates
                                       INNER JOIN
                                   pool_keys
                                   ON pool_keys.key_hash = position_updates.pool_key_hash
                                       INNER JOIN blocks
                                                  ON position_updates.block_number = blocks.number
                              UNION ALL
                              SELECT pool_keys.token0                     AS token,
                                     key_hash,
                                     swaps.delta0                         AS delta,
                                     DATE_TRUNC('hour', blocks.timestamp) AS hour
                              FROM swaps
                                       INNER JOIN
                                   pool_keys ON pool_keys.key_hash = swaps.pool_key_hash
                                       INNER JOIN blocks
                                                  ON swaps.block_number = blocks.number
                              UNION ALL
                              SELECT pool_keys.token1                     AS token,
                                     key_hash,
                                     swaps.delta1                         AS delta,
                                     DATE_TRUNC('hour', blocks.timestamp) AS hour
                              FROM swaps
                                       INNER JOIN
                                   pool_keys ON pool_keys.key_hash = swaps.pool_key_hash
                                       INNER JOIN blocks
                                                  ON swaps.block_number = blocks.number
                              UNION ALL
                              SELECT pool_keys.token0                     AS token,
                                     key_hash,
                                     position_fees_collected.delta0       AS delta,
                                     DATE_TRUNC('hour', blocks.timestamp) AS hour
                              FROM position_fees_collected
                                       INNER JOIN
                                   pool_keys
                                   ON pool_keys.key_hash = position_fees_collected.pool_key_hash
                                       INNER JOIN blocks
                                                  ON position_fees_collected.block_number = blocks.number
                              UNION ALL
                              SELECT pool_keys.token1                     AS token,
                                     key_hash,
                                     position_fees_collected.delta1       AS delta,
                                     DATE_TRUNC('hour', blocks.timestamp) AS hour
                              FROM position_fees_collected
                                       INNER JOIN
                                   pool_keys
                                   ON pool_keys.key_hash = position_fees_collected.pool_key_hash
                                       INNER JOIN blocks
                                                  ON position_fees_collected.block_number = blocks.number
                              UNION ALL
                              SELECT pool_keys.token0                     AS token,
                                     key_hash,
                                     protocol_fees_paid.delta0            AS delta,
                                     DATE_TRUNC('hour', blocks.timestamp) AS hour
                              FROM protocol_fees_paid
                                       INNER JOIN
                                   pool_keys
                                   ON pool_keys.key_hash = protocol_fees_paid.pool_key_hash
                                       INNER JOIN blocks
                                                  ON protocol_fees_paid.block_number = blocks.number
                              UNION ALL
                              SELECT pool_keys.token1                     AS token,
                                     key_hash,
                                     protocol_fees_paid.delta1            AS delta,
                                     DATE_TRUNC('hour', blocks.timestamp) AS hour
                              FROM protocol_fees_paid
                                       INNER JOIN
                                   pool_keys
                                   ON pool_keys.key_hash = protocol_fees_paid.pool_key_hash
                                       INNER JOIN blocks
                                                  ON protocol_fees_paid.block_number = blocks.number
                              UNION ALL
                              SELECT pool_keys.token0                     AS token,
                                     key_hash,
                                     amount0                              AS delta,
                                     DATE_TRUNC('hour', blocks.timestamp) AS hour
                              FROM fees_accumulated
                                       INNER JOIN
                                   pool_keys
                                   ON pool_keys.key_hash = fees_accumulated.pool_key_hash
                                       INNER JOIN blocks
                                                  ON fees_accumulated.block_number = blocks.number
                              UNION ALL
                              SELECT pool_keys.token1                     AS token,
                                     key_hash,
                                     amount1                              AS delta,
                                     DATE_TRUNC('hour', blocks.timestamp) AS hour
                              FROM fees_accumulated
                                       INNER JOIN
                                   pool_keys
                                   ON pool_keys.key_hash = fees_accumulated.pool_key_hash
                                       INNER JOIN blocks
                                                  ON fees_accumulated.block_number = blocks.number)

        SELECT token,
               key_hash,
               hour,
               SUM(delta) AS delta
        FROM token_deltas
        GROUP BY token, key_hash, hour
            );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_tvl_delta_by_token_by_hour_by_key_hash_token_hour_key_hash ON tvl_delta_by_token_by_hour_by_key_hash USING btree (key_hash, hour, token);

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

        CREATE MATERIALIZED VIEW IF NOT EXISTS per_pool_per_tick_liquidity AS
        (
        SELECT pool_key_hash, tick, net_liquidity_delta_diff
        FROM per_pool_per_tick_liquidity_view);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_per_pool_per_tick_liquidity_pool_key_hash_tick ON per_pool_per_tick_liquidity USING btree (pool_key_hash, tick);

        CREATE OR REPLACE VIEW pool_states AS
        (
          WITH lss AS (SELECT 
                        key_hash, 
                        pk.block_number, 
                        pk.transaction_index, 
                        pk.event_index, 
                        COALESCE(sqrt_ratio_after, sqrt_ratio) AS sqrt_ratio, 
                        COALESCE(tick_after, tick) AS tick,
                        COALESCE(liquidity_after,0) AS liquidity_last,
                        pi.block_number AS pi_block_number,
                        pi.transaction_index AS pi_transaction_index,
                        pi.event_index AS pi_event_index
                
                FROM pool_keys 
                LEFT JOIN lateral 
                  (SELECT block_number, 
                        transaction_index, 
                        event_index, 
                        sqrt_ratio_after,
                        tick_after, 
                        liquidity_after FROM swaps 
                  WHERE key_hash = swaps.pool_key_hash 
                  ORDER BY block_number DESC, transaction_index DESC, event_index DESC
                  LIMIT 1) pk on True
      LEFT JOIN lateral 
                  (SELECT sqrt_ratio, 
                      tick,
                      block_number,
                      transaction_index,
                      event_index
                  FROM pool_initializations 
                  WHERE key_hash = pool_initializations.pool_key_hash 
                  ORDER BY block_number DESC, transaction_index DESC, event_index DESC
                  LIMIT 1) pi on True
                )
              --SELECT * from lss
              ,better_liqudity AS (SELECT 
                          key_hash,
                          liquidity_delta 
                        FROM lss 
                        JOIN lateral 
                          (SELECT sum(liquidity_delta) AS liquidity_delta 
                          FROM position_updates pu 
                          WHERE key_hash = pool_key_hash										
                              AND lss.tick BETWEEN pu.lower_bound 
                            AND (pu.upper_bound - 1)
                              AND (lss.block_number IS NULL 
                              OR
                                  (lss.block_number, lss.transaction_index, lss.event_index) 
                                < (pu.block_number, pu.transaction_index, pu.event_index)
                              )
                          ) bl ON True),
              pl AS (
                SELECT 
                  lss.key_hash, 
                  pu.block_number, 
                  pu.transaction_index, 
                  pu.event_index, 
                  (COALESCE(lss.liquidity_last, 0)+ COALESCE(liquidity_delta, 0)) AS liquidity
                  FROM lss
                  JOIN better_liqudity bl ON bl.key_hash = lss.key_hash
                  LEFT JOIN lateral (SELECT 
                            block_number, 
                            transaction_index, 
                            event_index 
                            FROM position_updates 
                            WHERE lss.key_hash = pool_key_hash 
                            ORDER BY block_number DESC, transaction_index DESC, event_index DESC
                            LIMIT 1
                          ) pu ON True
                  )

                  
          SELECT 
            lss.key_hash AS pool_key_hash
            ,sqrt_ratio
            ,tick
            ,liquidity
            ,COALESCE((CASE
                  WHEN lss.block_number > pl.block_number 
                    THEN lss.block_number
                  ELSE pl.block_number 
                END), pi_block_number) AS block_number
            ,COALESCE((CASE
                  WHEN lss.block_number > pl.block_number 
                    OR (lss.block_number = pl.block_number 
                      AND lss.transaction_index > pl.transaction_index)
                  THEN lss.transaction_index
                  ELSE pl.transaction_index 
                  END), pi_transaction_index) AS transaction_index
            ,COALESCE((CASE
                  WHEN lss.block_number > pl.block_number 
                      OR (lss.block_number = pl.block_number 
                      AND lss.transaction_index > pl.transaction_index) 
                      OR (lss.block_number = pl.block_number 
                      AND lss.transaction_index = pl.transaction_index 
                      AND lss.event_index > pl.event_index
                      )
                  THEN lss.event_index
                  ELSE pl.event_index 
                  END), pi_event_index) AS event_index
        FROM lss
                 JOIN pl ON lss.key_hash = pl.key_hash
            );

        CREATE MATERIALIZED VIEW IF NOT EXISTS pool_states_materialized AS
        (
        SELECT pool_key_hash, block_number, transaction_index, event_index, sqrt_ratio, liquidity, tick
        FROM pool_states);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_pool_states_materialized_pool_key_hash ON pool_states_materialized USING btree (pool_key_hash);
    `);
  }

  public async refreshMaterializedViews() {
    await this.pg.query(`
      REFRESH MATERIALIZED VIEW CONCURRENTLY volume_by_token_by_hour_by_key_hash;
      REFRESH MATERIALIZED VIEW CONCURRENTLY tvl_delta_by_token_by_hour_by_key_hash;
    `);
  }

  public async refreshOperationalMaterializedView() {
    await this.pg.query(`
            REFRESH MATERIALIZED VIEW CONCURRENTLY per_pool_per_tick_liquidity;
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
                INSERT INTO cursor (id, order_key, unique_key)
                VALUES (1, $1, $2)
                ON CONFLICT (id) DO UPDATE SET order_key  = $1,
                                               unique_key = $2;
            `,
      values: [BigInt(cursor.orderKey), cursor.uniqueKey],
    });
  }

  public async insertBlock({
    number,
    hash,
    timestamp,
  }: {
    number: bigint;
    hash: bigint;
    timestamp: bigint;
  }) {
    await this.pg.query({
      text: `
                INSERT INTO blocks (number, hash, timestamp)
                VALUES ($1, $2, to_timestamp($3));
            `,
      values: [number, hash, timestamp],
    });
  }

  private async insertPoolKeyHash(pool_key: PoolKey) {
    const key_hash = computeKeyHash(pool_key);

    await this.pg.query({
      text: `
                insert into pool_keys (key_hash,
                                       token0,
                                       token1,
                                       fee,
                                       tick_spacing,
                                       extension)
                values ($1, $2, $3, $4, $5, $6)
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
                insert into position_minted
                (block_number,
                 transaction_index,
                 event_index,
                 transaction_hash,
                 token_id,
                 lower_bound,
                 upper_bound,
                 pool_key_hash)
                values ($1, $2, $3, $4, $5, $6, $7, $8);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,

        minted.id,
        minted.bounds.lower,
        minted.bounds.upper,
        pool_key_hash,
      ],
    });
  }

  public async insertPositionDeposit(deposit: DepositEvent, key: EventKey) {
    const pool_key_hash = await this.insertPoolKeyHash(deposit.pool_key);

    await this.pg.query({
      text: `
                insert into position_deposit
                (block_number,
                 transaction_index,
                 event_index,
                 transaction_hash,
                 token_id,
                 lower_bound,
                 upper_bound,
                 pool_key_hash,
                 liquidity,
                 delta0,
                 delta1)
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
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
                insert into position_withdraw
                (block_number,
                 transaction_index,
                 event_index,
                 transaction_hash,
                 token_id,
                 lower_bound,
                 upper_bound,
                 pool_key_hash,
                 collect_fees,
                 liquidity,
                 delta0,
                 delta1,
                 recipient)
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
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
                INSERT INTO position_transfers
                (block_number,
                 transaction_index,
                 event_index,
                 transaction_hash,
                 token_id,
                 from_address,
                 to_address)
                values ($1, $2, $3, $4, $5, $6, $7)
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
                INSERT INTO position_updates
                (block_number,
                 transaction_index,
                 event_index,
                 transaction_hash,
                 locker,
                 pool_key_hash,
                 salt,
                 lower_bound,
                 upper_bound,
                 liquidity_delta,
                 delta0,
                 delta1)
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
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
                INSERT INTO position_fees_collected
                (block_number,
                 transaction_index,
                 event_index,
                 transaction_hash,
                 pool_key_hash,
                 owner,
                 salt,
                 lower_bound,
                 upper_bound,
                 delta0,
                 delta1)
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
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
                INSERT INTO pool_initializations
                (block_number,
                 transaction_index,
                 event_index,
                 transaction_hash,
                 pool_key_hash,
                 tick,
                 sqrt_ratio,
                 call_points)
                values ($1, $2, $3, $4, $5, $6, $7, $8);
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
                INSERT INTO protocol_fees_withdrawn
                (block_number,
                 transaction_index,
                 event_index,
                 transaction_hash,
                 recipient,
                 token,
                 amount)
                values ($1, $2, $3, $4, $5, $6, $7);
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
                INSERT INTO protocol_fees_paid
                (block_number,
                 transaction_index,
                 event_index,
                 transaction_hash,
                 pool_key_hash,
                 owner,
                 salt,
                 lower_bound,
                 upper_bound,
                 delta0,
                 delta1)
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
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
                INSERT INTO fees_accumulated
                (block_number,
                 transaction_index,
                 event_index,
                 transaction_hash,
                 pool_key_hash,
                 amount0,
                 amount1)
                values ($1, $2, $3, $4, $5, $6, $7);
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

  public async insertSwappedEvent(event: SwappedEvent, key: EventKey) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      text: `
                INSERT INTO swaps
                (block_number,
                 transaction_index,
                 event_index,
                 transaction_hash,
                 locker,
                 pool_key_hash,
                 delta0,
                 delta1,
                 sqrt_ratio_after,
                 tick_after,
                 liquidity_after)
                values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
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
