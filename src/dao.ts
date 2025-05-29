import type { PoolClient } from "pg";
import { Client } from "pg";
import type { EventKey } from "./processor";
import type {
  FeesAccumulatedEvent,
  PoolInitializationEvent,
  PoolKey,
  PositionFeesCollectedEvent,
  PositionUpdatedEvent,
  ProtocolFeesPaidEvent,
  ProtocolFeesWithdrawnEvent,
  SwappedEvent,
} from "./events/core";
import type { TransferEvent } from "./events/nft";
import { computeKeyHash } from "./poolKeyHash.ts";
import type { PositionMintedWithReferrer } from "./events/positions";
import type {
  OrderKey,
  OrderProceedsWithdrawnEvent,
  OrderUpdatedEvent,
  VirtualOrdersExecutedEvent,
} from "./events/twamm";
import type { StakedEvent, WithdrawnEvent } from "./events/staker";
import type {
  DescribedEvent,
  GovernorCanceledEvent,
  GovernorExecutedEvent,
  GovernorProposedEvent,
  GovernorReconfiguredEvent,
  GovernorVotedEvent,
} from "./events/governor";
import type {
  TokenRegistrationEvent,
  TokenRegistrationEventV3,
} from "./events/tokenRegistry";
import type { SnapshotEvent } from "./events/oracle";
import type { OrderClosedEvent, OrderPlacedEvent } from "./events/limitOrders";

const MAX_TICK_SPACING = 354892;
const LIMIT_ORDER_TICK_SPACING = 128;

function orderKeyToPoolKey(event_key: EventKey, order_key: OrderKey): PoolKey {
  const [token0, token1]: [bigint, bigint] =
    order_key.buy_token > order_key.sell_token
      ? [order_key.sell_token, order_key.buy_token]
      : [order_key.buy_token, order_key.sell_token];

  return {
    token0,
    token1,
    fee: order_key.fee,
    tick_spacing: BigInt(MAX_TICK_SPACING),
    extension: event_key.emitter,
  };
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
    await this.createSchema();
    const cursor = await this.loadCursor();
    // we need to clear anything that was potentially inserted as pending before starting
    if (cursor) {
      await this.deleteOldBlockNumbers(Number(cursor.orderKey) + 1);
    }
    await this.commitTransaction();
    return cursor;
  }

  private async createSchema(): Promise<void> {
    await this.pg.query(`
        CREATE TABLE IF NOT EXISTS cursor
        (
            id           INT         NOT NULL UNIQUE CHECK (id = 1), -- only one row.
            order_key    BIGINT      NOT NULL,
            unique_key   bytea       NOT NULL,
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
            event_index       int2    NOT NULL,
            emitter           NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_event_keys_block_number_transaction_index_event_index ON event_keys USING btree (block_number, transaction_index, event_index);
        CREATE INDEX IF NOT EXISTS idx_event_keys_transaction_hash ON event_keys USING btree (transaction_hash);

        CREATE TABLE IF NOT EXISTS position_transfers
        (
            event_id     int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            token_id     int8    NOT NULL,
            from_address NUMERIC NOT NULL,
            to_address   NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_position_transfers_token_id_from_to ON position_transfers (token_id, from_address, to_address);
        CREATE INDEX IF NOT EXISTS idx_position_transfers_to_address ON position_transfers (to_address);

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

            pool_key_hash NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            tick          int4    NOT NULL,
            sqrt_ratio    NUMERIC NOT NULL
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

        CREATE TABLE IF NOT EXISTS token_registrations_v3
        (
            event_id     int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            address      NUMERIC NOT NULL,

            name         VARCHAR NOT NULL,
            symbol       VARCHAR NOT NULL,
            decimals     INT     NOT NULL,
            total_supply NUMERIC NOT NULL
        );

        CREATE TABLE IF NOT EXISTS staker_staked
        (
            event_id     int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            from_address NUMERIC NOT NULL,
            amount       NUMERIC NOT NULL,
            delegate     NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_staker_staked_delegate_from_address ON staker_staked USING btree (delegate, from_address);
        CREATE INDEX IF NOT EXISTS idx_staker_staked_from_address_delegate ON staker_staked USING btree (from_address, delegate);

        CREATE TABLE IF NOT EXISTS staker_withdrawn
        (
            event_id     int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            from_address NUMERIC NOT NULL,
            amount       NUMERIC NOT NULL,
            recipient    NUMERIC NOT NULL,
            delegate     NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_staker_withdrawn_delegate_from_address ON staker_staked USING btree (delegate, from_address);
        CREATE INDEX IF NOT EXISTS idx_staker_withdrawn_from_address_delegate ON staker_staked USING btree (from_address, delegate);

        CREATE TABLE IF NOT EXISTS governor_reconfigured
        (
            event_id                         int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            version                          BIGINT  NOT NULL,

            voting_start_delay               BIGINT  NOT NULL,
            voting_period                    BIGINT  NOT NULL,
            voting_weight_smoothing_duration BIGINT  NOT NULL,
            quorum                           NUMERIC NOT NULL,
            proposal_creation_threshold      NUMERIC NOT NULL,
            execution_delay                  BIGINT  NOT NULL,
            execution_window                 BIGINT  NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_governor_reconfigured_version ON governor_reconfigured USING btree (version);

        CREATE TABLE IF NOT EXISTS governor_proposed
        (
            event_id       int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            id             NUMERIC NOT NULL,
            proposer       NUMERIC NOT NULL,
            config_version BIGINT REFERENCES governor_reconfigured (version) ON DELETE CASCADE
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_governor_proposed_id ON governor_proposed USING btree (id);

        CREATE TABLE IF NOT EXISTS governor_proposed_calls
        (
            proposal_id NUMERIC   NOT NULL REFERENCES governor_proposed (id) ON DELETE CASCADE,
            index       int2      NOT NULL,
            to_address  NUMERIC   NOT NULL,
            selector    NUMERIC   NOT NULL,
            calldata    NUMERIC[] NOT NULL,
            PRIMARY KEY (proposal_id, index)
        );

        CREATE TABLE IF NOT EXISTS governor_canceled
        (
            event_id int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            id       NUMERIC NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_governor_canceled_id ON governor_canceled USING btree (id);

        CREATE TABLE IF NOT EXISTS governor_voted
        (
            event_id int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            id       NUMERIC NOT NULL,
            voter    NUMERIC NOT NULL,
            weight   NUMERIC NOT NULL,
            yea      BOOLEAN NOT NULL
        );

        CREATE TABLE IF NOT EXISTS governor_executed
        (
            event_id int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            id       NUMERIC NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_governor_executed_id ON governor_executed USING btree (id);

        CREATE TABLE IF NOT EXISTS governor_executed_results
        (
            proposal_id NUMERIC   NOT NULL REFERENCES governor_executed (id) ON DELETE CASCADE,
            index       int2      NOT NULL,
            results     NUMERIC[] NOT NULL,
            PRIMARY KEY (proposal_id, index)
        );

        CREATE TABLE IF NOT EXISTS governor_proposal_described
        (
            event_id    int8 REFERENCES event_keys (id) ON DELETE CASCADE PRIMARY KEY,

            id          NUMERIC NOT NULL,
            description TEXT    NOT NULL
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

        CREATE TABLE IF NOT EXISTS hourly_price_data
        (
            token0     NUMERIC,
            token1     NUMERIC,
            hour       timestamptz,
            k_volume   NUMERIC,
            total      NUMERIC,
            swap_count NUMERIC,
            PRIMARY KEY (token0, token1, hour)
        );

        CREATE TABLE IF NOT EXISTS hourly_tvl_delta_by_token
        (
            key_hash NUMERIC,
            hour     timestamptz,
            token    NUMERIC,
            delta    NUMERIC,
            PRIMARY KEY (key_hash, hour, token)
        );

        CREATE TABLE IF NOT EXISTS hourly_revenue_by_token
        (
            key_hash NUMERIC,
            hour     timestamptz,
            token    NUMERIC,
            revenue  NUMERIC,
            PRIMARY KEY (key_hash, hour, token)
        );

        CREATE OR REPLACE VIEW per_pool_per_tick_liquidity_view AS
        (
        WITH all_tick_deltas AS (SELECT pool_key_hash,
                                        lower_bound AS       tick,
                                        SUM(liquidity_delta) net_liquidity_delta,
                                        SUM(liquidity_delta) total_liquidity_on_tick
                                 FROM position_updates
                                 GROUP BY pool_key_hash, lower_bound
                                 UNION ALL
                                 SELECT pool_key_hash,
                                        upper_bound AS        tick,
                                        SUM(-liquidity_delta) net_liquidity_delta,
                                        SUM(liquidity_delta)  total_liquidity_on_tick
                                 FROM position_updates
                                 GROUP BY pool_key_hash, upper_bound),
             summed AS (SELECT pool_key_hash,
                               tick,
                               SUM(net_liquidity_delta)     AS net_liquidity_delta_diff,
                               SUM(total_liquidity_on_tick) AS total_liquidity_on_tick
                        FROM all_tick_deltas
                        GROUP BY pool_key_hash, tick)
        SELECT pool_key_hash, tick, net_liquidity_delta_diff, total_liquidity_on_tick
        FROM summed
        WHERE net_liquidity_delta_diff != 0
        ORDER BY tick);

        CREATE TABLE IF NOT EXISTS per_pool_per_tick_liquidity_incremental_view
        (
            pool_key_hash            NUMERIC,
            tick                     int4,
            net_liquidity_delta_diff NUMERIC,
            total_liquidity_on_tick  NUMERIC,
            PRIMARY KEY (pool_key_hash, tick)
        );

        DELETE
        FROM per_pool_per_tick_liquidity_incremental_view;
        INSERT INTO per_pool_per_tick_liquidity_incremental_view (pool_key_hash, tick, net_liquidity_delta_diff,
                                                                  total_liquidity_on_tick)
            (SELECT pool_key_hash, tick, net_liquidity_delta_diff, total_liquidity_on_tick
             FROM per_pool_per_tick_liquidity_view);

        CREATE OR REPLACE FUNCTION net_liquidity_deltas_after_insert()
            RETURNS TRIGGER AS
        $$
        BEGIN
            -- Update or insert for lower_bound
            UPDATE per_pool_per_tick_liquidity_incremental_view
            SET net_liquidity_delta_diff = net_liquidity_delta_diff + new.liquidity_delta,
                total_liquidity_on_tick  = total_liquidity_on_tick + new.liquidity_delta
            WHERE pool_key_hash = new.pool_key_hash
              AND tick = new.lower_bound;

            IF NOT found THEN
                INSERT INTO per_pool_per_tick_liquidity_incremental_view (pool_key_hash, tick,
                                                                          net_liquidity_delta_diff,
                                                                          total_liquidity_on_tick)
                VALUES (new.pool_key_hash, new.lower_bound, new.liquidity_delta, new.liquidity_delta);
            END IF;

            -- Delete if total_liquidity_on_tick is zero
            DELETE
            FROM per_pool_per_tick_liquidity_incremental_view
            WHERE pool_key_hash = new.pool_key_hash
              AND tick = new.lower_bound
              AND total_liquidity_on_tick = 0;

            -- Update or insert for upper_bound
            UPDATE per_pool_per_tick_liquidity_incremental_view
            SET net_liquidity_delta_diff = net_liquidity_delta_diff - new.liquidity_delta,
                total_liquidity_on_tick  = total_liquidity_on_tick + new.liquidity_delta
            WHERE pool_key_hash = new.pool_key_hash
              AND tick = new.upper_bound;

            IF NOT found THEN
                INSERT INTO per_pool_per_tick_liquidity_incremental_view (pool_key_hash, tick,
                                                                          net_liquidity_delta_diff,
                                                                          total_liquidity_on_tick)
                VALUES (new.pool_key_hash, new.upper_bound, -new.liquidity_delta, new.liquidity_delta);
            END IF;

            -- Delete if net_liquidity_delta_diff is zero
            DELETE
            FROM per_pool_per_tick_liquidity_incremental_view
            WHERE pool_key_hash = new.pool_key_hash
              AND tick = new.upper_bound
              AND total_liquidity_on_tick = 0;

            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION net_liquidity_deltas_after_delete()
            RETURNS TRIGGER AS
        $$
        BEGIN
            -- Reverse effect for lower_bound
            UPDATE per_pool_per_tick_liquidity_incremental_view
            SET net_liquidity_delta_diff = net_liquidity_delta_diff - old.liquidity_delta,
                total_liquidity_on_tick  = total_liquidity_on_tick - old.liquidity_delta
            WHERE pool_key_hash = old.pool_key_hash
              AND tick = old.lower_bound;

            IF NOT found THEN
                INSERT INTO per_pool_per_tick_liquidity_incremental_view (pool_key_hash, tick,
                                                                          net_liquidity_delta_diff,
                                                                          total_liquidity_on_tick)
                VALUES (old.pool_key_hash, old.lower_bound, -old.liquidity_delta, -old.liquidity_delta);
            END IF;

            -- Delete if net_liquidity_delta_diff is zero
            DELETE
            FROM per_pool_per_tick_liquidity_incremental_view
            WHERE pool_key_hash = old.pool_key_hash
              AND tick = old.lower_bound
              AND total_liquidity_on_tick = 0;

            -- Reverse effect for upper_bound
            UPDATE per_pool_per_tick_liquidity_incremental_view
            SET net_liquidity_delta_diff = net_liquidity_delta_diff + old.liquidity_delta,
                total_liquidity_on_tick  = total_liquidity_on_tick - old.liquidity_delta
            WHERE pool_key_hash = old.pool_key_hash
              AND tick = old.upper_bound;

            IF NOT found THEN
                INSERT INTO per_pool_per_tick_liquidity_incremental_view (pool_key_hash, tick,
                                                                          net_liquidity_delta_diff,
                                                                          total_liquidity_on_tick)
                VALUES (old.pool_key_hash, old.upper_bound, old.liquidity_delta, -old.liquidity_delta);
            END IF;

            -- Delete if net_liquidity_delta_diff is zero
            DELETE
            FROM per_pool_per_tick_liquidity_incremental_view
            WHERE pool_key_hash = old.pool_key_hash
              AND tick = old.upper_bound
              AND total_liquidity_on_tick = 0;

            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION net_liquidity_deltas_after_update()
            RETURNS TRIGGER AS
        $$
        BEGIN
            -- Reverse OLD row effects (similar to DELETE)
            PERFORM net_liquidity_deltas_after_delete();

            -- Apply NEW row effects (similar to INSERT)
            PERFORM net_liquidity_deltas_after_insert();

            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE TRIGGER net_liquidity_deltas_after_insert
            AFTER INSERT
            ON position_updates
            FOR EACH ROW
        EXECUTE FUNCTION net_liquidity_deltas_after_insert();

        CREATE OR REPLACE TRIGGER net_liquidity_deltas_after_delete
            AFTER DELETE
            ON position_updates
            FOR EACH ROW
        EXECUTE FUNCTION net_liquidity_deltas_after_delete();

        CREATE OR REPLACE TRIGGER net_liquidity_deltas_after_update
            AFTER UPDATE
            ON position_updates
            FOR EACH ROW
        EXECUTE FUNCTION net_liquidity_deltas_after_update();


        CREATE TABLE IF NOT EXISTS twamm_order_updates
        (
            event_id         int8        NOT NULL PRIMARY KEY REFERENCES event_keys (id) ON DELETE CASCADE,

            key_hash         NUMERIC     NOT NULL REFERENCES pool_keys (key_hash),

            owner            NUMERIC     NOT NULL,
            salt             NUMERIC     NOT NULL,
            sale_rate_delta0 NUMERIC     NOT NULL,
            sale_rate_delta1 NUMERIC     NOT NULL,
            start_time       timestamptz NOT NULL,
            end_time         timestamptz NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_twamm_order_updates_key_hash_event_id ON twamm_order_updates USING btree (key_hash, event_id);
        CREATE INDEX IF NOT EXISTS idx_twamm_order_updates_key_hash_time ON twamm_order_updates USING btree (key_hash, start_time, end_time);
        CREATE INDEX IF NOT EXISTS idx_twamm_order_updates_owner_salt ON twamm_order_updates USING btree (owner, salt);
        CREATE INDEX IF NOT EXISTS idx_twamm_order_updates_salt ON twamm_order_updates USING btree (salt);
        CREATE INDEX IF NOT EXISTS idx_twamm_order_updates_salt_key_hash_start_end_owner_event_id ON twamm_order_updates (salt, key_hash, start_time, end_time, owner, event_id);

        CREATE TABLE IF NOT EXISTS twamm_proceeds_withdrawals
        (
            event_id   int8        NOT NULL PRIMARY KEY REFERENCES event_keys (id) ON DELETE CASCADE,

            key_hash   NUMERIC     NOT NULL REFERENCES pool_keys (key_hash),

            owner      NUMERIC     NOT NULL,
            salt       NUMERIC     NOT NULL,
            amount0    NUMERIC     NOT NULL,
            amount1    NUMERIC     NOT NULL,
            start_time timestamptz NOT NULL,
            end_time   timestamptz NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_twamm_proceeds_withdrawals_key_hash_event_id ON twamm_proceeds_withdrawals USING btree (key_hash, event_id);
        CREATE INDEX IF NOT EXISTS idx_twamm_proceeds_withdrawals_key_hash_time ON twamm_proceeds_withdrawals USING btree (key_hash, start_time, end_time);
        CREATE INDEX IF NOT EXISTS idx_twamm_proceeds_withdrawals_owner_salt ON twamm_proceeds_withdrawals USING btree (owner, salt);
        CREATE INDEX IF NOT EXISTS idx_twamm_proceeds_withdrawals_salt ON twamm_proceeds_withdrawals USING btree (salt);
        CREATE INDEX IF NOT EXISTS idx_twamm_proceeds_withdrawals_salt_event_id_desc ON twamm_proceeds_withdrawals (salt, event_id DESC);

        CREATE TABLE IF NOT EXISTS twamm_virtual_order_executions
        (
            event_id         int8    NOT NULL PRIMARY KEY REFERENCES event_keys (id) ON DELETE CASCADE,

            key_hash         NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            token0_sale_rate NUMERIC NOT NULL,
            token1_sale_rate NUMERIC NOT NULL,
            delta0           NUMERIC NOT NULL,
            delta1           NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_twamm_virtual_order_executions_pool_key_hash_event_id ON twamm_virtual_order_executions USING btree (key_hash, event_id DESC);

        CREATE TABLE IF NOT EXISTS oracle_snapshots
        (
            event_id                 int8    NOT NULL PRIMARY KEY REFERENCES event_keys (id) ON DELETE CASCADE,

            key_hash                 NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            token0                   NUMERIC NOT NULL,
            token1                   NUMERIC NOT NULL,
            index                    int8    NOT NULL,
            snapshot_block_timestamp int8    NOT NULL,
            snapshot_tick_cumulative NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_oracle_snapshots_token0_token1_index ON oracle_snapshots USING btree (token0, token1, index);

        CREATE TABLE IF NOT EXISTS limit_order_placed
        (
            event_id  int8    NOT NULL PRIMARY KEY REFERENCES event_keys (id) ON DELETE CASCADE,

            key_hash  NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            owner     NUMERIC NOT NULL,
            salt      NUMERIC NOT NULL,
            token0    NUMERIC NOT NULL,
            token1    NUMERIC NOT NULL,
            tick      int4    NOT NULL,
            liquidity NUMERIC NOT NULL,
            amount    NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_limit_order_placed_owner_salt ON limit_order_placed USING btree (owner, salt);

        CREATE TABLE IF NOT EXISTS limit_order_closed
        (
            event_id int8    NOT NULL PRIMARY KEY REFERENCES event_keys (id) ON DELETE CASCADE,

            key_hash NUMERIC NOT NULL REFERENCES pool_keys (key_hash),

            owner    NUMERIC NOT NULL,
            salt     NUMERIC NOT NULL,
            token0   NUMERIC NOT NULL,
            token1   NUMERIC NOT NULL,
            tick     int4    NOT NULL,
            amount0  NUMERIC NOT NULL,
            amount1  NUMERIC NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_limit_order_closed ON limit_order_closed USING btree (owner, salt);

        CREATE OR REPLACE VIEW twamm_pool_states_view AS
        (
        WITH lvoe_id AS (SELECT key_hash, MAX(event_id) AS event_id
                         FROM twamm_virtual_order_executions
                         GROUP BY key_hash),

             last_virtual_order_execution AS (SELECT pk.key_hash,
                                                     last_voe.token0_sale_rate,
                                                     last_voe.token1_sale_rate,
                                                     last_voe.event_id AS last_virtual_order_execution_event_id,
                                                     b.time            AS last_virtual_execution_time
                                              FROM pool_keys pk
                                                       JOIN lvoe_id ON lvoe_id.key_hash = pk.key_hash
                                                       JOIN twamm_virtual_order_executions last_voe
                                                            ON last_voe.event_id = lvoe_id.event_id
                                                       JOIN event_keys ek ON last_voe.event_id = ek.id
                                                       JOIN blocks b ON ek.block_number = b.number),
             active_order_updates_after_lvoe AS (SELECT lvoe_1.key_hash,
                                                        SUM(tou.sale_rate_delta0) AS sale_rate_delta0,
                                                        SUM(tou.sale_rate_delta1) AS sale_rate_delta1,
                                                        MAX(tou.event_id)         AS last_order_update_event_id
                                                 FROM last_virtual_order_execution lvoe_1
                                                          JOIN twamm_order_updates tou
                                                               ON tou.key_hash = lvoe_1.key_hash AND
                                                                  tou.event_id >
                                                                  lvoe_1.last_virtual_order_execution_event_id AND
                                                                  tou.start_time <=
                                                                  lvoe_1.last_virtual_execution_time AND
                                                                  tou.end_time >
                                                                  lvoe_1.last_virtual_execution_time
                                                 GROUP BY lvoe_1.key_hash)
        SELECT lvoe.key_hash                                                          AS pool_key_hash,
               lvoe.token0_sale_rate + COALESCE(ou_lvoe.sale_rate_delta0, 0::NUMERIC) AS token0_sale_rate,
               lvoe.token1_sale_rate + COALESCE(ou_lvoe.sale_rate_delta1, 0::NUMERIC) AS token1_sale_rate,
               lvoe.last_virtual_execution_time,
               GREATEST(COALESCE(ou_lvoe.last_order_update_event_id, lvoe.last_virtual_order_execution_event_id),
                        psm.last_event_id)                                            AS last_event_id
        FROM last_virtual_order_execution lvoe
                 JOIN pool_states_materialized psm ON lvoe.key_hash = psm.pool_key_hash
                 LEFT JOIN active_order_updates_after_lvoe ou_lvoe ON lvoe.key_hash = ou_lvoe.key_hash
            );

        CREATE MATERIALIZED VIEW IF NOT EXISTS twamm_pool_states_materialized AS
        (
        SELECT pool_key_hash,
               token0_sale_rate,
               token1_sale_rate,
               last_virtual_execution_time,
               last_event_id
        FROM twamm_pool_states_view);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_twamm_pool_states_materialized_key_hash ON twamm_pool_states_materialized USING btree (pool_key_hash);

        CREATE OR REPLACE VIEW twamm_sale_rate_deltas_view AS
        (
        WITH all_order_deltas AS (SELECT key_hash,
                                         start_time AS         time,
                                         SUM(sale_rate_delta0) net_sale_rate_delta0,
                                         SUM(sale_rate_delta1) net_sale_rate_delta1
                                  FROM twamm_order_updates
                                  GROUP BY key_hash, start_time
                                  UNION ALL
                                  SELECT key_hash,
                                         end_time AS            time,
                                         -SUM(sale_rate_delta0) net_sale_rate_delta0,
                                         -SUM(sale_rate_delta1) net_sale_rate_delta1
                                  FROM twamm_order_updates
                                  GROUP BY key_hash, end_time),
             summed AS (SELECT key_hash,
                               time,
                               SUM(net_sale_rate_delta0) AS net_sale_rate_delta0,
                               SUM(net_sale_rate_delta1) AS net_sale_rate_delta1
                        FROM all_order_deltas
                        GROUP BY key_hash, time)
        SELECT key_hash AS pool_key_hash, time, net_sale_rate_delta0, net_sale_rate_delta1
        FROM summed
        WHERE net_sale_rate_delta0 != 0
           OR net_sale_rate_delta1 != 0
        ORDER BY key_hash, time);

        CREATE MATERIALIZED VIEW IF NOT EXISTS twamm_sale_rate_deltas_materialized AS
        (
        SELECT tsrdv.pool_key_hash, tsrdv.time, tsrdv.net_sale_rate_delta0, tsrdv.net_sale_rate_delta1
        FROM twamm_sale_rate_deltas_view AS tsrdv
                 JOIN twamm_pool_states_materialized tpsm
                      ON tpsm.pool_key_hash = tsrdv.pool_key_hash AND
                         tpsm.last_virtual_execution_time < tsrdv.time);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_twamm_sale_rate_deltas_materialized_pool_key_hash_time ON twamm_sale_rate_deltas_materialized USING btree (pool_key_hash, time);

        CREATE OR REPLACE VIEW limit_order_pool_states_view AS
        (
        WITH last_limit_order_placed AS (SELECT key_hash, MAX(event_id) AS event_id
                                         FROM limit_order_placed
                                         GROUP BY key_hash),
             last_limit_order_closed AS (SELECT key_hash, MAX(event_id) AS event_id
                                         FROM limit_order_closed
                                         GROUP BY key_hash)
        SELECT COALESCE(llop.key_hash, lloc.key_hash) AS pool_key_hash,
               GREATEST(GREATEST(llop.event_id, COALESCE(lloc.event_id, 0)),
                        psm.last_event_id)            AS last_event_id
        FROM last_limit_order_placed llop
                 JOIN pool_states_materialized psm ON llop.key_hash = psm.pool_key_hash
                 LEFT JOIN last_limit_order_closed lloc ON llop.key_hash = lloc.key_hash
            );

        CREATE MATERIALIZED VIEW IF NOT EXISTS limit_order_pool_states_materialized AS
        (
        SELECT pool_key_hash, last_event_id
        FROM limit_order_pool_states_view);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_limit_order_pool_states_materialized_pool_key_hash ON limit_order_pool_states_materialized USING btree (pool_key_hash);

        CREATE OR REPLACE VIEW last_24h_pool_stats_view AS
        (
        WITH volume AS (SELECT vbt.key_hash,
                               SUM(CASE WHEN vbt.token = token0 THEN vbt.volume ELSE 0 END) AS volume0,
                               SUM(CASE WHEN vbt.token = token1 THEN vbt.volume ELSE 0 END) AS volume1,
                               SUM(CASE WHEN vbt.token = token0 THEN vbt.fees ELSE 0 END)   AS fees0,
                               SUM(CASE WHEN vbt.token = token1 THEN vbt.fees ELSE 0 END)   AS fees1
                        FROM hourly_volume_by_token vbt
                                 JOIN pool_keys ON vbt.key_hash = pool_keys.key_hash
                        WHERE hour >= NOW() - INTERVAL '24 hours'
                        GROUP BY vbt.key_hash),
             tvl_total AS (SELECT tbt.key_hash,
                                  SUM(CASE WHEN token = token0 THEN delta ELSE 0 END) AS tvl0,
                                  SUM(CASE WHEN token = token1 THEN delta ELSE 0 END) AS tvl1
                           FROM hourly_tvl_delta_by_token tbt
                                    JOIN pool_keys pk ON tbt.key_hash = pk.key_hash
                           GROUP BY tbt.key_hash),
             tvl_delta_24h AS (SELECT tbt.key_hash,
                                      SUM(CASE WHEN token = token0 THEN delta ELSE 0 END) AS tvl0,
                                      SUM(CASE WHEN token = token1 THEN delta ELSE 0 END) AS tvl1
                               FROM hourly_tvl_delta_by_token tbt
                                        JOIN pool_keys pk ON tbt.key_hash = pk.key_hash
                               WHERE hour >= NOW() - INTERVAL '24 hours'
                               GROUP BY tbt.key_hash)
        SELECT pool_keys.key_hash,
               COALESCE(volume.volume0, 0)     AS volume0_24h,
               COALESCE(volume.volume1, 0)     AS volume1_24h,
               COALESCE(volume.fees0, 0)       AS fees0_24h,
               COALESCE(volume.fees1, 0)       AS fees1_24h,
               COALESCE(tvl_total.tvl0, 0)     AS tvl0_total,
               COALESCE(tvl_total.tvl1, 0)     AS tvl1_total,
               COALESCE(tvl_delta_24h.tvl0, 0) AS tvl0_delta_24h,
               COALESCE(tvl_delta_24h.tvl1, 0) AS tvl1_delta_24h
        FROM pool_keys
                 LEFT JOIN volume ON volume.key_hash = pool_keys.key_hash
                 LEFT JOIN
             tvl_total ON pool_keys.key_hash = tvl_total.key_hash
                 LEFT JOIN tvl_delta_24h
                           ON tvl_delta_24h.key_hash = pool_keys.key_hash
            );

        CREATE MATERIALIZED VIEW IF NOT EXISTS last_24h_pool_stats_materialized AS
        (
        SELECT key_hash,
               volume0_24h,
               volume1_24h,
               fees0_24h,
               fees1_24h,
               tvl0_total,
               tvl1_total,
               tvl0_delta_24h,
               tvl1_delta_24h
        FROM last_24h_pool_stats_view
            );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_last_24h_pool_stats_materialized_key_hash ON last_24h_pool_stats_materialized USING btree (key_hash);

        CREATE OR REPLACE FUNCTION parse_short_string(numeric_value NUMERIC) RETURNS VARCHAR AS
        $$
        DECLARE
            result_text TEXT    := '';
            byte_value  INTEGER;
            ascii_char  TEXT;
            n           NUMERIC := numeric_value;
        BEGIN
            IF n < 0 THEN
                RETURN NULL;
            END IF;

            IF n % 1 != 0 THEN
                RETURN NULL;
            END IF;

            IF n = 0 THEN
                RETURN '';
            END IF;

            WHILE n > 0
                LOOP
                    byte_value := MOD(n, 256)::INTEGER;
                    ascii_char := CHR(byte_value);
                    result_text := ascii_char || result_text; -- Prepend to maintain correct order
                    n := FLOOR(n / 256);
                END LOOP;

            RETURN result_text;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE VIEW latest_token_registrations_view AS
        (
        WITH all_token_registrations AS (SELECT address,
                                                event_id,
                                                parse_short_string(name)   AS name,
                                                parse_short_string(symbol) AS symbol,
                                                decimals,
                                                total_supply
                                         FROM token_registrations tr
                                         UNION ALL
                                         SELECT address,
                                                event_id,
                                                name,
                                                symbol,
                                                decimals,
                                                total_supply
                                         FROM token_registrations_v3 tr_v3),
             validated_registrations AS (SELECT *
                                         FROM all_token_registrations
                                         WHERE LENGTH(symbol) > 1
                                           AND LENGTH(symbol) < 10
                                           AND REGEXP_LIKE(symbol, '^[\\x00-\\x7F]*$', 'i')
                                           AND LENGTH(name) < 128
                                           AND REGEXP_LIKE(name, '^[\\x00-\\x7F]*$', 'i')),
             event_ids_per_address AS (SELECT address,
                                              MIN(event_id) AS first_registration_id,
                                              MAX(event_id) AS last_registration_id
                                       FROM validated_registrations vr
                                       GROUP BY address),
             first_registration_of_each_symbol AS (SELECT LOWER(symbol) AS lower_symbol, MIN(event_id) first_id
                                                   FROM validated_registrations
                                                   GROUP BY 1)
        SELECT iba.address,
               vr.name,
               vr.symbol,
               vr.decimals,
               vr.total_supply
        FROM event_ids_per_address AS iba
                 JOIN validated_registrations AS vr
                      ON iba.address = vr.address
                          AND iba.last_registration_id = vr.event_id
                 JOIN first_registration_of_each_symbol fr
                      ON fr.lower_symbol = LOWER(vr.symbol) AND iba.first_registration_id = fr.first_id
            );

        CREATE MATERIALIZED VIEW IF NOT EXISTS latest_token_registrations AS
        (
        SELECT address,
               name,
               symbol,
               decimals,
               total_supply
        FROM latest_token_registrations_view);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_latest_token_registrations_by_address ON latest_token_registrations USING btree (address);

        CREATE OR REPLACE VIEW oracle_pool_states_view AS
        (
        SELECT key_hash AS pool_key_hash, MAX(snapshot_block_timestamp) AS last_snapshot_block_timestamp
        FROM oracle_snapshots
        GROUP BY key_hash);

        CREATE MATERIALIZED VIEW IF NOT EXISTS oracle_pool_states_materialized AS
        (
        SELECT pool_key_hash,
               last_snapshot_block_timestamp
        FROM oracle_pool_states_view);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_oracle_pool_states_materialized_pool_key_hash ON oracle_pool_states_materialized USING btree (pool_key_hash);

        DROP MATERIALIZED VIEW IF EXISTS token_pair_realized_volatility;
        CREATE MATERIALIZED VIEW IF NOT EXISTS token_pair_realized_volatility AS
        (
        WITH times AS (SELECT blocks.time - INTERVAL '3 days' AS start_time,
                              blocks.time                     AS end_time
                       FROM blocks
                       ORDER BY number DESC
                       LIMIT 1),

             prices AS (SELECT token0,
                               token1,
                               hour,

                               total / k_volume AS price
                        FROM hourly_price_data p,
                             times t
                        WHERE p.hour BETWEEN t.start_time AND t.end_time
                        ORDER BY hour),

             log_price_changes AS (SELECT token0,
                                          token1,
                                          LN(price) -
                                          LN(COALESCE(
                                                          LAG(price) OVER (PARTITION BY token0, token1 ORDER BY hour),
                                                          price))                   AS price_change,
                                          EXTRACT(HOURS FROM hour - COALESCE(LAG(hour)
                                                                             OVER (PARTITION BY token0, token1 ORDER BY hour),
                                                                             hour)) AS hours_since_last
                                   FROM prices p,
                                        times t
                                   ORDER BY hour),

             realized_volatility_by_pair AS (SELECT token0,
                                                    token1,
                                                    STDDEV(lpc.price_change) * SQRT(SUM(hours_since_last)) AS realized_volatility
                                             FROM log_price_changes lpc
                                             GROUP BY token0, token1)

        SELECT token0,
               token1,
               realized_volatility,
               int4(FLOOR(LOG(EXP(realized_volatility)) / LOG(1.000001::NUMERIC))) AS volatility_in_ticks
        FROM realized_volatility_by_pair
        WHERE realized_volatility IS NOT NULL);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_token_pair_realized_volatility_pair
            ON token_pair_realized_volatility (token0, token1);

        CREATE OR REPLACE FUNCTION numeric_to_hex(num NUMERIC) RETURNS TEXT
            IMMUTABLE
            LANGUAGE plpgsql
        AS
        $$
        DECLARE
            hex       TEXT;
            remainder NUMERIC;
        BEGIN
            hex := '';
            LOOP
                IF num = 0 THEN
                    EXIT;
                END IF;
                remainder := num % 16;
                hex := SUBSTRING('0123456789abcdef' FROM (remainder::INT + 1) FOR 1) || hex;
                num := (num - remainder) / 16;
            END LOOP;
            RETURN '0x' || hex;
        END;
        $$;

        CREATE OR REPLACE FUNCTION calculate_staker_rewards(
            start_time timestamptz,
            end_time timestamptz,
            total_rewards NUMERIC,
            staking_share NUMERIC,
            delegate_share NUMERIC
        )
            RETURNS TABLE
                    (
                        id               BIGINT,
                        claimee          TEXT,
                        amount           NUMERIC,
                        delegate_portion NUMERIC,
                        staker_portion   NUMERIC
                    )
        AS
        $$
        BEGIN
            RETURN QUERY
                WITH
                    -- Step 0: Compute total duration and total rewards
                    calculated_parameters
                        AS (SELECT EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC AS total_duration_seconds),

                    -- Step 1: Collect all time points where any stake change occurs
                    time_points AS (SELECT DISTINCT time
                                    FROM (SELECT b.time
                                          FROM staker_staked s
                                                   JOIN event_keys ek ON s.event_id = ek.id
                                                   JOIN blocks b ON ek.block_number = b.number
                                          WHERE b.time BETWEEN start_time AND end_time
                                          UNION ALL
                                          SELECT b.time
                                          FROM staker_withdrawn w
                                                   JOIN event_keys ek ON w.event_id = ek.id
                                                   JOIN blocks b ON ek.block_number = b.number
                                          WHERE b.time BETWEEN start_time AND end_time
                                          UNION ALL
                                          SELECT start_time
                                          UNION ALL
                                          SELECT end_time) t),

                    ordered_time_points AS (SELECT time
                                            FROM time_points
                                            ORDER BY time),

                    -- Step 2: Generate intervals
                    intervals AS (SELECT time                            AS start_time,
                                         LEAD(time) OVER (ORDER BY time) AS end_time
                                  FROM ordered_time_points
                                  WHERE time < end_time),

                    -- Step 3: Collect all stake changes
                    stake_changes AS (SELECT b.time,
                                             s.from_address AS staker,
                                             s.amount       AS amount_change
                                      FROM staker_staked s
                                               JOIN event_keys ek ON s.event_id = ek.id
                                               JOIN blocks b ON ek.block_number = b.number
                                      WHERE b.time <= end_time
                                      UNION ALL
                                      SELECT b.time,
                                             w.from_address AS staker,
                                             -w.amount      AS amount_change
                                      FROM staker_withdrawn w
                                               JOIN event_keys ek ON w.event_id = ek.id
                                               JOIN blocks b ON ek.block_number = b.number
                                      WHERE b.time <= end_time
                                      UNION ALL
                                      SELECT start_time     AS time,
                                             s.from_address AS staker,
                                             SUM(s.amount)  AS amount_change
                                      FROM staker_staked s
                                               JOIN event_keys ek ON s.event_id = ek.id
                                               JOIN blocks b ON ek.block_number = b.number
                                      WHERE b.time < start_time
                                      GROUP BY s.from_address
                                      UNION ALL
                                      SELECT start_time     AS time,
                                             w.from_address AS staker,
                                             -SUM(w.amount) AS amount_change
                                      FROM staker_withdrawn w
                                               JOIN event_keys ek ON w.event_id = ek.id
                                               JOIN blocks b ON ek.block_number = b.number
                                      WHERE b.time < start_time
                                      GROUP BY w.from_address),

                    -- Step 4: Calculate cumulative stake
                    stake_events AS (SELECT time,
                                            staker,
                                            SUM(amount_change)
                                            OVER (PARTITION BY staker ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS stake_amount
                                     FROM stake_changes),

                    -- Step 5: For each interval and staker
                    staker_intervals AS (SELECT i.start_time,
                                                i.end_time,
                                                se.staker,
                                                se.stake_amount
                                         FROM intervals i
                                                  JOIN stake_events se ON se.time <= i.start_time
                                             AND NOT EXISTS (SELECT 1
                                                             FROM stake_events se2
                                                             WHERE se2.staker = se.staker
                                                               AND se2.time > se.time
                                                               AND se2.time <= i.start_time)),

                    -- Step 6: Total stake per interval
                    total_stake_per_interval AS (SELECT si.start_time,
                                                        si.end_time,
                                                        SUM(stake_amount) AS total_stake
                                                 FROM staker_intervals si
                                                 GROUP BY si.start_time, si.end_time),

                    -- Step 7: Compute rewards per staker
                    staker_rewards AS (SELECT si.staker,
                                              si.start_time,
                                              si.end_time,
                                              si.stake_amount,
                                              tsi.total_stake,
                                              total_rewards * ((staking_share) / (staking_share + delegate_share))
                                                  * (EXTRACT(EPOCH FROM (si.end_time - si.start_time))::NUMERIC /
                                                     p.total_duration_seconds)
                                                  * (si.stake_amount / tsi.total_stake) AS reward
                                       FROM staker_intervals si
                                                JOIN total_stake_per_interval tsi
                                                     ON si.start_time = tsi.start_time AND si.end_time = tsi.end_time,
                                            calculated_parameters p
                                       WHERE tsi.total_stake > 0
                                         AND si.stake_amount > 0
                                         AND EXTRACT(EPOCH FROM (si.end_time - si.start_time)) > 0),

                    -- Proposals and delegate rewards
                    proposals_in_period AS (SELECT gp.id
                                            FROM governor_proposed gp
                                                     JOIN event_keys ek ON gp.event_id = ek.id
                                                     JOIN blocks b ON ek.block_number = b.number
                                            WHERE b.time BETWEEN start_time AND end_time),

                    delegate_total_votes_weight AS (SELECT gv.voter AS delegate, SUM(gv.weight) AS total_weight
                                                    FROM governor_voted gv
                                                    WHERE gv.id IN (SELECT pip.id FROM proposals_in_period pip)
                                                    GROUP BY gv.voter),

                    total_votes_weight_in_period
                        AS (SELECT SUM(total_weight) AS total FROM delegate_total_votes_weight),

                    delegate_rewards AS (SELECT dtvw.delegate,
                                                dtvw.total_weight * total_rewards *
                                                (delegate_share / (staking_share + delegate_share)) /
                                                tvwp.total AS reward
                                         FROM delegate_total_votes_weight dtvw,
                                              total_votes_weight_in_period tvwp,
                                              calculated_parameters p),

                    total_staker_rewards AS (SELECT staker      AS claimee,
                                                    SUM(reward) AS reward
                                             FROM staker_rewards
                                             GROUP BY staker),

                    all_rewards AS (SELECT delegate   AS claimee,
                                           reward     AS delegate_reward,
                                           0::NUMERIC AS staker_reward
                                    FROM delegate_rewards
                                    UNION ALL
                                    SELECT tsr.claimee,
                                           0::NUMERIC AS delegate_reward,
                                           reward     AS staker_reward
                                    FROM total_staker_rewards tsr),

                    final_rewards AS (SELECT ar.claimee,
                                             SUM(staker_reward)                        AS total_staker_reward,
                                             SUM(delegate_reward)                      AS total_delegate_reward,
                                             SUM(staker_reward) + SUM(delegate_reward) AS total_reward
                                      FROM all_rewards ar
                                      GROUP BY ar.claimee)

                SELECT ROW_NUMBER() OVER (ORDER BY fr.total_reward DESC) - 1 AS id,
                       numeric_to_hex(fr.claimee)                            AS claimee,
                       FLOOR(fr.total_reward)                                AS amount,
                       FLOOR(fr.total_delegate_reward)                       AS staker_portion,
                       FLOOR(fr.total_staker_reward)                         AS delegate_portion
                FROM final_rewards fr
                WHERE fr.total_reward > 0
                ORDER BY total_reward DESC;
        END;
        $$ LANGUAGE plpgsql;

    `);
  }

  public async refreshAnalyticalTables({ since }: { since: Date }) {
    await this.pg.query({
      text: `
                INSERT INTO hourly_volume_by_token
                    (SELECT swaps.pool_key_hash                                                      AS   key_hash,
                            DATE_TRUNC('hour', blocks.time)                                          AS   hour,
                            (CASE WHEN swaps.delta0 >= 0 THEN pool_keys.token0 ELSE pool_keys.token1 END) token,
                            SUM(CASE WHEN swaps.delta0 >= 0 THEN swaps.delta0 ELSE swaps.delta1 END) AS   volume,
                            SUM(FLOOR(((CASE WHEN delta0 >= 0 THEN swaps.delta0 ELSE swaps.delta1 END) *
                                       pool_keys.fee) /
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
    });

    await this.pg.query({
      text: `
                INSERT INTO hourly_volume_by_token
                    (SELECT fa.pool_key_hash                AS key_hash,
                            DATE_TRUNC('hour', blocks.time) AS hour,
                            pool_keys.token0                AS token,
                            0                               AS volume,
                            SUM(fa.amount0)                 AS fees,
                            0                               AS swap_count
                     FROM fees_accumulated fa
                              JOIN pool_keys ON fa.pool_key_hash = pool_keys.key_hash
                              JOIN event_keys ON fa.event_id = event_keys.id
                              JOIN blocks ON event_keys.block_number = blocks.number
                     WHERE DATE_TRUNC('hour', blocks.time) >= DATE_TRUNC('hour', $1::timestamptz)
                       AND fa.amount0 > 0
                     GROUP BY hour, fa.pool_key_hash, token)
                ON CONFLICT (key_hash, hour, token)
                    DO UPDATE SET fees = excluded.fees + hourly_volume_by_token.fees;
            `,
      values: [since],
    });

    await this.pg.query({
      text: `
                INSERT INTO hourly_volume_by_token
                    (SELECT fa.pool_key_hash                AS key_hash,
                            DATE_TRUNC('hour', blocks.time) AS hour,
                            pool_keys.token1                AS token,
                            0                               AS volume,
                            SUM(fa.amount1)                 AS fees,
                            0                               AS swap_count
                     FROM fees_accumulated fa
                              JOIN pool_keys ON fa.pool_key_hash = pool_keys.key_hash
                              JOIN event_keys ON fa.event_id = event_keys.id
                              JOIN blocks ON event_keys.block_number = blocks.number
                     WHERE DATE_TRUNC('hour', blocks.time) >= DATE_TRUNC('hour', $1::timestamptz)
                       AND fa.amount1 > 0
                     GROUP BY hour, fa.pool_key_hash, token)
                ON CONFLICT (key_hash, hour, token)
                    DO UPDATE SET fees = excluded.fees + hourly_volume_by_token.fees;
            `,
      values: [since],
    });

    await this.pg.query({
      text: `
                INSERT INTO hourly_revenue_by_token
                    (WITH rev0 AS (SELECT pfp.pool_key_hash               AS key_hash,
                                          DATE_TRUNC('hour', blocks.time) AS hour,
                                          pk.token0                          token,
                                          -SUM(pfp.delta0)                AS revenue
                                   FROM protocol_fees_paid pfp
                                            JOIN pool_keys pk ON pfp.pool_key_hash = pk.key_hash
                                            JOIN event_keys ek ON pfp.event_id = ek.id
                                            JOIN blocks ON ek.block_number = blocks.number
                                   WHERE DATE_TRUNC('hour', blocks.time) >= DATE_TRUNC('hour', $1::timestamptz)
                                     AND pfp.delta0 != 0
                                   GROUP BY hour, pfp.pool_key_hash, token),
                          rev1 AS (SELECT pfp.pool_key_hash               AS key_hash,
                                          DATE_TRUNC('hour', blocks.time) AS hour,
                                          pk.token1                          token,
                                          -SUM(pfp.delta1)                AS revenue
                                   FROM protocol_fees_paid pfp
                                            JOIN pool_keys pk ON pfp.pool_key_hash = pk.key_hash
                                            JOIN event_keys ek ON pfp.event_id = ek.id
                                            JOIN blocks ON ek.block_number = blocks.number
                                   WHERE DATE_TRUNC('hour', blocks.time) >= DATE_TRUNC('hour', $1::timestamptz)
                                     AND pfp.delta1 != 0
                                   GROUP BY hour, pfp.pool_key_hash, token),
                          total AS (SELECT key_hash, hour, token, revenue
                                    FROM rev0
                                    UNION ALL
                                    SELECT key_hash, hour, token, revenue
                                    FROM rev1)
                     SELECT key_hash, hour, token, SUM(revenue) AS revenue
                     FROM total
                     GROUP BY key_hash, hour, token)
                ON CONFLICT (key_hash, hour, token)
                    DO UPDATE SET revenue = excluded.revenue;
            `,
      values: [since],
    });

    await this.pg.query({
      text: `
                INSERT INTO hourly_price_data
                    (SELECT pk.token0                                                    AS token0,
                            pk.token1                                                    AS token1,
                            date_bin(INTERVAL '1 hour', b.time,
                                     '2000-01-01 00:00:00'::TIMESTAMP WITHOUT TIME ZONE) AS hour,
                            SUM(ABS(delta1 * delta0))                                    AS k_volume,
                            SUM(delta1 * delta1)                                         AS total,
                            COUNT(1)                                                     AS swap_count
                     FROM swaps s
                              JOIN event_keys ek ON s.event_id = ek.id
                              JOIN blocks b ON ek.block_number = b.number
                              JOIN pool_keys pk ON s.pool_key_hash = pk.key_hash
                     WHERE DATE_TRUNC('hour', b.time) >= DATE_TRUNC('hour', $1::timestamptz)
                       AND delta0 != 0
                       AND delta1 != 0
                     GROUP BY pk.token0, pk.token1, hour)
                ON CONFLICT (token0, token1, hour)
                    DO UPDATE SET k_volume   = excluded.k_volume,
                                  total      = excluded.total,
                                  swap_count = excluded.swap_count;
            `,
      values: [since],
    });

    await this.pg.query({
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
                                           GROUP BY pool_key_hash, grouped_pool_key_hash_deltas.hour,
                                                    pool_keys.token0

                                           UNION ALL

                                           SELECT pool_key_hash,
                                                  grouped_pool_key_hash_deltas.hour,
                                                  pool_keys.token1 AS token,
                                                  SUM(delta1)      AS delta
                                           FROM grouped_pool_key_hash_deltas
                                                    JOIN pool_keys
                                                         ON pool_keys.key_hash = grouped_pool_key_hash_deltas.pool_key_hash
                                           GROUP BY pool_key_hash, grouped_pool_key_hash_deltas.hour,
                                                    pool_keys.token1)
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
    });

    await this.pg.query(`
      REFRESH MATERIALIZED VIEW CONCURRENTLY last_24h_pool_stats_materialized;
      REFRESH MATERIALIZED VIEW CONCURRENTLY latest_token_registrations;
      REFRESH MATERIALIZED VIEW CONCURRENTLY token_pair_realized_volatility;
    `);
  }

  public async refreshOperationalMaterializedView() {
    await this.pg.query(`
      REFRESH MATERIALIZED VIEW CONCURRENTLY pool_states_materialized;
      REFRESH MATERIALIZED VIEW CONCURRENTLY twamm_pool_states_materialized;
      REFRESH MATERIALIZED VIEW CONCURRENTLY twamm_sale_rate_deltas_materialized;
      REFRESH MATERIALIZED VIEW CONCURRENTLY oracle_pool_states_materialized;
      REFRESH MATERIALIZED VIEW CONCURRENTLY limit_order_pool_states_materialized;
    `);
  }

  private async loadCursor(): Promise<
    | {
        orderKey: bigint;
        uniqueKey: `0x${string}`;
      }
    | { orderKey: bigint }
    | null
  > {
    const { rows } = await this.pg.query({
      text: `SELECT order_key, unique_key
                   FROM cursor
                   WHERE id = 1;`,
    });
    if (rows.length === 1) {
      const { order_key, unique_key } = rows[0];

      if (BigInt(unique_key) === 0n) {
        return {
          orderKey: BigInt(order_key),
        };
      } else {
        return {
          orderKey: BigInt(order_key),
          uniqueKey: `0x${BigInt(unique_key).toString(16)}`,
        };
      }
    } else {
      return null;
    }
  }

  public async writeCursor(cursor: { orderKey: bigint; uniqueKey?: string }) {
    await this.pg.query({
      text: `
          INSERT INTO cursor (id, order_key, unique_key, last_updated)
          VALUES (1, $1, $2, NOW())
          ON CONFLICT (id) DO UPDATE SET order_key    = excluded.order_key,
                                         unique_key   = excluded.unique_key,
                                         last_updated = NOW();
      `,
      values: [cursor.orderKey, BigInt(cursor.uniqueKey ?? 0)],
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
    key: EventKey,
  ) {
    // The `*` operator is the PostgreSQL range intersection operator.
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO position_transfers
                (event_id,
                 token_id,
                 from_address,
                 to_address)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        transfer.id,
        transfer.from,
        transfer.to,
      ],
    });
  }

  public async insertPositionMintedWithReferrerEvent(
    minted: PositionMintedWithReferrer,
    key: EventKey,
  ) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO position_minted_with_referrer
                (event_id,
                 token_id,
                 referrer)
                VALUES ((SELECT id FROM inserted_event), $6, $7)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        minted.id,
        minted.referrer,
      ],
    });
  }

  public async insertPositionUpdatedEvent(
    event: PositionUpdatedEvent,
    key: EventKey,
  ) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
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
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10, $11, $12, $13);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,

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
    key: EventKey,
  ) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      name: "insert-position-fees-collected",
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
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
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10, $11, $12);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,

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
    key: EventKey,
  ) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO pool_initializations
                (event_id,
                 pool_key_hash,
                 tick,
                 sqrt_ratio)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,

        pool_key_hash,

        event.tick,
        event.sqrt_ratio,
      ],
    });
  }

  public async insertProtocolFeesWithdrawn(
    event: ProtocolFeesWithdrawnEvent,
    key: EventKey,
  ) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO protocol_fees_withdrawn
                (event_id,
                 recipient,
                 token,
                 amount)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        event.recipient,
        event.token,
        event.amount,
      ],
    });
  }

  public async insertProtocolFeesPaid(
    event: ProtocolFeesPaidEvent,
    key: EventKey,
  ) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      name: "insert-protocol-fees-paid",
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
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
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10, $11, $12);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,

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
    key: EventKey,
  ) {
    const pool_key_hash = await this.insertPoolKeyHash(event.pool_key);

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO fees_accumulated
                (event_id,
                 pool_key_hash,
                 amount0,
                 amount1)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,

        pool_key_hash,

        event.amount0,
        event.amount1,
      ],
    });
  }

  public async insertRegistration(
    event: TokenRegistrationEvent,
    key: EventKey,
  ) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO token_registrations
                (event_id,
                 address,
                 decimals,
                 name,
                 symbol,
                 total_supply)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,

        event.address,
        event.decimals,
        event.name,
        event.symbol,
        event.total_supply,
      ],
    });
  }

  public async insertRegistrationV3(
    event: TokenRegistrationEventV3,
    key: EventKey,
  ) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO token_registrations_v3
                (event_id,
                 address,
                 decimals,
                 name,
                 symbol,
                 total_supply)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,

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
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
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
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10, $11, $12);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,

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
    if (rowCount === null) throw new Error("Null row count after delete");
    return rowCount;
  }

  public async insertTWAMMOrderUpdatedEvent(
    order_updated: OrderUpdatedEvent,
    key: EventKey,
  ) {
    const { order_key } = order_updated;

    const key_hash = await this.insertPoolKeyHash(
      orderKeyToPoolKey(key, order_key),
    );

    const [sale_rate_delta0, sale_rate_delta1] =
      order_key.sell_token > order_key.buy_token
        ? [0, order_updated.sale_rate_delta]
        : [order_updated.sale_rate_delta, 0];

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys
                        (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO twamm_order_updates
                (event_id,
                 key_hash,
                 owner,
                 salt,
                 sale_rate_delta0,
                 sale_rate_delta1,
                 start_time,
                 end_time)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10, $11, $12);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,

        key_hash,

        BigInt(order_updated.owner),
        order_updated.salt,
        sale_rate_delta0,
        sale_rate_delta1,
        new Date(Number(order_key.start_time * 1000n)),
        new Date(Number(order_key.end_time * 1000n)),
      ],
    });
  }

  public async insertTWAMMOrderProceedsWithdrawnEvent(
    order_proceeds_withdrawn: OrderProceedsWithdrawnEvent,
    key: EventKey,
  ) {
    const { order_key } = order_proceeds_withdrawn;

    const key_hash = await this.insertPoolKeyHash(
      orderKeyToPoolKey(key, order_key),
    );

    const [amount0, amount1] =
      order_key.sell_token > order_key.buy_token
        ? [0, order_proceeds_withdrawn.amount]
        : [order_proceeds_withdrawn.amount, 0];

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO twamm_proceeds_withdrawals
                (event_id, key_hash, owner, salt, amount0, amount1, start_time, end_time)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10, $11, $12);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,

        key_hash,

        BigInt(order_proceeds_withdrawn.owner),
        order_proceeds_withdrawn.salt,
        amount0,
        amount1,
        new Date(Number(order_key.start_time * 1000n)),
        new Date(Number(order_key.end_time * 1000n)),
      ],
    });
  }

  public async insertTWAMMVirtualOrdersExecutedEvent(
    virtual_orders_executed: VirtualOrdersExecutedEvent,
    key: EventKey,
  ) {
    let { key: state_key } = virtual_orders_executed;

    const key_hash = await this.insertPoolKeyHash({
      token0: state_key.token0,
      token1: state_key.token1,
      fee: state_key.fee,
      tick_spacing: BigInt(MAX_TICK_SPACING),
      extension: key.emitter,
    });

    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys
                        (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO twamm_virtual_order_executions
                (event_id, key_hash, token0_sale_rate, token1_sale_rate, delta0, delta1)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10);
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,

        key_hash,
        virtual_orders_executed.token0_sale_rate,
        virtual_orders_executed.token1_sale_rate,
        virtual_orders_executed.twamm_delta.amount0,
        virtual_orders_executed.twamm_delta.amount1,
      ],
    });
  }

  async insertStakerStakedEvent(parsed: StakedEvent, key: EventKey) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO staker_staked
                (event_id,
                 from_address,
                 delegate,
                 amount)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        parsed.from,
        parsed.delegate,
        parsed.amount,
      ],
    });
  }

  async insertStakerWithdrawnEvent(parsed: WithdrawnEvent, key: EventKey) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO staker_withdrawn
                (event_id,
                 from_address,
                 delegate,
                 amount,
                 recipient)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        parsed.from,
        parsed.delegate,
        parsed.amount,
        parsed.to,
      ],
    });
  }

  async insertGovernorProposedEvent(
    parsed: GovernorProposedEvent,
    key: EventKey,
  ) {
    const query =
      parsed.calls.length > 0
        ? `
                        WITH inserted_event AS (
                            INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                                VALUES ($1, $2, $3, $4, $5)
                                RETURNING id),
                             inserted_governor_proposed AS (
                                 INSERT
                                     INTO governor_proposed
                                         (event_id, id, proposer, config_version)
                                         VALUES ((SELECT id FROM inserted_event), $6, $7, $8))
                        INSERT
                        INTO governor_proposed_calls (proposal_id, index, to_address, selector, calldata)
                        VALUES
                        ${parsed.calls
                          .map(
                            (call, ix) =>
                              `($6, ${ix}, ${call.to}, ${
                                call.selector
                              }, '{${call.calldata
                                .map((c) => c.toString())
                                .join(",")}}')`,
                          )
                          .join(",")};
                `
        : `
                        WITH inserted_event AS (
                            INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                                VALUES ($1, $2, $3, $4, $5)
                                RETURNING id)
                        INSERT
                        INTO governor_proposed
                            (event_id, id, proposer, config_version)
                        VALUES ((SELECT id FROM inserted_event), $6, $7, $8);
                `;
    await this.pg.query({
      text: query,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        parsed.id,
        parsed.proposer,
        parsed.config_version,
      ],
    });
  }

  async insertGovernorExecutedEvent(
    parsed: GovernorExecutedEvent,
    key: EventKey,
  ) {
    const query =
      parsed.result_data.length > 0
        ? `
                        WITH inserted_event AS (
                            INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                                VALUES ($1, $2, $3, $4, $5)
                                RETURNING id),
                             inserted_governor_executed AS (
                                 INSERT
                                     INTO governor_executed
                                         (event_id, id)
                                         VALUES ((SELECT id FROM inserted_event), $6))
                        INSERT
                        INTO governor_executed_results (proposal_id, index, results)
                        VALUES
                        ${parsed.result_data
                          .map(
                            (results, ix) =>
                              `($6, ${ix}, '{${results
                                .map((c) => c.toString())
                                .join(",")}}')`,
                          )
                          .join(",")};
                `
        : `

                        WITH inserted_event AS (
                            INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                                VALUES ($1, $2, $3, $4, $5)
                                RETURNING id)
                        INSERT
                        INTO governor_executed
                            (event_id, id)
                        VALUES ((SELECT id FROM inserted_event), $6)
                `;

    await this.pg.query({
      text: query,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        parsed.id,
      ],
    });
  }

  async insertGovernorVotedEvent(parsed: GovernorVotedEvent, key: EventKey) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO governor_voted
                    (event_id, id, voter, weight, yea)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        parsed.id,
        parsed.voter,
        parsed.weight,
        parsed.yea,
      ],
    });
  }

  async insertGovernorCanceledEvent(
    parsed: GovernorCanceledEvent,
    key: EventKey,
  ) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO governor_canceled
                    (event_id, id)
                VALUES ((SELECT id FROM inserted_event), $6)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        parsed.id,
      ],
    });
  }

  async insertGovernorProposalDescribedEvent(
    parsed: DescribedEvent,
    key: EventKey,
  ) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO governor_proposal_described
                    (event_id, id, description)
                VALUES ((SELECT id FROM inserted_event), $6, $7)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        parsed.id,
        // postgres does not support null characters
        parsed.description.replaceAll("\u0000", "?"),
      ],
    });
  }

  async insertGovernorReconfiguredEvent(
    parsed: GovernorReconfiguredEvent,
    key: EventKey,
  ) {
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO governor_reconfigured
                (event_id, version, voting_start_delay, voting_period, voting_weight_smoothing_duration, quorum,
                 proposal_creation_threshold, execution_delay, execution_window)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10, $11, $12, $13)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        parsed.version,
        parsed.new_config.voting_start_delay,
        parsed.new_config.voting_period,
        parsed.new_config.voting_weight_smoothing_duration,
        parsed.new_config.quorum,
        parsed.new_config.proposal_creation_threshold,
        parsed.new_config.execution_delay,
        parsed.new_config.execution_window,
      ],
    });
  }

  async insertOracleSnapshotEvent(parsed: SnapshotEvent, key: EventKey) {
    const poolKeyHash = await this.insertPoolKeyHash({
      fee: 0n,
      tick_spacing: BigInt(MAX_TICK_SPACING),
      extension: key.emitter,
      token0: parsed.token0,
      token1: parsed.token1,
    });
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO oracle_snapshots
                (event_id, key_hash, token0, token1, index, snapshot_block_timestamp, snapshot_tick_cumulative)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10, $11)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        poolKeyHash,
        parsed.token0,
        parsed.token1,
        parsed.index,
        parsed.snapshot.block_timestamp,
        parsed.snapshot.tick_cumulative,
      ],
    });
  }

  async insertOrderPlacedEvent(parsed: OrderPlacedEvent, key: EventKey) {
    const poolKeyHash = await this.insertPoolKeyHash({
      fee: 0n,
      tick_spacing: BigInt(LIMIT_ORDER_TICK_SPACING),
      extension: key.emitter,
      token0: parsed.order_key.token0,
      token1: parsed.order_key.token1,
    });
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO limit_order_placed
                (event_id, key_hash, owner, salt, token0, token1, tick, liquidity, amount)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10, $11, $12, $13)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        poolKeyHash,
        parsed.owner,
        parsed.salt,
        parsed.order_key.token0,
        parsed.order_key.token1,
        parsed.order_key.tick,
        parsed.liquidity,
        parsed.amount,
      ],
    });
  }

  async insertOrderClosedEvent(parsed: OrderClosedEvent, key: EventKey) {
    const poolKeyHash = await this.insertPoolKeyHash({
      fee: 0n,
      tick_spacing: BigInt(LIMIT_ORDER_TICK_SPACING),
      extension: key.emitter,
      token0: parsed.order_key.token0,
      token1: parsed.order_key.token1,
    });
    await this.pg.query({
      text: `
                WITH inserted_event AS (
                    INSERT INTO event_keys (block_number, transaction_index, event_index, transaction_hash, emitter)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id)
                INSERT
                INTO limit_order_closed
                (event_id, key_hash, owner, salt, token0, token1, tick, amount0, amount1)
                VALUES ((SELECT id FROM inserted_event), $6, $7, $8, $9, $10, $11, $12, $13)
            `,
      values: [
        key.blockNumber,
        key.transactionIndex,
        key.eventIndex,
        key.transactionHash,
        key.emitter,
        poolKeyHash,
        parsed.owner,
        parsed.salt,
        parsed.order_key.token0,
        parsed.order_key.token1,
        parsed.order_key.tick,
        parsed.amount0,
        parsed.amount1,
      ],
    });
  }
}
