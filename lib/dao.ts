import { Client } from "pg";
import {
  PoolKey,
  PositionMintedEvent,
  PositionUpdatedEvent,
  SwappedEvent,
  TransferEvent,
} from "./parse";
import { BlockMeta } from "./processor";
import { Cursor, v1alpha2 } from "@apibara/protocol";
import { pedersen_from_hex, pedersen_from_dec } from "pedersen-fast";
import { FieldElement } from "@apibara/starknet";

function computeKeyHash(pool_key: PositionMintedEvent["pool_key"]) {
  return pedersen_from_hex(
    pedersen_from_hex(
      pedersen_from_hex(pool_key.token0, pool_key.token1),
      pedersen_from_hex(
        `0x${pool_key.fee.toString(16)}`,
        `0x${pool_key.tick_spacing.toString(16)}`
      )
    ),
    pool_key.extension
  );
}

// Data access object that manages inserts/deletes
export class EventDAO {
  private pg: Client;

  constructor(pg: Client) {
    this.pg = pg;
  }

  public async startTransaction(): Promise<void> {
    await this.pg.query("BEGIN");
  }

  public async endTransaction(): Promise<void> {
    await this.pg.query("COMMIT");
  }

  async connectAndInit() {
    await this.pg.connect();
    await this.initSchema();
  }

  private async initSchema(): Promise<void> {
    await this.startTransaction();
    const result = await Promise.all([
      this.pg.query(`create table if not exists cursor(
          id int not null UNIQUE CHECK (id = 1), -- only one row.
          order_key numeric not null,
          unique_key text not null
      )`),

      this.pg.query(`create table if not exists pool_keys(
          key_hash text not null PRIMARY KEY,
          token0 text not null,
          token1 text not null,
          fee numeric not null,
          tick_spacing numeric not null,
          extension text not null
        )`),

      this.pg.query(`create table if not exists position_metadata(
          token_id numeric not null PRIMARY KEY,
          lower_bound numeric not null,
          upper_bound numeric not null,
        
          -- pool stuff.
          key_hash text not null REFERENCES pool_keys(key_hash),
          
          -- validity range.
          _valid int8range not null
        )`),

      this.pg.query(`create table if not exists position_updates(
          salt numeric not null,
          lower_bound numeric not null,
          upper_bound numeric not null,
          liquidity_delta numeric not null,
          delta0 numeric not null,
          delta1 numeric not null,
        
          -- pool stuff.
          key_hash text not null REFERENCES pool_keys(key_hash),
        
          -- validity range.
          _valid int8range not null
        )`),

      this.pg.query(`create table if not exists swaps(
          -- pool stuff.
          key_hash text not null REFERENCES pool_keys(key_hash),
          
          delta0 numeric not null,
          delta1 numeric not null,
        
          -- validity range.
          _valid int8range not null
        )`),
    ]);
    await this.endTransaction();
  }

  public async loadCursor() {
    const { rows } = await this.pg.query({
      name: "load-cursor",
      text: `
          SELECT order_key, unique_key FROM cursor WHERE id = 1;
        `,
    });
    if (rows.length === 1) {
      return Cursor.fromObject({
        orderKey: rows[0].order_key,
        uniqueKey: rows[0].unique_key,
      });
    } else {
      return null;
    }
  }

  public async writeCursor(cursor: v1alpha2.ICursor) {
    const stringified = Cursor.toObject(cursor);
    await this.pg.query({
      name: "write-cursor",
      text: `
          INSERT INTO cursor (id, order_key, unique_key)
          VALUES (1, $1, $2)
          ON CONFLICT (id) DO UPDATE SET order_key = $1, unique_key = $2;
      `,
      values: [Number(stringified.orderKey), stringified.uniqueKey],
    });
  }

  private async insertKeyHash(pool_key: PoolKey) {
    const key_hash = computeKeyHash(pool_key);

    await this.pg.query({
      name: "insert-key-hash",
      text: `
        insert into pool_keys (
          key_hash,
          token0,
          token1,
          fee,
          tick_spacing,
          extension
        ) values ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING;
      `,
      values: [
        key_hash,
        pool_key.token0,
        pool_key.token1,
        pool_key.fee,
        pool_key.tick_spacing,
        pool_key.extension,
      ],
    });
    return key_hash;
  }

  public async insertPositionMetadata(
    token: PositionMintedEvent,
    meta: BlockMeta
  ) {
    const key_hash = await this.insertKeyHash(token.pool_key);

    await this.pg.query({
      name: "insert-token",
      text: `
      insert into position_metadata (
        token_id,
        lower_bound,
        upper_bound,
        key_hash,
        _valid
      ) values ($1, $2, $3, $4, $5) 
        ON CONFLICT (token_id)
            DO UPDATE 
            SET lower_bound = $2,
                upper_bound = $3,
                key_hash = $4,
                _valid = $5
      `,
      values: [
        token.token_id,
        token.bounds.lower,
        token.bounds.upper,
        key_hash,
        `[${meta.blockNumber},)`,
      ],
    });
  }

  public async deletePositionMetadata(token: TransferEvent, meta: BlockMeta) {
    // The `*` operator is the PostgreSQL range intersection operator.
    await this.pg.query({
      name: "delete-token",
      text: `
      update position_metadata
      set
        _valid = _valid * $1::int8range
      where
        token_id = $2;
      `,
      values: [`[,${meta.blockNumber})`, token.token_id],
    });
  }

  public async insertPositionUpdated(
    event: PositionUpdatedEvent,
    block: BlockMeta
  ) {
    const key_hash = await this.insertKeyHash(event.pool_key);

    await this.pg.query({
      name: "insert-position",
      text: `
      INSERT INTO position_updates (
        salt,
        lower_bound,
        upper_bound,
        liquidity_delta,
        delta0,
        delta1,
        key_hash,
        _valid
      ) values ($1, $2, $3, $4, $5, $6, $7, $8::int8range);
      `,
      values: [
        event.params.salt,
        event.params.bounds.lower,
        event.params.bounds.upper,
        event.params.liquidity_delta,
        event.delta.amount0,
        event.delta.amount1,
        key_hash,
        `[${block.blockNumber},)`,
      ],
    });
  }

  public async insertSwappedEvent(event: SwappedEvent, meta: BlockMeta) {
    const key_hash = await this.insertKeyHash(event.pool_key);

    await this.pg.query({
      name: "insert-swapped",
      text: `
      INSERT INTO swaps (
        key_hash,
        delta0,
        delta1,
        _valid
      ) values ($1, $2, $3, $4);
      `,
      values: [
        key_hash,
        event.delta.amount0,
        event.delta.amount1,
        `[${meta.blockNumber},)`,
      ],
    });
  }

  private async invalidate(
    table: "swaps" | "position_updates" | "position_metadata",
    invalidatedBlockNumber: bigint
  ) {
    await this.pg.query({
      name: "invalidate",
      text: `
        DELETE FROM ${table}
        WHERE LOWER(_valid) >= $1;
      `,
      values: [invalidatedBlockNumber],
    });
    await this.pg.query({
      name: "update-upper-bounds",
      text: `
      UPDATE ${table}
        SET _valid = int8range(LOWER(_valid), 'infinity'::int8)
        WHERE UPPER(_valid) >= $1;
      `,
      values: [invalidatedBlockNumber],
    });
  }

  public async invalidateBlockNumber(invalidatedBlockNumber: bigint) {
    await Promise.all([
      this.invalidate("swaps", invalidatedBlockNumber),
      this.invalidate("position_updates", invalidatedBlockNumber),
      this.invalidate("position_metadata", invalidatedBlockNumber),
    ]);
  }

  public async close() {
    await this.pg.end();
  }
}
