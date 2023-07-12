import { Client } from "pg";
import {
  PoolKey,
  PositionMintedEvent,
  PositionUpdatedEvent,
  SwappedEvent,
  TransferEvent,
} from "./parse";
import { pedersen_from_hex } from "pedersen-fast";
import { EventKey } from "./processor";

function toHex(x: bigint): string {
  return `0x${x.toString(16)}`;
}

function computeKeyHash(pool_key: PositionMintedEvent["pool_key"]): bigint {
  return BigInt(
    pedersen_from_hex(
      pedersen_from_hex(
        pedersen_from_hex(toHex(pool_key.token0), toHex(pool_key.token1)),
        pedersen_from_hex(toHex(pool_key.fee), toHex(pool_key.tick_spacing))
      ),
      toHex(pool_key.extension)
    )
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
    await this.startTransaction();
    await this.initSchema();
    const cursor = await this.loadCursor();
    // we need to clear anything that was potentially inserted as pending before starting
    if (cursor) {
      await this.invalidateBlockNumber(BigInt(cursor.orderKey) + 1n);
    }
    await this.endTransaction();
    return cursor;
  }

  private async initSchema(): Promise<void> {
    const result = await Promise.all([
      this.pg.query(`CREATE TABLE IF NOT EXISTS cursor(
        id INT NOT NULL UNIQUE CHECK (id = 1), -- only one row.
        order_key NUMERIC NOT NULL,
        unique_key TEXT NOT NULL
      )`),

      this.pg.query(`CREATE TABLE IF NOT EXISTS pool_keys(
        key_hash NUMERIC NOT NULL PRIMARY KEY,
        token0 NUMERIC NOT NULL,
        token1 NUMERIC NOT NULL,
        fee NUMERIC NOT NULL,
        tick_spacing NUMERIC NOT NULL,
        extension NUMERIC NOT NULL
      )`),

      this.pg.query(`CREATE TABLE IF NOT EXISTS position_metadata(
        token_id NUMERIC NOT NULL PRIMARY KEY,
        lower_bound NUMERIC NOT NULL,
        upper_bound NUMERIC NOT NULL,
        
        pool_key_hash NUMERIC NOT NULL REFERENCES pool_keys(key_hash),
        
        -- validity range.
        _valid int8range NOT NULL
      )`),

      this.pg.query(`CREATE TABLE IF NOT EXISTS position_updates(
        transaction_hash NUMERIC NOT NULL,
        block_number INT8 NOT NULL,
        index INT8 NOT NULL,
    
        pool_key_hash NUMERIC NOT NULL REFERENCES pool_keys(key_hash),
    
        salt NUMERIC NOT NULL,
        lower_bound NUMERIC NOT NULL,
        upper_bound NUMERIC NOT NULL,
        
        liquidity_delta NUMERIC NOT NULL,
        delta0 NUMERIC NOT NULL,
        delta1 NUMERIC NOT NULL,
        
        PRIMARY KEY (transaction_hash, block_number, index)
      )`),

      this.pg.query(`CREATE TABLE IF NOT EXISTS swaps(
          transaction_hash NUMERIC NOT NULL,
          block_number INT8 NOT NULL,
          index INT8 NOT NULL,
          
          pool_key_hash NUMERIC NOT NULL REFERENCES pool_keys(key_hash),
          
          delta0 NUMERIC NOT NULL,
          delta1 NUMERIC NOT NULL,
          
          PRIMARY KEY (transaction_hash, block_number, index)
        );`),
    ]);
  }

  private async loadCursor(): Promise<{
    orderKey: string;
    uniqueKey: string;
  } | null> {
    const { rows } = await this.pg.query({
      name: "load-cursor",
      text: `
          SELECT order_key, unique_key FROM cursor WHERE id = 1;
        `,
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
      name: "write-cursor",
      text: `
          INSERT INTO cursor (id, order_key, unique_key)
          VALUES (1, $1, $2)
          ON CONFLICT (id) DO UPDATE SET order_key = $1, unique_key = $2;
      `,
      values: [BigInt(cursor.orderKey), cursor.uniqueKey],
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
        BigInt(pool_key.token0),
        BigInt(pool_key.token1),
        pool_key.fee,
        pool_key.tick_spacing,
        BigInt(pool_key.extension),
      ],
    });
    return key_hash;
  }

  public async setPositionMetadata(
    token: PositionMintedEvent,
    blockNumber: bigint
  ) {
    const pool_key_hash = await this.insertKeyHash(token.pool_key);

    await this.pg.query({
      name: "insert-token",
      text: `
      insert into position_metadata (
        token_id,
        lower_bound,
        upper_bound,
        pool_key_hash,
        _valid
      ) values ($1, $2, $3, $4, $5); 
      `,
      values: [
        token.token_id,
        token.bounds.lower,
        token.bounds.upper,
        pool_key_hash,
        `[${blockNumber},)`,
      ],
    });
  }

  public async deletePositionMetadata(
    token: TransferEvent,
    blockNumber: bigint
  ) {
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
      values: [`[,${blockNumber})`, token.token_id],
    });
  }

  public async insertPositionUpdatedEvent(
    event: PositionUpdatedEvent,
    key: EventKey
  ) {
    const pool_key_hash = await this.insertKeyHash(event.pool_key);

    await this.pg.query({
      name: "insert-position",
      text: `
      INSERT INTO position_updates (
        transaction_hash,
        block_number,
        index,

        pool_key_hash,

        salt,
        lower_bound,
        upper_bound,

        liquidity_delta,
        delta0,
        delta1
      ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
      `,
      values: [
        key.txHash,
        key.blockNumber,
        key.logIndex,

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

  public async insertSwappedEvent(event: SwappedEvent, key: EventKey) {
    const pool_key_hash = await this.insertKeyHash(event.pool_key);

    await this.pg.query({
      name: "insert-swapped",
      text: `
      INSERT INTO swaps (
        transaction_hash,
        block_number,
        index,

        pool_key_hash,

        delta0,
        delta1
      ) values ($1, $2, $3, $4, $5, $6);
      `,
      values: [
        key.txHash,
        key.blockNumber,
        key.logIndex,

        pool_key_hash,

        event.delta.amount0,
        event.delta.amount1,
      ],
    });
  }

  private async invalidatePositionMetadata(invalidatedBlockNumber: bigint) {
    await this.pg.query({
      name: "invalidate-position-metadata",
      text: `
        DELETE FROM position_metadata
        WHERE LOWER(_valid) >= $1;
      `,
      values: [invalidatedBlockNumber],
    });
    await this.pg.query({
      name: "update-position-metadata-upper-bounds",
      text: `
      UPDATE position_metadata
        SET _valid = int8range(LOWER(_valid), NULL)
        WHERE UPPER(_valid) >= $1;
      `,
      values: [invalidatedBlockNumber],
    });
  }

  private async deleteFromTableWithBlockNumber(
    table: "swaps" | "position_updates",
    invalidatedBlockNumber: bigint
  ): Promise<void> {
    await this.pg.query({
      name: `delete-${table}-after-block-number`,
      text: `
        DELETE FROM ${table}
        WHERE block_number >= $1;
      `,
      values: [invalidatedBlockNumber],
    });
  }

  public async invalidateBlockNumber(invalidatedBlockNumber: bigint) {
    await Promise.all([
      this.invalidatePositionMetadata(invalidatedBlockNumber),
      this.deleteFromTableWithBlockNumber("swaps", invalidatedBlockNumber),
      this.deleteFromTableWithBlockNumber(
        "position_updates",
        invalidatedBlockNumber
      ),
    ]);
  }

  public async close() {
    await this.pg.end();
  }
}
