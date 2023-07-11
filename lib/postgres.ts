import { Client } from "pg";


export class PostgresClient {
  private pg: Client

  constructor() {
    this.pg = new Client()
  }

  async connect() {
    await this.pg.connect();
  }

  async insertToken(token, meta) {
    const query = {
      name: 'insert-token',
      text: `
      insert into tokens (
        token_id,
        lower_bound,
        upper_bound,
        pool_extension,
        pool_fee,
        pool_tick_spacing,
        pool_token0,
        pool_token1,
        _valid
      ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9::int8range);
      `,
      values: [
        token.token_id,
        token.bounds.lower,
        token.bounds.upper,
        token.pool_key.extension,
        token.pool_key.fee,
        token.pool_key.tick_spacing,
        token.pool_key.token0,
        token.pool_key.token1,
        `[${meta.blockNumber},)`,
      ]
    };
    await this.pg.query(query);
  }

  async deleteToken(token, meta) {
    // The `*` operator is the PostgreSQL range intersection operator.
    const query = {
      name: 'delete-token',
      text: `
      update tokens
      set
        _valid = _valid * $1::int8range
      where
        token_id = $2;
      `,
      values: [
        `[,${meta.blockNumber})`,
        token.token_id,
      ]
    };
    await this.pg.query(query);
  }

  async insertPosition(pos, meta) {
    const query = {
      name: 'insert-position',
      text: `
      insert into positions (
        salt,
        lower_bound,
        upper_bound,
        liquidity_delta,
        delta0,
        delta1,
        pool_extension,
        pool_fee,
        pool_tick_spacing,
        pool_token0,
        pool_token1,
        _valid
      ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::int8range);
      `,
      values: [
         pos.params.salt,
         pos.params.bounds.lower,
         pos.params.bounds.upper,
         pos.params.liquidity_delta,
         pos.delta.amount0,
         pos.delta.amount1,
         pos.pool_key.extension,
         pos.pool_key.fee,
         pos.pool_key.tick_spacing,
         pos.pool_key.token0,
         pos.pool_key.token1,
        `[${meta.blockNumber},)`,
      ]
    };
    await this.pg.query(query);
  }
}

