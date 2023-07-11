-- supabase migration
--
-- run locally with:
-- > supabase start # starts supabase locally
-- > supabase migration up # runs migrations
-- > open ui at http://localhost:54323
-- (there's also a command to sync with the hosted version)
--
--
-- store entities in a way that can easily handle reorgs.
-- the main idea is that together which each entity's state we store in which
-- block range that state is valid.
--
-- when we insert a new entity, we set the range as [blockNumber, +inf).
--
-- ? blockNumber = 1234;
-- > insert 0xA with state 0x9;
-- +----------------+-----+----------+
-- | valid          | id  | state    |
-- +----------------+-----+----------+
-- | [1234,)        | 0xA | 0x9      |
-- +----------------+-----+----------+
--
-- when updating an entity, we set any previous open ranges to [blockNumber,
-- currentBlockNumber) and insert the new state.
--
-- ? blockNumber = 1500;
-- > update 0xA set state 0x8;
-- +----------------+-----+----------+
-- | valid          | id  | state    |
-- +----------------+-----+----------+
-- | [1234,1500)    | 0xA | 0x9      |
-- | [1500,)        | 0xA | 0x8      |
-- +----------------+-----+----------+
--
-- when deleting an entity, we set any previous open ranges to [blockNumber,
-- currentBlockNumber).
--
-- ? blockNumber = 1600;
-- > delete 0xA;
-- +----------------+-----+----------+
-- | valid          | id  | state    |
-- +----------------+-----+----------+
-- | [1234,1500)    | 0xA | 0x9      |
-- | [1500,1600)    | 0xA | 0x8      |
-- +----------------+-----+----------+
--
--
-- when querying for an entity, we select the most recent state that has ub +inf.
--
-- ? blockNumber = 1700;
-- > select 0xA;
-- => select * from my_table where id = 0xA and upper_inf(valid);
--
-- what's interesting about this approach is that we can easily query for the
-- state of an entity at any point in time.
-- using postgres rangetypes results in very clean queries. in the following example
-- `valid` is an `int8range`.
--
-- ? blockNumber = 1300;
-- > select 0xA;
-- => select * from my_table where id = 0xA and valid @> 1300;

-- NOTE: where it makes sense i encode felt as text because i'm too lazy to
-- convert back and forth to hex.
-- NOTE: it's a good idea to add a block_timestamp and block_number column to
-- make queries like "transfers in the past day" trivial.

-- token table.
create table if not exists tokens(
  token_id numeric,
  lower_bound numeric,
  upper_bound numeric,

  -- pool stuff.
  pool_extension text,
  pool_fee numeric,
  pool_tick_spacing numeric,
  pool_token0 text,
  pool_token1 text,
  
  -- validity range.
  _valid int8range not null
);

-- positions.
create table if not exists positions(
  salt numeric,
  lower_bound numeric,
  upper_bound numeric,
  liquidity_delta numeric,
  delta0 numeric,
  delta1 numeric,

  -- pool stuff.
  pool_extension text,
  pool_fee numeric,
  pool_tick_spacing numeric,
  pool_token0 text,
  pool_token1 text,

  -- validity range.
  _valid int8range not null
);
