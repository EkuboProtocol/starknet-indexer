import "./config";
import {
  FieldElement,
  Filter,
  StarkNetCursor,
  v1alpha2 as starknet,
} from "@apibara/starknet";
import { Cursor, StreamClient, v1alpha2 } from "@apibara/protocol";
import { EventKey } from "./processor";
import { logger } from "./logger";
import { DAO } from "./dao";
import { Pool } from "pg";
import { throttle } from "tadaaa";
import { positionsContract } from "./positions";
import { EVENT_PROCESSORS } from "./EVENT_PROCESSORS";
import PQueue from "p-queue-cjs";
import {
  FeesPaidEvent,
  FeesWithdrawnEvent,
  PositionFeesCollectedEvent,
} from "./events/core";

const pool = new Pool({
  connectionString: process.env.PG_CONNECTION_STRING,
});

const client = new StreamClient({
  url: process.env.APIBARA_URL,
  token: process.env.APIBARA_AUTH_TOKEN,
});

export function parseLong(long: number | Long): bigint {
  return BigInt(typeof long === "number" ? long : long.toNumber());
}

const refreshAnalyticalViews = throttle(
  async function () {
    const start = process.hrtime.bigint();
    logger.info("Started refreshing analytical views", {
      timestamp: new Date().toISOString(),
    });
    const client = await pool.connect();
    const dao = new DAO(client);
    await dao.refreshAnalyticalMaterializedViews();
    client.release();
    logger.info("Refreshed analytical views", {
      timestamp: new Date().toISOString(),
      processTimeMs: `${(process.hrtime.bigint() - start) / 1_000_000n}ms`,
    });
  },
  {
    delay: parseInt(process.env.REFRESH_RATE_ANALYTICAL_VIEWS),
    leading: true,
    async onError(err) {
      logger.error("Failed to refresh analytical views", err);
    },
  }
);

const MAX_POSTGRES_SMALLINT = 32767;

const refreshLeaderboard = throttle(
  async function () {
    const start = process.hrtime.bigint();
    logger.info("Started refreshing leaderboard", {
      timestamp: new Date().toISOString(),
    });
    const client = await pool.connect();

    const {
      rows: [
        { id: latestEventId, block_number: blockNumber, transaction_index },
      ],
    } = await client.query<{
      id: string;
      block_number: number;
      transaction_index: number;
    }>(`
            SELECT id, block_number, transaction_index
            FROM event_keys
            WHERE block_number != (SELECT block_number FROM event_keys ORDER BY id DESC LIMIT 1)
            ORDER BY id DESC
            LIMIT 1
        `);

    logger.debug("Leaderboard refresh starting at block number", {
      latestEventId,
      blockNumber,
    });

    // get all the active positions
    const { rows: positions } = await client.query<{
      token_id: string;
      owner: string;
      token0: string;
      token1: string;
      fee: string;
      tick_spacing: number;
      extension: string;
      lower_bound: number;
      upper_bound: number;
    }>(`
            SELECT token_id,
                   owner,
                   token0,
                   token1,
                   fee,
                   tick_spacing,
                   extension,
                   lower_bound,
                   upper_bound
            FROM position_minted AS pm
                     LEFT JOIN LATERAL (
                SELECT to_address AS owner
                FROM position_transfers AS pt
                WHERE pt.token_id = pm.token_id
                  AND event_id <= ${latestEventId}
                ORDER BY event_id DESC
                LIMIT 1
                ) ON TRUE
                     JOIN pool_keys AS pk ON pm.pool_key_hash = pk.key_hash
            WHERE owner != 0
        `);

    logger.debug(
      `Getting position information for ${positions.length} positions`
    );

    const CHUNK_SIZE = 200;

    const chunks = Array(Math.ceil(positions.length / CHUNK_SIZE))
      .fill(null)
      .map((_, ix) => {
        return positions
          .slice(ix * CHUNK_SIZE, ix * CHUNK_SIZE + CHUNK_SIZE)
          .map((p) => ({
            id: BigInt(p.token_id),
            pool_key: {
              token0: BigInt(p.token0),
              token1: BigInt(p.token1),
              fee: BigInt(p.fee),
              tick_spacing: p.tick_spacing,
              extension: BigInt(p.extension),
            },
            bounds: {
              lower: { mag: Math.abs(p.lower_bound), sign: p.lower_bound < 0 },
              upper: { mag: Math.abs(p.upper_bound), sign: p.upper_bound < 0 },
            },
          }));
      });

    logger.info(
      `Leaderboard query needs ${chunks.length} chunks at block number ${blockNumber}`
    );

    const queue = new PQueue({ concurrency: 20 });

    const allPositionTokenInfos = await Promise.all(
      chunks.map((chunk, ix) =>
        queue.add(() => {
          logger.debug("Loading chunk for leaderboard", {
            chunkIndex: ix,
          });
          return positionsContract.call("get_tokens_info", [chunk], {
            blockIdentifier: blockNumber,
          });
        })
      )
    );

    logger.info(`Positions state fetched, starting leaderboard table refresh`);

    const { feeWithdrawnEvents, protocolFeesPaidEvents } =
      allPositionTokenInfos.reduce<{
        feeWithdrawnEvents: PositionFeesCollectedEvent[];
        protocolFeesPaidEvents: FeesPaidEvent[];
      }>(
        (
          memo,
          {
            fees0,
            fees1,
            amount0,
            amount1,
          }: {
            amount0: bigint;
            amount1: bigint;
            fees0: bigint;
            fees1: bigint;
          },
          ix
        ) => {
          const position = positions[ix];
          const pool_key = {
            token0: BigInt(position.token0),
            token1: BigInt(position.token1),
            fee: BigInt(position.fee),
            tick_spacing: BigInt(position.tick_spacing),
            extension: BigInt(position.extension),
          };
          const position_key = {
            bounds: {
              lower: BigInt(position.lower_bound),
              upper: BigInt(position.lower_bound),
            },
            owner: BigInt(positionsContract.address),
            salt: BigInt(position.token_id),
          };

          if (fees0 > 0n || fees1 > 0n) {
            memo.feeWithdrawnEvents.push({
              pool_key,
              position_key,
              delta: {
                amount0: fees0,
                amount1: fees1,
              },
            });
          }

          if (amount0 > 0n || amount1 > 0n) {
            const protocolFees0 = (amount0 * pool_key.fee) / (1n << 128n);
            const protocolFees1 = (amount1 * pool_key.fee) / (1n << 128n);
            if (protocolFees0 > 0n || protocolFees1 > 0n)
              memo.protocolFeesPaidEvents.push({
                pool_key,
                position_key,
                delta: {
                  amount0: protocolFees0,
                  amount1: protocolFees1,
                },
              });
          }
          return memo;
        },
        {
          feeWithdrawnEvents: [],
          protocolFeesPaidEvents: [],
        }
      );

    const transactionHash = 0n;

    let transactionIndex = transaction_index + 1;
    let nextEventIndex = 0;

    function nextEventKey() {
      if (nextEventIndex >= MAX_POSTGRES_SMALLINT) {
        nextEventIndex = 0;
        transactionIndex++;
      }
      if (transactionIndex >= MAX_POSTGRES_SMALLINT) {
        throw new Error("Event key too large");
      }
      return {
        transactionIndex,
        blockNumber,
        transactionHash,
        eventIndex: nextEventIndex++,
      };
    }

    const dao = new DAO(client);

    await dao.beginTransaction();

    await Promise.all(
      feeWithdrawnEvents
        .map((event) =>
          dao.insertPositionFeesCollectedEvent(event, nextEventKey())
        )
        .concat(
          protocolFeesPaidEvents.map((event) =>
            dao.insertProtocolFeesPaid(event, nextEventKey())
          )
        )
    );

    await dao.refreshLeaderboard(blockNumber);

    await dao.deleteFakeLeaderboardEvents(blockNumber);

    await dao.commitTransaction();

    client.release();

    logger.info("Refreshed leaderboard", {
      timestamp: new Date().toISOString(),
      processTimeMs: `${(process.hrtime.bigint() - start) / 1_000_000n}ms`,
    });
  },
  {
    delay: parseInt(process.env.REFRESH_RATE_LEADERBOARD),
    leading: true,
    async onError(err) {
      logger.error("Failed to refresh leaderboard", err);
    },
  }
);

(async function () {
  // first set up the schema
  let databaseStartingCursor;
  {
    const client = await pool.connect();
    databaseStartingCursor = await new DAO(client).initializeSchema();
    client.release();
  }

  logger.info(`Initialized`, {
    startingCursor: databaseStartingCursor,
  });

  client.configure({
    filter: EVENT_PROCESSORS.reduce((memo, value) => {
      return memo.addEvent((ev) =>
        ev
          .withKeys(value.filter.keys)
          .withIncludeReceipt(true)
          .withFromAddress(value.filter.fromAddress)
      );
    }, Filter.create().withHeader({ weak: true })).encode(),
    batchSize: 1,
    finality: v1alpha2.DataFinality.DATA_STATUS_PENDING,
    cursor: databaseStartingCursor
      ? Cursor.fromObject(databaseStartingCursor)
      : StarkNetCursor.createWithBlockNumber(
          Number(process.env.STARTING_CURSOR_BLOCK_NUMBER ?? 0)
        ),
  });

  for await (const message of client) {
    const start = process.hrtime.bigint();

    let messageType = !!message.heartbeat
      ? "heartbeat"
      : !!message.invalidate
      ? "invalidate"
      : !!message.data
      ? "data"
      : "unknown";

    switch (messageType) {
      case "data": {
        if (!message.data.data) {
          logger.error(`Data message is empty`);
          break;
        } else {
          const client = await pool.connect();
          const dao = new DAO(client);

          await dao.beginTransaction();

          let isPending: boolean = false;

          let deletedCount: number = 0;

          for (const encodedBlockData of message.data.data) {
            const block = starknet.Block.decode(encodedBlockData);

            const blockNumber = Number(parseLong(block.header.blockNumber));
            deletedCount += await dao.deleteOldBlockNumbers(blockNumber);

            // for pending blocks we update operational materialized views before we commit
            isPending =
              isPending || FieldElement.toBigInt(block.header.blockHash) === 0n;

            const events = block.events;

            const blockTime = new Date(
              Number(parseLong(block.header.timestamp.seconds) * 1000n)
            );
            await dao.insertBlock({
              hash: FieldElement.toBigInt(block.header.blockHash),
              number: parseLong(block.header.blockNumber),
              time: blockTime,
            });

            for (
              let blockEventsIndex = 0;
              blockEventsIndex < events.length;
              blockEventsIndex++
            ) {
              const { event, transaction, receipt } = events[blockEventsIndex];

              const eventKey: EventKey = {
                blockNumber,
                transactionHash: FieldElement.toBigInt(transaction.meta.hash),
                transactionIndex: Number(parseLong(receipt.transactionIndex)),
                eventIndex: Number(parseLong(event.index)),
              };

              // process each event sequentially through all the event processors in parallel
              // assumption is that none of the event processors operate on the same events, i.e. have the same filters
              // this assumption could be validated at runtime
              await Promise.all(
                EVENT_PROCESSORS.map(async ({ parser, handle, filter }) => {
                  if (
                    FieldElement.toBigInt(event.fromAddress) ===
                      FieldElement.toBigInt(filter.fromAddress) &&
                    event.keys.length === filter.keys.length &&
                    event.keys.every(
                      (key, ix) =>
                        FieldElement.toBigInt(key) ===
                        FieldElement.toBigInt(filter.keys[ix])
                    )
                  ) {
                    const parsed = parser(event.data, 0).value;

                    await handle(dao, {
                      parsed: parsed as any,
                      key: eventKey,
                    });
                  }
                })
              );

              logger.debug("Processed item", {
                blockNumber,
              });
            }

            await dao.writeCursor(Cursor.toObject(message.data.cursor));

            // refresh operational views at the end of the batch
            if (isPending || deletedCount > 0) {
              await dao.refreshOperationalMaterializedView();
            }

            await dao.commitTransaction();

            const processTimeNanos = process.hrtime.bigint() - start;
            logger.info(`Processed to block`, {
              blockNumber,
              isPending,
              blockTimestamp: blockTime,
              lagMilliseconds: Math.floor(
                Date.now() - Number(blockTime.getTime())
              ),
              processTime: `${(processTimeNanos / 1_000_000n).toString()}ms`,
            });
          }

          client.release();

          if (isPending) {
            refreshAnalyticalViews();
            refreshLeaderboard();
          }
        }

        break;
      }

      case "heartbeat": {
        logger.info(`Heartbeat`);
        break;
      }

      case "invalidate": {
        let invalidatedCursor = Cursor.toObject(message.invalidate.cursor);

        logger.warn(`Invalidated cursor`, {
          cursor: invalidatedCursor,
        });

        const client = await pool.connect();
        const dao = new DAO(client);

        await dao.beginTransaction();
        await dao.deleteOldBlockNumbers(Number(invalidatedCursor.orderKey) + 1);
        await dao.writeCursor(Cursor.toObject(message.invalidate.cursor));
        await dao.commitTransaction();

        client.release();
        break;
      }

      case "unknown": {
        logger.error(`Unknown message type`);
        break;
      }
    }
  }
})()
  .then(() => {
    logger.info("Stream closed gracefully");
  })
  .catch((error) => {
    logger.error(error);
    process.exit(1);
  });
