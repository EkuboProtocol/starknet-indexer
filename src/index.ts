import "./config";
import { Filter, StarknetStream } from "@apibara/starknet";
import { createClient } from "@apibara/protocol";
import type { EventKey } from "./processor";
import { logger } from "./logger";
import { DAO } from "./dao";
import { Pool } from "pg";
import { throttle } from "tadaaa";
import { EVENT_PROCESSORS } from "./eventProcessors";

const pool = new Pool({
  connectionString: process.env.PG_CONNECTION_STRING,
  connectionTimeoutMillis: 1000,
});

const streamClient = createClient(StarknetStream, process.env["APIBARA_URL"]!);

function millisecondsToHumanReadable(milliseconds: number): string {
  if (milliseconds < 10000) {
    return `${milliseconds / 1000}ms`;
  }
  if (milliseconds < 240000) {
    return `${Math.floor(milliseconds / 1000)}s`;
  }
  if (milliseconds < 86400000) {
    return `${Math.floor(milliseconds / 60000)} min, ${millisecondsToHumanReadable(milliseconds % 60000)}`;
  }
  return `${Math.floor(milliseconds / 86400000)}d, ${millisecondsToHumanReadable(milliseconds % 86400000)}`;
}

const refreshAnalyticalTables = throttle(
  async function (
    since: Date = new Date(
      Date.now() - parseInt(process.env["REFRESH_RATE_ANALYTICAL_VIEWS"]!) * 2,
    ),
  ) {
    const timer = logger.startTimer();
    logger.info("Started refreshing analytical tables", {
      start: timer.start,
      since: since.toISOString(),
    });
    const client = await pool.connect();
    const dao = new DAO(client);
    await dao.beginTransaction();
    await dao.refreshAnalyticalTables({
      since,
    });
    await dao.commitTransaction();
    client.release();
    timer.done({
      message: "Refreshed analytical tables",
      since: since.toISOString(),
    });
  },
  {
    delay: parseInt(process.env["REFRESH_RATE_ANALYTICAL_VIEWS"]!),
    leading: true,
    async onError(err) {
      logger.error("Failed to refresh analytical tables", err);
    },
  },
);

(async function () {
  // first set up the schema
  let databaseStartingCursor;
  {
    const client = await pool.connect();
    const dao = new DAO(client);

    const initializeTimer = logger.startTimer();
    databaseStartingCursor = await dao.initializeSchema();
    initializeTimer.done({
      message: "Initialized schema",
      startingCursor: databaseStartingCursor,
    });
    client.release();
  }

  refreshAnalyticalTables(new Date(0));

  for await (const message of streamClient.streamData({
    filter: [
      Filter.make({
        header: "always",
        events: EVENT_PROCESSORS.map((ep, ix) => ({
          id: ix + 1,
          address: ep.filter.fromAddress,
          keys: ep.filter.keys,
        })),
      }),
    ],
    finality: "pending",
    startingCursor: databaseStartingCursor
      ? databaseStartingCursor
      : { orderKey: BigInt(process.env.STARTING_CURSOR_BLOCK_NUMBER ?? 0) },
  })) {
    switch (message._tag) {
      case "heartbeat": {
        logger.info(`Heartbeat`);
        break;
      }

      case "systemMessage": {
        switch (message.systemMessage.output?._tag) {
          case "stderr":
            logger.error(`System message: ${message.systemMessage.output}`);
            break;
          case "stdout":
            logger.info(`System message: ${message.systemMessage.output}`);
            break;
        }
        break;
      }

      case "invalidate": {
        let invalidatedCursor = message.invalidate.cursor;

        if (invalidatedCursor) {
          logger.warn(`Invalidated cursor`, {
            cursor: invalidatedCursor,
          });

          const client = await pool.connect();
          const dao = new DAO(client);

          await dao.beginTransaction();
          await dao.deleteOldBlockNumbers(
            Number(invalidatedCursor.orderKey) + 1,
          );
          await dao.writeCursor(invalidatedCursor);
          await dao.commitTransaction();

          client.release();
        }

        break;
      }

      case "data": {
        const blockProcessingTimer = logger.startTimer();

        const client = await pool.connect();
        const dao = new DAO(client);

        await dao.beginTransaction();

        let isPending: boolean = false;

        let deletedCount: number = 0;

        let eventsProcessed: number = 0;
        for (const block of message.data.data) {
          const blockNumber = Number(block!.header!.blockNumber);
          deletedCount += await dao.deleteOldBlockNumbers(blockNumber);

          const blockTime = block!.header!.timestamp!;

          // for pending blocks we update operational materialized views before we commit
          isPending =
            isPending ||
            BigInt(block!.header!.blockHash!) === 0n ||
            // blocks in the last 5 minutes are considered pending
            blockTime.getTime() > Date.now() - 300_000;

          await dao.insertBlock({
            hash: BigInt(block!.header!.blockHash!),
            number: block!.header!.blockNumber,
            time: blockTime,
          });

          for (const event of block!.events) {
            const eventKey: EventKey = {
              blockNumber,
              transactionIndex: event.transactionIndex!,
              eventIndex: event.eventIndex!,
              emitter: BigInt(event.address!),
              transactionHash: BigInt(event.transactionHash!),
            };

            // process each event sequentially through all the event processors in parallel
            // assumption is that none of the event processors operate on the same events, i.e. have the same filters
            // this assumption could be validated at runtime
            await Promise.all(
              event.filterIds!.map(async (matchingFilterId) => {
                eventsProcessed++;
                const { parser, handle } =
                  EVENT_PROCESSORS[matchingFilterId - 1];
                const parsed = parser(event.data!, 0).value;

                await handle(dao, {
                  parsed: parsed as any,
                  key: eventKey,
                });
              }),
            );
          }

          await dao.writeCursor(message.data.cursor!);

          // refresh operational views at the end of the batch
          if (isPending || deletedCount > 0) {
            await dao.refreshOperationalMaterializedView();
          }

          await dao.commitTransaction();

          blockProcessingTimer.done({
            message: `Processed to block`,
            blockNumber,
            isPending,
            eventsProcessed,
            blockTimestamp: blockTime,
            lag: millisecondsToHumanReadable(
              Math.floor(Date.now() - Number(blockTime.getTime())),
            ),
          });
        }

        client.release();

        if (isPending) {
          refreshAnalyticalTables();
        }

        break;
      }

      default: {
        logger.error(`Unhandled message type: ${message._tag}`);
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
