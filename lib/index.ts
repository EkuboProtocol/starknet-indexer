import "./config";
import {
  FieldElement,
  Filter,
  StarkNetCursor,
  v1alpha2 as starknet,
} from "@apibara/starknet";
import { Cursor, StreamClient, v1alpha2 } from "@apibara/protocol";
import {
  parseLong,
  parsePositionMintedEvent,
  parsePositionUpdatedEvent,
  parseSwappedEvent,
  parseTransferEvent,
  PositionMintedEvent,
  PositionUpdatedEvent,
  SwappedEvent,
  TransferEvent,
} from "./parse";
import { BlockMeta, EventProcessor } from "./processor";
import { logger } from "./logger";
import { EventDAO } from "./dao";
import { Client } from "pg";

const dao = new EventDAO(
  new Client({
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    host: process.env.PGHOST,
    port: Number(process.env.PGPORT),
    database: process.env.PGDATABASE,
    ssl: process.env.PGCERT
      ? {
          ca: process.env.PGCERT,
        }
      : false,
  })
);

const EVENT_PROCESSORS = [
  <EventProcessor<PositionMintedEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.POSITIONS_ADDRESS),
      keys: [
        // PositionMinted
        FieldElement.fromBigInt(
          0x2a9157ea1542bfe11220258bf15d8aa02d791e7f94426446ec85b94159929fn
        ),
      ],
    },
    parser: parsePositionMintedEvent,
    handle: async (ev, meta) => {
      logger.debug("PositionMinted", { ev, meta });
      await dao.insertPositionMetadata(ev, meta);
    },
  },
  <EventProcessor<TransferEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.POSITIONS_ADDRESS),
      keys: [
        // Transfer to address 0, i.e. a burn
        FieldElement.fromBigInt(
          0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9n
        ),
      ],
    },
    parser: parseTransferEvent,
    async handle(ev: TransferEvent, meta): Promise<void> {
      if (meta.isFinal && BigInt(ev.to) === 0n) {
        logger.debug("Position Burned", { ev, meta });
        await dao.deletePositionMetadata(ev, meta);
      }
    },
  },
  <EventProcessor<PositionUpdatedEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.CORE_ADDRESS),
      keys: [
        // PositionUpdated
        FieldElement.fromBigInt(
          0x03a7adca3546c213ce791fabf3b04090c163e419c808c9830fb343a4a395946en
        ),
      ],
    },
    parser: parsePositionUpdatedEvent,
    async handle(ev: PositionUpdatedEvent, meta): Promise<void> {
      logger.debug("PositionUpdated", { ev, meta });
      if (meta.isFinal) {
        await dao.insertPositionUpdated(ev, meta);
      }
    },
  },
  <EventProcessor<SwappedEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.CORE_ADDRESS),
      keys: [
        // swap events
        FieldElement.fromBigInt(
          0x157717768aca88da4ac4279765f09f4d0151823d573537fbbeb950cdbd9a870n
        ),
      ],
    },
    parser: parseSwappedEvent,
    async handle(ev: SwappedEvent, meta): Promise<void> {
      if (meta.isFinal) {
        logger.debug("Swapped Event", { ev, meta });
        await dao.insertSwappedEvent(ev, meta);
      }
    },
  },
] as const;

const client = new StreamClient({
  url: process.env.APIBARA_URL,
  token: process.env.APIBARA_AUTH_TOKEN,
});

(async function () {
  // first set up the schema
  await dao.connectAndInit();

  // then load the cursor
  let cursor =
    (await dao.loadCursor()) ??
    StarkNetCursor.createWithBlockNumber(
      Number(process.env.STARTING_CURSOR_BLOCK_NUMBER ?? 0)
    );

  client.configure({
    filter: EVENT_PROCESSORS.reduce((memo, value) => {
      return memo.addEvent((ev) =>
        ev.withKeys(value.filter.keys).withFromAddress(value.filter.fromAddress)
      );
    }, Filter.create().withHeader({ weak: true })).encode(),
    batchSize: 1,
    finality: v1alpha2.DataFinality.DATA_STATUS_PENDING,
    cursor,
  });

  for await (const message of client) {
    let messageType = !!message.heartbeat
      ? "heartbeat"
      : !!message.invalidate
      ? "invalidate"
      : !!message.data
      ? "data"
      : "unknown";

    switch (messageType) {
      case "data":
        if (!message.data.data) {
          logger.error(`Data message is empty`);
          break;
        } else {
          for (const item of message.data.data) {
            const block = starknet.Block.decode(item);

            const meta: BlockMeta = {
              blockNumber: Number(parseLong(block.header.blockNumber)),
              blockTimestamp: new Date(
                Number(parseLong(block.header.timestamp.seconds) * 1000n)
              ),
              isFinal:
                block.status ===
                starknet.BlockStatus.BLOCK_STATUS_ACCEPTED_ON_L1,
            };

            const events = block.events;

            await dao.startTransaction();
            await Promise.all(
              EVENT_PROCESSORS.flatMap(({ parser, handle, filter }) => {
                return (
                  events
                    .filter((ev) => {
                      return (
                        FieldElement.toBigInt(ev.event.fromAddress) ===
                          FieldElement.toBigInt(filter.fromAddress) &&
                        ev.event.keys.length === filter.keys.length &&
                        ev.event.keys.every(
                          (key, ix) =>
                            FieldElement.toBigInt(key) ===
                            FieldElement.toBigInt(filter.keys[ix])
                        )
                      );
                    })
                    .map((ev) => parser(ev.event.data, 0).value)
                    // unavoidable `as any` because the parser type is a union of all the types
                    .map((ev) => handle(ev as any, meta))
                );
              })
            );
            await dao.writeCursor(message.data.cursor);
            await dao.endTransaction();

            logger.info(`Processed block`, { meta });
          }
        }
        break;
      case "heartbeat":
        logger.debug(`Heartbeat`);
        break;
      case "invalidate":
        logger.warn(`Invalidated cursor`, {
          cursor: Cursor.toObject(message.data.endCursor),
        });
        await dao.writeCursor(message.invalidate.cursor);
        break;

      case "unknown":
        logger.error(`Unknown message type`);
        break;
    }
  }
})()
  .then(() => {
    logger.info("Stream closed gracefully");
    process.exit(1);
  })
  .catch((error) => {
    logger.error("Stream crashed", { error });
    process.exit(1);
  });
