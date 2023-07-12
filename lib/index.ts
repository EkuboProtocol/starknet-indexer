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
  parseTransferEvent,
  PositionUpdatedEvent,
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
  })
);

const EVENT_PROCESSORS: EventProcessor<any>[] = [
  {
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.POSITIONS_ADDRESS),
      keys: [
        // PositionMinted
        FieldElement.fromBigInt(
          0x2a9157ea1542bfe11220258bf15d8aa02d791e7f94426446ec85b94159929fn
        ),
      ],
    },
    parser: (ev) => parsePositionMintedEvent(ev.event.data, 0).value,
    handle: async (ev, meta) => {
      logger.debug("PositionMinted", { ev, meta });
      await dao.insertPositionMetadata(ev, meta);
    },
  },
  {
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.POSITIONS_ADDRESS),
      keys: [
        // Transfer to address 0, i.e. a burn
        FieldElement.fromBigInt(
          0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9n
        ),
      ],
    },
    parser: (ev) => parseTransferEvent(ev.event.data, 0).value,
    async handle(ev: TransferEvent, meta): Promise<void> {
      if (meta.isFinal && BigInt(ev.to) === 0n) {
        logger.debug("Position Burned", { ev, meta });
        await dao.deletePositionMetadata(ev, meta);
      }
    },
  },
  {
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.CORE_ADDRESS),
      keys: [
        // PositionUpdated
        FieldElement.fromBigInt(
          0x03a7adca3546c213ce791fabf3b04090c163e419c808c9830fb343a4a395946en
        ),
      ],
    },
    parser: (ev) => parsePositionUpdatedEvent(ev.event.data, 0).value,
    async handle(ev: PositionUpdatedEvent, meta): Promise<void> {
      logger.debug("PositionUpdated", { ev, meta });
      await dao.insertPositionUpdated(ev, meta);
    },
  },
];

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
              EVENT_PROCESSORS.flatMap((processor) => {
                return events
                  .filter((ev) => {
                    return (
                      FieldElement.toBigInt(ev.event.fromAddress) ===
                        FieldElement.toBigInt(processor.filter.fromAddress) &&
                      ev.event.keys.length === processor.filter.keys.length &&
                      ev.event.keys.every(
                        (key, ix) =>
                          FieldElement.toBigInt(key) ===
                          FieldElement.toBigInt(processor.filter.keys[ix])
                      )
                    );
                  })
                  .map(processor.parser)
                  .map((ev) => processor.handle(ev, meta));
              })
            );
            await dao.writeCursor(message.data.cursor);
            await dao.endTransaction();
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
