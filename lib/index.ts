import "./config";

import { debounce } from "debounce";
import {
  FieldElement,
  Filter,
  StarkNetCursor,
  v1alpha2 as starknet,
} from "@apibara/starknet";
import { existsSync, readFileSync, writeFileSync } from "fs";
import { Cursor, StreamClient, v1alpha2 } from "@apibara/protocol";
import { toNftAttributes } from "./minted";
import {
  parseLong,
  parsePositionMintedEvent,
  parsePositionUpdatedEvent,
  parseTransferEvent,
  PositionUpdatedEvent,
  TransferEvent,
} from "./parse";
import { BlockMeta, EventProcessor } from "./processor";
import { PostgresClient } from "./postgres";
import { createLogger, format, transports } from "winston";

const logger = createLogger({
  level: "debug",
  format: format.combine(
    format.timestamp({
      format: "YYYY-MM-DD HH:mm:ss",
    }),
    format.errors({ stack: true }),
    format.splat(),
    format.json(),
  ),
  defaultMeta: { service: "ekubo-indexer" },
  transports: [new transports.Console()],
});

const pg = new PostgresClient();

let cursor: v1alpha2.ICursor;
const CURSOR_PATH = process.env.CURSOR_FILE;
if (existsSync(CURSOR_PATH)) {
  try {
    cursor = Cursor.fromObject(JSON.parse(readFileSync(CURSOR_PATH, "utf8")));
  } catch (error) {
    logger.error(`Failed to parse cursor file`, error);
    throw error;
  }
} else {
  const blockNumber = process.env.STARTING_CURSOR_BLOCK_NUMBER ?? 0;
  cursor = StarkNetCursor.createWithBlockNumber(Number(blockNumber));
  logger.info(`Cursor file not found, starting with block number`, {
    blockNumber,
  });
}

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
      logger.info("Minted", { ev, meta })
      await pg.insertToken(ev, meta);
    },
  },
  {
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.POSITIONS_ADDRESS),
      keys: [
        // Transfer to address 0, i.e. a burn
        FieldElement.fromBigInt(
          0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
        ),
      ],
    },
    parser: (ev) => parseTransferEvent(ev.event.data, 0).value,
    async handle(ev: TransferEvent, meta): Promise<void> {
      if (meta.isFinal && BigInt(ev.to) === 0n) {
        logger.info("Burned", { ev, meta })
        await pg.deleteToken(ev, meta);
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
      // todo: handle these events
      logger.info("Position updated", { ev, meta })
      await pg.insertPosition(ev, meta);
    },
  },
];

const client = new StreamClient({
  url: process.env.APIBARA_URL,
  token: process.env.APIBARA_AUTH_TOKEN,
});

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

const writeCursorIfNecessary = debounce(
  (value: v1alpha2.ICursor) => {
    const next = JSON.stringify(Cursor.toObject(value));
    if (next === JSON.stringify(Cursor.toObject(cursor))) {
      return;
    }
    cursor = value;
    writeFileSync(CURSOR_PATH, next);

    logger.info("Wrote cursor", {
      cursor: Cursor.toObject(value),
    });
  },
  100,
  true
);

(async function () {
  await pg.connect();
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
          }

          writeCursorIfNecessary(message.data.cursor);
        }
        break;
      case "heartbeat":
        logger.debug(`Heartbeat`);
        break;
      case "invalidate":
        logger.warn(`Invalidated cursor`, {
          cursor: Cursor.toObject(message.data.endCursor),
        });
        writeCursorIfNecessary(message.invalidate.cursor);
        break;

      case "unknown":
        logger.error(`Unknown message type`);
        break;
    }
  }
})()
  .then(() => logger.info("Stream closed"))
  .catch((error) => logger.error("Stream crashed", { error }));
