import { EventProcessor } from "./processor";
import { FieldElement } from "@apibara/starknet";
import { logger } from "./logger";
import { parseTransferEvent, TransferEvent } from "./events/nft";
import {
  FeesAccumulatedEvent,
  parseFeesAccumulatedEvent,
  parsePoolInitializedEvent,
  parsePositionFeesCollectedEvent,
  parsePositionUpdatedEvent,
  parseProtocolFeesPaidEvent,
  parseProtocolFeesWithdrawnEvent,
  parseRegistrationEvent,
  parseSwappedEvent,
  PoolInitializationEvent,
  PositionFeesCollectedEvent,
  PositionUpdatedEvent,
  ProtocolFeesPaidEvent,
  ProtocolFeesWithdrawnEvent,
  SwappedEvent,
  TokenRegistrationEvent,
} from "./events/core";
import {
  LegacyPositionMintedEvent,
  parseLegacyPositionMintedEvent,
  parsePositionMintedWithReferrerEvent,
  PositionMintedWithReferrer,
} from "./events/positions";
import {
  OrderUpdatedEvent,
  parseOrderUpdated,
  OrderProceedsWithdrawnEvent,
  parseOrderProceedsWithdrawn,
  VirtualOrdersExecutedEvent,
  parseVirtualOrdersExecuted
} from "./events/twamm";

export const EVENT_PROCESSORS = [
  <EventProcessor<LegacyPositionMintedEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.POSITIONS_ADDRESS),
      keys: [
        // PositionMinted
        FieldElement.fromBigInt(
          0x2a9157ea1542bfe11220258bf15d8aa02d791e7f94426446ec85b94159929fn
        ),
      ],
    },
    parser: parseLegacyPositionMintedEvent,
    handle: async (dao, { key, parsed }) => {
      logger.debug("PositionMinted", { parsed, key });
      if (parsed.referrer !== null && parsed.referrer !== 0n) {
        await dao.insertPositionMintedWithReferrerEvent(parsed, key);
      }
    },
  },
  <EventProcessor<PositionMintedWithReferrer>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.POSITIONS_ADDRESS),
      keys: [
        // PositionMintedWithReferrer
        FieldElement.fromBigInt(
          0x0289e57bf153052470392b578fad8d64393d2b5307e0cf1bf59f7967db3480fdn
        ),
      ],
    },
    parser: parsePositionMintedWithReferrerEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("Referral", { parsed, key });
      await dao.insertPositionMintedWithReferrerEvent(parsed, key);
    },
  },
  <EventProcessor<TransferEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.NFT_ADDRESS),
      keys: [
        // Transfer
        FieldElement.fromBigInt(
          0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9n
        ),
      ],
    },
    parser: parseTransferEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("NFT transferred", { parsed, key });
      await dao.insertPositionTransferEvent(parsed, key);
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
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("PositionUpdated", { parsed, key });
      await dao.insertPositionUpdatedEvent(parsed, key);
    },
  },
  <EventProcessor<PositionFeesCollectedEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.CORE_ADDRESS),
      keys: [
        // PositionFeesCollected
        FieldElement.fromBigInt(
          0x96982abd597114bdaa4a60612f87fabfcc7206aa12d61c50e7ba1e6c291100n
        ),
      ],
    },
    parser: parsePositionFeesCollectedEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("PositionFeesCollected", { parsed, key });
      await dao.insertPositionFeesCollectedEvent(parsed, key);
    },
  },
  <EventProcessor<SwappedEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.CORE_ADDRESS),
      keys: [
        // Swapped
        FieldElement.fromBigInt(
          0x157717768aca88da4ac4279765f09f4d0151823d573537fbbeb950cdbd9a870n
        ),
      ],
    },
    parser: parseSwappedEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("Swapped", { parsed, key });
      await dao.insertSwappedEvent(parsed, key);
    },
  },
  <EventProcessor<PoolInitializationEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.CORE_ADDRESS),
      keys: [
        // PoolInitialized
        FieldElement.fromBigInt(
          0x025ccf80ee62b2ca9b97c76ccea317c7f450fd6efb6ed6ea56da21d7bb9da5f1n
        ),
      ],
    },
    parser: parsePoolInitializedEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("PoolInitialized", { parsed, key });
      await dao.insertInitializationEvent(parsed, key);
    },
  },
  <EventProcessor<ProtocolFeesWithdrawnEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.CORE_ADDRESS),
      keys: [
        // ProtocolFeesWithdrawn
        FieldElement.fromBigInt(
          0x291697c8230383d5c3cc8dc39443356a7da6b0735605fb0ee0f7bfbb7b824an
        ),
      ],
    },
    parser: parseProtocolFeesWithdrawnEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("ProtocolFeesWithdrawn", { parsed, key });
      await dao.insertProtocolFeesWithdrawn(parsed, key);
    },
  },
  <EventProcessor<ProtocolFeesPaidEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.CORE_ADDRESS),
      keys: [
        // ProtocolFeesPaid
        FieldElement.fromBigInt(
          0x5dacf59794364ad1555bb3c9b2346afa81e57e5c19bb6bae0d22721c96c4e5n
        ),
      ],
    },
    parser: parseProtocolFeesPaidEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("ProtocolFeesPaid", { parsed, key });
      await dao.insertProtocolFeesPaid(parsed, key);
    },
  },
  <EventProcessor<FeesAccumulatedEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.CORE_ADDRESS),
      keys: [
        // FeesAccumulated
        FieldElement.fromBigInt(
          0x0237e5e0677822acfc9117ed0f7ba4810b2c6b539a2359e8d73f9025d56957aan
        ),
      ],
    },
    parser: parseFeesAccumulatedEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("FeesAccumulated", { parsed, key });
      await dao.insertFeesAccumulatedEvent(parsed, key);
    },
  },
  <EventProcessor<TokenRegistrationEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(process.env.TOKEN_REGISTRY_ADDRESS),
      keys: [
        // Registration
        FieldElement.fromBigInt(
          0x3ea44da5af08f985c5ac763fa2573381d77aeee47d9a845f0c6764cb805d74n
        ),
      ],
    },
    parser: parseRegistrationEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("Registration", { parsed, key });
      await dao.insertRegistration(parsed, key);
    },
  },
  <EventProcessor<TokenRegistrationEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(
        process.env.TOKEN_REGISTRY_V2_ADDRESS
      ),
      keys: [
        // Registration
        FieldElement.fromBigInt(
          0x3ea44da5af08f985c5ac763fa2573381d77aeee47d9a845f0c6764cb805d74n
        ),
      ],
    },
    parser: parseRegistrationEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("Registration V2", { parsed, key });
      await dao.insertRegistration(parsed, key);
    },
  },
  <EventProcessor<OrderUpdatedEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(
        process.env.TWAMM_ADDRESS
      ),
      keys: [
        // OrderUpdated
        FieldElement.fromBigInt(
          0xb670ed7b7ee8ccb350963a7dea39493daff6e7a43ab021a0e4ac2d652d359en
        ),
      ],
    },
    parser: parseOrderUpdated,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("OrderUpdated", { parsed, key });
      await dao.insertTWAMMOrderUpdatedEvent(parsed, key);
    },
  },
  <EventProcessor<OrderProceedsWithdrawnEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(
        process.env.TWAMM_ADDRESS
      ),
      keys: [
        // OrderProceedsWithdrawn
        FieldElement.fromBigInt(
          0x3e074150c5906b2e323cea942b41f67f3639fcae5dc1fe4cf19c6801dff89b5n
        ),
      ],
    },
    parser: parseOrderProceedsWithdrawn,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("OrderProceedsWithdrawn", { parsed, key });
      await dao.insertTWAMMOrderProceedsWithdrawnEvent(parsed, key);
    },
  },
  <EventProcessor<VirtualOrdersExecutedEvent>>{
    filter: {
      fromAddress: FieldElement.fromBigInt(
        process.env.TWAMM_ADDRESS
      ),
      keys: [
        // VirtualOrdersExecuted
        FieldElement.fromBigInt(
          0x29416aa69fb4a5270dd3c2b3e6d05f457dc0dbf96f423db1f86c5b7b2e6840fn
        ),
      ],
    },
    parser: parseVirtualOrdersExecuted,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("VirtualOrdersExecuted", { parsed, key });
      await dao.insertTWAMMVirtualOrdersExecutedEvent(parsed, key, process.env.TWAMM_ADDRESS);
    },
  },
] as const;
