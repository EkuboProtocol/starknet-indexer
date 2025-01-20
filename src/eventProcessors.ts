import type { EventProcessor } from "./processor";
import { logger } from "./logger";
import { parseTransferEvent } from "./events/nft";
import type { TransferEvent } from "./events/nft";
import {
  parseFeesAccumulatedEvent,
  parsePoolInitializedEvent,
  parsePositionFeesCollectedEvent,
  parsePositionUpdatedEvent,
  parseProtocolFeesPaidEvent,
  parseProtocolFeesWithdrawnEvent,
  parseSwappedEvent,
} from "./events/core";
import type {
  FeesAccumulatedEvent,
  PoolInitializationEvent,
  PositionFeesCollectedEvent,
  PositionUpdatedEvent,
  ProtocolFeesPaidEvent,
  ProtocolFeesWithdrawnEvent,
  SwappedEvent,
} from "./events/core";
import {
  parseLegacyPositionMintedEvent,
  parsePositionMintedWithReferrerEvent,
} from "./events/positions";
import type {
  LegacyPositionMintedEvent,
  PositionMintedWithReferrer,
} from "./events/positions";
import {
  parseOrderProceedsWithdrawn,
  parseOrderUpdated,
  parseVirtualOrdersExecuted,
} from "./events/twamm";
import type {
  OrderProceedsWithdrawnEvent,
  OrderUpdatedEvent,
  VirtualOrdersExecutedEvent,
} from "./events/twamm";
import { parseStakedEvent, parseWithdrawnEvent } from "./events/staker";
import type { StakedEvent, WithdrawnEvent } from "./events/staker";
import {
  parseDescribedEvent,
  parseGovernorCanceledEvent,
  parseGovernorCreationThresholdBreached,
  parseGovernorExecutedEvent,
  parseGovernorProposedEvent,
  parseGovernorReconfigured,
  parseGovernorVotedEvent,
} from "./events/governor";
import type {
  DescribedEvent,
  GovernorCanceledEvent,
  GovernorCreationThresholdBreached,
  GovernorExecutedEvent,
  GovernorProposedEvent,
  GovernorReconfiguredEvent,
  GovernorVotedEvent,
} from "./events/governor";
import {
  parseRegistrationEvent,
  parseRegistrationEventV3,
} from "./events/tokenRegistry";
import type {
  TokenRegistrationEvent,
  TokenRegistrationEventV3,
} from "./events/tokenRegistry";
import { parseSnapshotEvent } from "./events/oracle";
import type { SnapshotEvent } from "./events/oracle";
import { parseOrderClosed, parseOrderPlaced } from "./events/limitOrders";
import type { OrderClosedEvent, OrderPlacedEvent } from "./events/limitOrders";

export const EVENT_PROCESSORS = [
  <EventProcessor<LegacyPositionMintedEvent>>{
    filter: {
      fromAddress: process.env.POSITIONS_ADDRESS,
      keys: [
        // PositionMinted
        "0x2a9157ea1542bfe11220258bf15d8aa02d791e7f94426446ec85b94159929f",
      ],
    },
    parser: parseLegacyPositionMintedEvent,
    handle: async (dao, { key, parsed }) => {
      logger.debug("PositionMinted", { parsed, key });
      if (parsed.referrer !== null && parsed.referrer !== 0n) {
        await dao.insertPositionMintedWithReferrerEvent(
          { id: parsed.id, referrer: parsed.referrer ?? 0n },
          key,
        );
      }
    },
  },
  <EventProcessor<PositionMintedWithReferrer>>{
    filter: {
      fromAddress: process.env.POSITIONS_ADDRESS,
      keys: [
        // PositionMintedWithReferrer
        "0x0289e57bf153052470392b578fad8d64393d2b5307e0cf1bf59f7967db3480fd",
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
      fromAddress: process.env.NFT_ADDRESS,
      keys: [
        // Transfer
        "0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9",
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
      fromAddress: process.env.CORE_ADDRESS,
      keys: [
        // PositionUpdated
        "0x03a7adca3546c213ce791fabf3b04090c163e419c808c9830fb343a4a395946e",
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
      fromAddress: process.env.CORE_ADDRESS,
      keys: [
        // PositionFeesCollected
        "0x96982abd597114bdaa4a60612f87fabfcc7206aa12d61c50e7ba1e6c291100",
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
      fromAddress: process.env.CORE_ADDRESS,
      keys: [
        // Swapped
        "0x157717768aca88da4ac4279765f09f4d0151823d573537fbbeb950cdbd9a870",
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
      fromAddress: process.env.CORE_ADDRESS,
      keys: [
        // PoolInitialized
        "0x025ccf80ee62b2ca9b97c76ccea317c7f450fd6efb6ed6ea56da21d7bb9da5f1",
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
      fromAddress: process.env.CORE_ADDRESS,
      keys: [
        // ProtocolFeesWithdrawn
        "0x291697c8230383d5c3cc8dc39443356a7da6b0735605fb0ee0f7bfbb7b824a",
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
      fromAddress: process.env.CORE_ADDRESS,
      keys: [
        // ProtocolFeesPaid
        "0x5dacf59794364ad1555bb3c9b2346afa81e57e5c19bb6bae0d22721c96c4e5",
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
      fromAddress: process.env.CORE_ADDRESS,
      keys: [
        // FeesAccumulated
        "0x0237e5e0677822acfc9117ed0f7ba4810b2c6b539a2359e8d73f9025d56957aa",
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
      fromAddress: process.env.TOKEN_REGISTRY_ADDRESS,
      keys: [
        // Registration
        "0x3ea44da5af08f985c5ac763fa2573381d77aeee47d9a845f0c6764cb805d74",
      ],
    },
    parser: parseRegistrationEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("Registration from V1 Registry", { parsed, key });
      await dao.insertRegistration(parsed, key);
    },
  },
  <EventProcessor<TokenRegistrationEvent>>{
    filter: {
      fromAddress: process.env.TOKEN_REGISTRY_V2_ADDRESS,
      keys: [
        // Registration
        "0x3ea44da5af08f985c5ac763fa2573381d77aeee47d9a845f0c6764cb805d74",
      ],
    },
    parser: parseRegistrationEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("Registration from V2 Registry", {
        parsed,
        key,
      });
      await dao.insertRegistration(parsed, key);
    },
  },
  <EventProcessor<TokenRegistrationEventV3>>{
    filter: {
      fromAddress: process.env.TOKEN_REGISTRY_V3_ADDRESS,
      keys: [
        // Registration
        "0x3ea44da5af08f985c5ac763fa2573381d77aeee47d9a845f0c6764cb805d74",
      ],
    },
    parser: parseRegistrationEventV3,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("Registration event from V3 Registry", { parsed, key });
      await dao.insertRegistrationV3(parsed, key);
    },
  },
  <EventProcessor<OrderUpdatedEvent>>{
    filter: {
      fromAddress: process.env.TWAMM_ADDRESS,
      keys: [
        // OrderUpdated
        "0xb670ed7b7ee8ccb350963a7dea39493daff6e7a43ab021a0e4ac2d652d359e",
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
      fromAddress: process.env.TWAMM_ADDRESS,
      keys: [
        // OrderProceedsWithdrawn
        "0x3e074150c5906b2e323cea942b41f67f3639fcae5dc1fe4cf19c6801dff89b5",
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
      fromAddress: process.env.TWAMM_ADDRESS,
      keys: [
        // VirtualOrdersExecuted
        "0x29416aa69fb4a5270dd3c2b3e6d05f457dc0dbf96f423db1f86c5b7b2e6840f",
      ],
    },
    parser: parseVirtualOrdersExecuted,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("VirtualOrdersExecuted", { parsed, key });
      await dao.insertTWAMMVirtualOrdersExecutedEvent(parsed, key);
    },
  },
  <EventProcessor<StakedEvent>>{
    filter: {
      fromAddress: process.env.STAKER_ADDRESS,
      keys: [
        // Staked
        "0x024fdaadc324c3bb8e59febfb2e8a399331e58193489e54ac40fec46745a9ebe",
      ],
    },
    parser: parseStakedEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("StakerStakedEvent", { parsed, key });
      await dao.insertStakerStakedEvent(parsed, key);
    },
  },
  <EventProcessor<WithdrawnEvent>>{
    filter: {
      fromAddress: process.env.STAKER_ADDRESS,
      keys: [
        // Withdrawn
        "0x036a4d15ab9e146faab90d4abc1c0cad17c4ded24551c781ba100392b5a70248",
      ],
    },
    parser: parseWithdrawnEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("StakerWithdrawnEvent", { parsed, key });
      await dao.insertStakerWithdrawnEvent(parsed, key);
    },
  },
  <EventProcessor<GovernorProposedEvent>>{
    filter: {
      fromAddress: process.env.GOVERNOR_ADDRESS,
      keys: [
        // Proposed
        "0x02a98c37f5b13fe14803e72b284c81be9ebbedc6cf74ed8d1489ed74951cba3f",
      ],
    },
    parser: parseGovernorProposedEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("GovernorProposed", { parsed, key });
      await dao.insertGovernorProposedEvent(parsed, key);
    },
  },
  <EventProcessor<GovernorCanceledEvent>>{
    filter: {
      fromAddress: process.env.GOVERNOR_ADDRESS,
      keys: [
        // Canceled
        "0xad1f80a0e6ac2d42f6ce99670de84817aef2368cd22a19f85fcb721f689192",
      ],
    },
    parser: parseGovernorCanceledEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("GovernorCanceled", { parsed, key });
      await dao.insertGovernorCanceledEvent(parsed, key);
    },
  },
  <EventProcessor<GovernorCreationThresholdBreached>>{
    filter: {
      fromAddress: process.env.GOVERNOR_ADDRESS,
      keys: [
        // CreationThresholdBreached
        "0xda0eb1cb71bdbfac21648d8b87024714f7eb6207978c7eb359a20144a99baf",
      ],
    },
    parser: parseGovernorCreationThresholdBreached,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("GovernorCreationThresholdBreached", { parsed, key });
      // just use the canceled table
      await dao.insertGovernorCanceledEvent(parsed, key);
    },
  },
  <EventProcessor<GovernorVotedEvent>>{
    filter: {
      fromAddress: process.env.GOVERNOR_ADDRESS,
      keys: [
        // Voted
        "0x5c9afac1c510b50d3e0004024ba7b8e190864f1543dd8025d08f88410fb162",
      ],
    },
    parser: parseGovernorVotedEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("GovernorVoted", { parsed, key });
      await dao.insertGovernorVotedEvent(parsed, key);
    },
  },
  <EventProcessor<GovernorExecutedEvent>>{
    filter: {
      fromAddress: process.env.GOVERNOR_ADDRESS,
      keys: [
        // Executed
        "0x01f4317aae43f6c24b2b85c6d8b21d5fa0a28cee0476cd52ca5d60d4787aab78",
      ],
    },
    parser: parseGovernorExecutedEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("GovernorExecuted", { parsed, key });
      await dao.insertGovernorExecutedEvent(parsed, key);
    },
  },
  <EventProcessor<DescribedEvent>>{
    filter: {
      fromAddress: process.env.GOVERNOR_ADDRESS,
      keys: [
        // Described
        "0x8643a1c8a461189d5b77de7576b06aa9148c9127101228f02816d13768e7a9",
      ],
    },
    parser: parseDescribedEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("GovernorProposalDescribed", { parsed, key });
      await dao.insertGovernorProposalDescribedEvent(parsed, key);
    },
  },
  <EventProcessor<GovernorReconfiguredEvent>>{
    filter: {
      fromAddress: process.env.GOVERNOR_ADDRESS,
      keys: [
        // Reconfigured
        "0x02b9973fd701ab68169e139e241db74576eca4e885bad73d016982a59f1ac9fb",
      ],
    },
    parser: parseGovernorReconfigured,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("GovernorReconfigured", { parsed, key });
      await dao.insertGovernorReconfiguredEvent(parsed, key);
    },
  },
  <EventProcessor<SnapshotEvent>>{
    filter: {
      fromAddress: process.env.ORACLE_ADDRESS,
      keys: [
        // SnapshotEvent
        "0x0385e1b60fdfb8aeee9212a69cdb72415cef7b24ec07a60cdd65b65d0582238b",
      ],
    },
    parser: parseSnapshotEvent,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("Snapshot", { parsed, key });
      await dao.insertOracleSnapshotEvent(parsed, key);
    },
  },
  <EventProcessor<OrderPlacedEvent>>{
    filter: {
      fromAddress: process.env.LIMIT_ORDERS_ADDRESS,
      keys: [
        // OrderPlaced
        "0x03b935dbbdb7f463a394fc8729e7e26e30edebbc3bd5617bf1d7cf9e1ce6f7cb",
      ],
    },
    parser: parseOrderPlaced,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("OrderPlaced", { parsed, key });
      await dao.insertOrderPlacedEvent(parsed, key);
    },
  },
  <EventProcessor<OrderClosedEvent>>{
    filter: {
      fromAddress: process.env.LIMIT_ORDERS_ADDRESS,
      keys: [
        // OrderClosed
        "0x0196e77c6eab92283e3fc303198bb0a523c0c7d93b4de1d8bf636eab7517c4ae",
      ],
    },
    parser: parseOrderClosed,
    async handle(dao, { parsed, key }): Promise<void> {
      logger.debug("OrderClosed", { parsed, key });
      await dao.insertOrderClosedEvent(parsed, key);
    },
  },
] as const;
