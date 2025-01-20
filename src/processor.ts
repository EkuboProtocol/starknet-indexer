import type { Parser } from "./parse";
import { DAO } from "./dao";

export interface EventKey {
  blockNumber: number;
  transactionIndex: number;
  eventIndex: number;
  emitter: bigint;
  transactionHash: bigint;
}

export interface ParsedEventWithKey<T> {
  key: EventKey;
  parsed: T;
}

export interface EventProcessor<T> {
  filter: {
    keys: `0x${string}`[];
    fromAddress: `0x${string}`;
  };

  parser: Parser<T>;

  handle(dao: DAO, result: ParsedEventWithKey<T>): Promise<void>;
}
