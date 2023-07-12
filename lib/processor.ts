import { v1alpha2 as starknet } from "@apibara/starknet";
import { Parser } from "./parse";

export interface EventKey {
  blockNumber: bigint;
  txHash: bigint;
  logIndex: bigint;
}

export interface EventProcessor<T> {
  filter: {
    keys: starknet.IFieldElement[];
    fromAddress: starknet.IFieldElement;
  };

  parser: Parser<T>;

  handle(result: { parsed: T; key: EventKey }): Promise<void>;
}
