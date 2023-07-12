import { v1alpha2 as starknet } from "@apibara/starknet";
import { Parser } from "./parse";

export interface BlockMeta {
  blockNumber: bigint;
  blockTimestamp: Date;
}

export interface TxMeta {
  hash: string;
}

export interface EventProcessor<T> {
  filter: {
    keys: starknet.IFieldElement[];
    fromAddress: starknet.IFieldElement;
  };

  parser: Parser<T>;

  handle(result: { parsed: T; tx: TxMeta; block: BlockMeta }): Promise<void>;
}
