import { v1alpha2 as starknet } from "@apibara/starknet";

export interface EventProcessor<T> {
  filter: {
    keys: starknet.IFieldElement[];
    fromAddress: starknet.IFieldElement;
  };

  parser(ev: starknet.IEventWithTransaction): T;

  handle(
    ev: T,
    meta: { blockNumber: number; blockTimestamp: Date }
  ): Promise<void>;
}
