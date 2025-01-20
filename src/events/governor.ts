import {
  backwardsCompatibleParserAdditionalArgument,
  combineParsers,
  parseAddress,
  parseBoolean,
  parseByteArray,
  parseFelt252,
  parseSpanOf,
  parseU128,
  parseU64,
} from "../parse";
import type { GetParserType } from "../parse";

export const parseCall = combineParsers({
  to: { index: 0, parser: parseAddress },
  selector: { index: 1, parser: parseFelt252 },
  calldata: { index: 2, parser: parseSpanOf(parseFelt252) },
});
export type CallType = GetParserType<typeof parseCall>;

export const parseGovernorProposedEvent = combineParsers({
  id: { index: 0, parser: parseFelt252 },
  proposer: { index: 1, parser: parseAddress },
  calls: { index: 2, parser: parseSpanOf(parseCall) },
  config_version: {
    index: 3,
    parser: backwardsCompatibleParserAdditionalArgument(parseU64),
  },
});
export type GovernorProposedEvent = GetParserType<
  typeof parseGovernorProposedEvent
>;

export const parseGovernorVotedEvent = combineParsers({
  id: { index: 0, parser: parseFelt252 },
  voter: { index: 1, parser: parseAddress },
  weight: { index: 2, parser: parseU128 },
  yea: { index: 3, parser: parseBoolean },
});
export type GovernorVotedEvent = GetParserType<typeof parseGovernorVotedEvent>;

export const parseGovernorCanceledEvent = combineParsers({
  id: { index: 0, parser: parseFelt252 },
});
export type GovernorCanceledEvent = GetParserType<
  typeof parseGovernorCanceledEvent
>;

export const parseGovernorCreationThresholdBreached = combineParsers({
  id: { index: 0, parser: parseFelt252 },
  breach_timestamp: { index: 0, parser: parseU64 },
});
export type GovernorCreationThresholdBreached = GetParserType<
  typeof parseGovernorCreationThresholdBreached
>;

export const parseGovernorExecutedEvent = combineParsers({
  id: { index: 0, parser: parseFelt252 },
  result_data: { index: 1, parser: parseSpanOf(parseSpanOf(parseFelt252)) },
});
export type GovernorExecutedEvent = GetParserType<
  typeof parseGovernorExecutedEvent
>;

export const parseDescribedEvent = combineParsers({
  id: { index: 0, parser: parseFelt252 },
  description: { index: 1, parser: parseByteArray },
});
export type DescribedEvent = GetParserType<typeof parseDescribedEvent>;

export const parseGovernorConfig = combineParsers({
  voting_start_delay: { index: 0, parser: parseU64 },
  voting_period: { index: 1, parser: parseU64 },
  voting_weight_smoothing_duration: { index: 2, parser: parseU64 },
  quorum: { index: 3, parser: parseU128 },
  proposal_creation_threshold: { index: 4, parser: parseU128 },
  execution_delay: { index: 5, parser: parseU64 },
  execution_window: { index: 6, parser: parseU64 },
});
export type GovernorConfig = GetParserType<typeof parseGovernorConfig>;

export const parseGovernorReconfigured = combineParsers({
  new_config: { index: 0, parser: parseGovernorConfig },
  version: { index: 1, parser: parseU64 },
});
export type GovernorReconfiguredEvent = GetParserType<
  typeof parseGovernorReconfigured
>;
