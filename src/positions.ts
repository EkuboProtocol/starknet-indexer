import { Contract, RpcProvider } from "starknet";

const provider = new RpcProvider({ nodeUrl: process.env.RPC_URL });

export const positionsContract = new Contract(
  [
    {
      type: "impl",
      name: "ILockerImpl",
      interface_name: "ekubo::interfaces::core::ILocker",
    },
    {
      type: "interface",
      name: "ekubo::interfaces::core::ILocker",
      items: [
        {
          type: "function",
          name: "locked",
          inputs: [
            {
              name: "id",
              type: "core::integer::u32",
            },
            {
              name: "data",
              type: "core::array::Array::<core::felt252>",
            },
          ],
          outputs: [
            {
              type: "core::array::Array::<core::felt252>",
            },
          ],
          state_mutability: "external",
        },
      ],
    },
    {
      type: "impl",
      name: "PositionsImpl",
      interface_name: "ekubo::interfaces::positions::IPositions",
    },
    {
      type: "struct",
      name: "ekubo::types::keys::PoolKey",
      members: [
        {
          name: "token0",
          type: "core::starknet::contract_address::ContractAddress",
        },
        {
          name: "token1",
          type: "core::starknet::contract_address::ContractAddress",
        },
        {
          name: "fee",
          type: "core::integer::u128",
        },
        {
          name: "tick_spacing",
          type: "core::integer::u128",
        },
        {
          name: "extension",
          type: "core::starknet::contract_address::ContractAddress",
        },
      ],
    },
    {
      type: "enum",
      name: "core::bool",
      variants: [
        {
          name: "False",
          type: "()",
        },
        {
          name: "True",
          type: "()",
        },
      ],
    },
    {
      type: "struct",
      name: "ekubo::types::i129::i129",
      members: [
        {
          name: "mag",
          type: "core::integer::u128",
        },
        {
          name: "sign",
          type: "core::bool",
        },
      ],
    },
    {
      type: "struct",
      name: "ekubo::types::bounds::Bounds",
      members: [
        {
          name: "lower",
          type: "ekubo::types::i129::i129",
        },
        {
          name: "upper",
          type: "ekubo::types::i129::i129",
        },
      ],
    },
    {
      type: "struct",
      name: "ekubo::interfaces::positions::GetTokenInfoRequest",
      members: [
        {
          name: "id",
          type: "core::integer::u64",
        },
        {
          name: "pool_key",
          type: "ekubo::types::keys::PoolKey",
        },
        {
          name: "bounds",
          type: "ekubo::types::bounds::Bounds",
        },
      ],
    },
    {
      type: "struct",
      name: "core::integer::u256",
      members: [
        {
          name: "low",
          type: "core::integer::u128",
        },
        {
          name: "high",
          type: "core::integer::u128",
        },
      ],
    },
    {
      type: "struct",
      name: "ekubo::types::call_points::CallPoints",
      members: [
        {
          name: "after_initialize_pool",
          type: "core::bool",
        },
        {
          name: "before_swap",
          type: "core::bool",
        },
        {
          name: "after_swap",
          type: "core::bool",
        },
        {
          name: "before_update_position",
          type: "core::bool",
        },
        {
          name: "after_update_position",
          type: "core::bool",
        },
      ],
    },
    {
      type: "struct",
      name: "ekubo::types::pool_price::PoolPrice",
      members: [
        {
          name: "sqrt_ratio",
          type: "core::integer::u256",
        },
        {
          name: "tick",
          type: "ekubo::types::i129::i129",
        },
        {
          name: "call_points",
          type: "ekubo::types::call_points::CallPoints",
        },
      ],
    },
    {
      type: "struct",
      name: "ekubo::interfaces::positions::GetTokenInfoResult",
      members: [
        {
          name: "pool_price",
          type: "ekubo::types::pool_price::PoolPrice",
        },
        {
          name: "liquidity",
          type: "core::integer::u128",
        },
        {
          name: "amount0",
          type: "core::integer::u128",
        },
        {
          name: "amount1",
          type: "core::integer::u128",
        },
        {
          name: "fees0",
          type: "core::integer::u128",
        },
        {
          name: "fees1",
          type: "core::integer::u128",
        },
      ],
    },
    {
      type: "interface",
      name: "ekubo::interfaces::positions::IPositions",
      items: [
        {
          type: "function",
          name: "get_nft_address",
          inputs: [],
          outputs: [
            {
              type: "core::starknet::contract_address::ContractAddress",
            },
          ],
          state_mutability: "view",
        },
        {
          type: "function",
          name: "get_tokens_info",
          inputs: [
            {
              name: "params",
              type: "core::array::Array::<ekubo::interfaces::positions::GetTokenInfoRequest>",
            },
          ],
          outputs: [
            {
              type: "core::array::Array::<ekubo::interfaces::positions::GetTokenInfoResult>",
            },
          ],
          state_mutability: "view",
        },
        {
          type: "function",
          name: "get_token_info",
          inputs: [
            {
              name: "id",
              type: "core::integer::u64",
            },
            {
              name: "pool_key",
              type: "ekubo::types::keys::PoolKey",
            },
            {
              name: "bounds",
              type: "ekubo::types::bounds::Bounds",
            },
          ],
          outputs: [
            {
              type: "ekubo::interfaces::positions::GetTokenInfoResult",
            },
          ],
          state_mutability: "view",
        },
        {
          type: "function",
          name: "mint",
          inputs: [
            {
              name: "pool_key",
              type: "ekubo::types::keys::PoolKey",
            },
            {
              name: "bounds",
              type: "ekubo::types::bounds::Bounds",
            },
          ],
          outputs: [
            {
              type: "core::integer::u64",
            },
          ],
          state_mutability: "external",
        },
        {
          type: "function",
          name: "mint_with_referrer",
          inputs: [
            {
              name: "pool_key",
              type: "ekubo::types::keys::PoolKey",
            },
            {
              name: "bounds",
              type: "ekubo::types::bounds::Bounds",
            },
            {
              name: "referrer",
              type: "core::starknet::contract_address::ContractAddress",
            },
          ],
          outputs: [
            {
              type: "core::integer::u64",
            },
          ],
          state_mutability: "external",
        },
        {
          type: "function",
          name: "unsafe_burn",
          inputs: [
            {
              name: "id",
              type: "core::integer::u64",
            },
          ],
          outputs: [],
          state_mutability: "external",
        },
        {
          type: "function",
          name: "deposit_last",
          inputs: [
            {
              name: "pool_key",
              type: "ekubo::types::keys::PoolKey",
            },
            {
              name: "bounds",
              type: "ekubo::types::bounds::Bounds",
            },
            {
              name: "min_liquidity",
              type: "core::integer::u128",
            },
          ],
          outputs: [
            {
              type: "core::integer::u128",
            },
          ],
          state_mutability: "external",
        },
        {
          type: "function",
          name: "deposit",
          inputs: [
            {
              name: "id",
              type: "core::integer::u64",
            },
            {
              name: "pool_key",
              type: "ekubo::types::keys::PoolKey",
            },
            {
              name: "bounds",
              type: "ekubo::types::bounds::Bounds",
            },
            {
              name: "min_liquidity",
              type: "core::integer::u128",
            },
          ],
          outputs: [
            {
              type: "core::integer::u128",
            },
          ],
          state_mutability: "external",
        },
        {
          type: "function",
          name: "mint_and_deposit",
          inputs: [
            {
              name: "pool_key",
              type: "ekubo::types::keys::PoolKey",
            },
            {
              name: "bounds",
              type: "ekubo::types::bounds::Bounds",
            },
            {
              name: "min_liquidity",
              type: "core::integer::u128",
            },
          ],
          outputs: [
            {
              type: "(core::integer::u64, core::integer::u128)",
            },
          ],
          state_mutability: "external",
        },
        {
          type: "function",
          name: "mint_and_deposit_with_referrer",
          inputs: [
            {
              name: "pool_key",
              type: "ekubo::types::keys::PoolKey",
            },
            {
              name: "bounds",
              type: "ekubo::types::bounds::Bounds",
            },
            {
              name: "min_liquidity",
              type: "core::integer::u128",
            },
            {
              name: "referrer",
              type: "core::starknet::contract_address::ContractAddress",
            },
          ],
          outputs: [
            {
              type: "(core::integer::u64, core::integer::u128)",
            },
          ],
          state_mutability: "external",
        },
        {
          type: "function",
          name: "mint_and_deposit_and_clear_both",
          inputs: [
            {
              name: "pool_key",
              type: "ekubo::types::keys::PoolKey",
            },
            {
              name: "bounds",
              type: "ekubo::types::bounds::Bounds",
            },
            {
              name: "min_liquidity",
              type: "core::integer::u128",
            },
          ],
          outputs: [
            {
              type: "(core::integer::u64, core::integer::u128, core::integer::u256, core::integer::u256)",
            },
          ],
          state_mutability: "external",
        },
        {
          type: "function",
          name: "withdraw",
          inputs: [
            {
              name: "id",
              type: "core::integer::u64",
            },
            {
              name: "pool_key",
              type: "ekubo::types::keys::PoolKey",
            },
            {
              name: "bounds",
              type: "ekubo::types::bounds::Bounds",
            },
            {
              name: "liquidity",
              type: "core::integer::u128",
            },
            {
              name: "min_token0",
              type: "core::integer::u128",
            },
            {
              name: "min_token1",
              type: "core::integer::u128",
            },
            {
              name: "collect_fees",
              type: "core::bool",
            },
          ],
          outputs: [
            {
              type: "(core::integer::u128, core::integer::u128)",
            },
          ],
          state_mutability: "external",
        },
      ],
    },
    {
      type: "impl",
      name: "Upgradeable",
      interface_name: "ekubo::interfaces::upgradeable::IUpgradeable",
    },
    {
      type: "interface",
      name: "ekubo::interfaces::upgradeable::IUpgradeable",
      items: [
        {
          type: "function",
          name: "replace_class_hash",
          inputs: [
            {
              name: "class_hash",
              type: "core::starknet::class_hash::ClassHash",
            },
          ],
          outputs: [],
          state_mutability: "external",
        },
      ],
    },
    {
      type: "impl",
      name: "Clear",
      interface_name: "ekubo::clear::IClear",
    },
    {
      type: "struct",
      name: "ekubo::interfaces::erc20::IERC20Dispatcher",
      members: [
        {
          name: "contract_address",
          type: "core::starknet::contract_address::ContractAddress",
        },
      ],
    },
    {
      type: "interface",
      name: "ekubo::clear::IClear",
      items: [
        {
          type: "function",
          name: "clear",
          inputs: [
            {
              name: "token",
              type: "ekubo::interfaces::erc20::IERC20Dispatcher",
            },
          ],
          outputs: [
            {
              type: "core::integer::u256",
            },
          ],
          state_mutability: "view",
        },
      ],
    },
    {
      type: "struct",
      name: "ekubo::interfaces::core::ICoreDispatcher",
      members: [
        {
          name: "contract_address",
          type: "core::starknet::contract_address::ContractAddress",
        },
      ],
    },
    {
      type: "constructor",
      name: "constructor",
      inputs: [
        {
          name: "core",
          type: "ekubo::interfaces::core::ICoreDispatcher",
        },
        {
          name: "nft_class_hash",
          type: "core::starknet::class_hash::ClassHash",
        },
        {
          name: "token_uri_base",
          type: "core::felt252",
        },
      ],
    },
    {
      type: "event",
      name: "ekubo::upgradeable::Upgradeable::ClassHashReplaced",
      kind: "struct",
      members: [
        {
          name: "new_class_hash",
          type: "core::starknet::class_hash::ClassHash",
          kind: "data",
        },
      ],
    },
    {
      type: "event",
      name: "ekubo::upgradeable::Upgradeable::Event",
      kind: "enum",
      variants: [
        {
          name: "ClassHashReplaced",
          type: "ekubo::upgradeable::Upgradeable::ClassHashReplaced",
          kind: "nested",
        },
      ],
    },
    {
      type: "struct",
      name: "ekubo::types::delta::Delta",
      members: [
        {
          name: "amount0",
          type: "ekubo::types::i129::i129",
        },
        {
          name: "amount1",
          type: "ekubo::types::i129::i129",
        },
      ],
    },
    {
      type: "event",
      name: "ekubo::positions::Positions::Deposit",
      kind: "struct",
      members: [
        {
          name: "id",
          type: "core::integer::u64",
          kind: "data",
        },
        {
          name: "pool_key",
          type: "ekubo::types::keys::PoolKey",
          kind: "data",
        },
        {
          name: "bounds",
          type: "ekubo::types::bounds::Bounds",
          kind: "data",
        },
        {
          name: "liquidity",
          type: "core::integer::u128",
          kind: "data",
        },
        {
          name: "delta",
          type: "ekubo::types::delta::Delta",
          kind: "data",
        },
      ],
    },
    {
      type: "event",
      name: "ekubo::positions::Positions::Withdraw",
      kind: "struct",
      members: [
        {
          name: "id",
          type: "core::integer::u64",
          kind: "data",
        },
        {
          name: "pool_key",
          type: "ekubo::types::keys::PoolKey",
          kind: "data",
        },
        {
          name: "bounds",
          type: "ekubo::types::bounds::Bounds",
          kind: "data",
        },
        {
          name: "liquidity",
          type: "core::integer::u128",
          kind: "data",
        },
        {
          name: "delta",
          type: "ekubo::types::delta::Delta",
          kind: "data",
        },
        {
          name: "collect_fees",
          type: "core::bool",
          kind: "data",
        },
        {
          name: "recipient",
          type: "core::starknet::contract_address::ContractAddress",
          kind: "data",
        },
      ],
    },
    {
      type: "event",
      name: "ekubo::positions::Positions::PositionMinted",
      kind: "struct",
      members: [
        {
          name: "id",
          type: "core::integer::u64",
          kind: "data",
        },
        {
          name: "pool_key",
          type: "ekubo::types::keys::PoolKey",
          kind: "data",
        },
        {
          name: "bounds",
          type: "ekubo::types::bounds::Bounds",
          kind: "data",
        },
        {
          name: "referrer",
          type: "core::starknet::contract_address::ContractAddress",
          kind: "data",
        },
      ],
    },
    {
      type: "event",
      name: "ekubo::positions::Positions::Event",
      kind: "enum",
      variants: [
        {
          name: "UpgradeableEvent",
          type: "ekubo::upgradeable::Upgradeable::Event",
          kind: "flat",
        },
        {
          name: "Deposit",
          type: "ekubo::positions::Positions::Deposit",
          kind: "nested",
        },
        {
          name: "Withdraw",
          type: "ekubo::positions::Positions::Withdraw",
          kind: "nested",
        },
        {
          name: "PositionMinted",
          type: "ekubo::positions::Positions::PositionMinted",
          kind: "nested",
        },
      ],
    },
  ],
  process.env.POSITIONS_ADDRESS,
  provider
);
