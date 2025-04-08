declare namespace NodeJS {
  export interface ProcessEnv {
    LOG_LEVEL: string;

    CORE_ADDRESS: `0x${string}`;
    POSITIONS_ADDRESS: `0x${string}`;
    NFT_ADDRESS: `0x${string}`;
    TWAMM_ADDRESS: `0x${string}`;
    STAKER_ADDRESS: `0x${string}`;
    GOVERNOR_ADDRESS: `0x${string}`;
    ORACLE_ADDRESS: `0x${string}`;
    LIMIT_ORDERS_ADDRESS: `0x${string}`;

    TOKEN_REGISTRY_ADDRESS: `0x${string}`;
    TOKEN_REGISTRY_V2_ADDRESS: `0x${string}`;
    TOKEN_REGISTRY_V3_ADDRESS: `0x${string}`;

    STARTING_CURSOR_BLOCK_NUMBER: string;

    REFRESH_RATE_ANALYTICAL_VIEWS: string;

    NETWORK: "mainnet" | "sepolia" | string;

    APIBARA_URL: string;
    APIBARA_AUTH_TOKEN: string;

    PG_CONNECTION_STRING: string;
    
    NO_BLOCKS_TIMEOUT_MS: string; // Time in milliseconds before exiting if no blocks are received
  }
}
