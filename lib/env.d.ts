declare namespace NodeJS {
    export interface ProcessEnv {
        LOG_LEVEL: string;

        CORE_ADDRESS: string;
        POSITIONS_ADDRESS: string;
        STARTING_CURSOR_BLOCK_NUMBER: string;

        APIBARA_AUTH_TOKEN: string;

        PGHOST: string;
        PGPORT: string;
        PGCERT: string;
        PGUSER: string;
        PGPASSWORD: string;
        PGDATABASE: string;
    }
}
