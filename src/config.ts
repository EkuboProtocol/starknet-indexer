import { config } from "dotenv";

config({ path: `./.env.local` });
config({ path: `./.env.${process.env.NETWORK}.local` });
config({ path: `./.env.${process.env.NETWORK}` });
config({ path: `./.env` });
