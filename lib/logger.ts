import { createLogger, format, transports } from "winston";

export const logger = createLogger({
  level: process.env.LOG_LEVEL ?? "info",
  format: format.combine(
    format.timestamp({
      format: "YYYY-MM-DD HH:mm:ss",
    }),
    format.errors({ stack: true }),
    format.splat(),
    format.json()
  ),
  defaultMeta: { service: "ekubo-indexer" },
  transports: [new transports.Console()],
});

process.on("uncaughtException", function (err) {
  logger.error("Uncaught exception", err);
  process.exit(1); // Exit the process with failure
});

process.on("unhandledRejection", function (err, promise) {
  logger.error("Unhandled promise rejection", err);
  process.exit(1); // Exit the process with failure
});
