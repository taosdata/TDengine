import winston from "winston";
import DailyRotateFile from "winston-daily-rotate-file";

const customFormat = winston.format.printf(
    ({ level, message, label, timestamp }) => {
        if (
            message &&
            typeof message === "object" &&
            typeof (message as any).toJSON === "function"
        ) {
            message = (message as any).toJSON();
        }
        return `${timestamp} [${label}] ${level}: ${message}`;
    }
);

const transport = new DailyRotateFile({
    filename: "./logs/app-%DATE%.log", // Here is the file name template
    datePattern: "YYYY-MM-DD", // date format
    zippedArchive: true, // Whether to compress the archive file into gzip format
    maxSize: "20m", // Single file size limit
    maxFiles: "14d", // Keep log files for 14 days
    handleExceptions: true, // Whether to handle exceptions
    json: false, // Whether to output logs in JSON format
    format: winston.format.combine(
        winston.format.label({ label: "node.js websocket" }),
        winston.format.timestamp(),
        customFormat
    ),
    level: "info", // set log level
});

const logger = winston.createLogger({
    transports: [transport],
    exitOnError: false, // Do not exit the process when an error occurs
});

export function setLevel(level: string) {
    transport.level = level;
}

export default logger;
