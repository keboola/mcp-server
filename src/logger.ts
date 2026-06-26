import pino from 'pino';

// Python log levels (DEBUG/INFO/...) map onto pino's lowercase levels.
const LEVELS: Record<string, string> = {
  DEBUG: 'debug',
  INFO: 'info',
  WARNING: 'warn',
  ERROR: 'error',
  CRITICAL: 'fatal',
};

export const createLogger = (level = 'INFO') =>
  // Logs go to stderr (fd 2) so stdout stays clean for the stdio JSON-RPC transport.
  pino({ level: LEVELS[level.toUpperCase()] ?? 'info' }, pino.destination(2));

export const logger = createLogger(process.env.LOG_LEVEL);
