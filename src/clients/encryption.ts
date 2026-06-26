// Secret redaction, ported from clients/encryption.py.

const SECRET_KEY_PREFIX = '#';
const ENCRYPTED_VALUE_PREFIX = 'KBC::';
export const REDACTED_SECRET_VALUE = '[REDACTED]';

const isEncryptedValue = (value: unknown): boolean =>
  typeof value === 'string' && value.startsWith(ENCRYPTED_VALUE_PREFIX);

/**
 * Deep-copies a value, replacing plaintext `#`-prefixed secret values with `[REDACTED]`.
 * Values already encrypted by the encryption service (`KBC::` ciphers) are kept (opaque).
 * Plaintext secrets must never reach the model context.
 */
export const redactSecrets = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map(redactSecrets);
  }
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      out[key] =
        key.startsWith(SECRET_KEY_PREFIX) && !isEncryptedValue(item)
          ? REDACTED_SECRET_VALUE
          : redactSecrets(item);
    }
    return out;
  }
  return value;
};
