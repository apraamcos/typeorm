/**
 * Formats an array of query parameters for the Snowflake Node.js SDK.
 *
 * Snowflake binds expect scalar values (string, number, boolean, Date, or null).
 * This function normalises common JavaScript types:
 * - `undefined` / `null` → `null`
 * - `Date` → passed through unchanged
 * - `Buffer` / `Uint8Array` → base64-encoded string
 * - `BigInt` → string (Snowflake accepts numeric strings for BIGINT columns)
 * - Plain objects → `JSON.stringify` with BigInt safety
 * - Nested arrays (batch binds) → recursively formatted
 *
 * @param arr - The parameter array to format, or `undefined`.
 * @returns The formatted array suitable for Snowflake SDK `binds`, or `undefined`
 *          if the input was `undefined`.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const formatSnowflakeParameter = (arr?: any[]): any[] | undefined => {
    if (!arr || arr.length === 0) {
        return arr
    }
    if (arr.every((x) => Array.isArray(x))) {
        return arr.map((x) => formatSnowflakeParameter(x))
    }
    return arr.map((item) => {
        if (item == null) {
            return null
        }
        if (item instanceof Date) {
            return item
        }
        if (typeof item === "bigint") {
            return item.toString()
        }
        if (
            (typeof Buffer !== "undefined" && Buffer.isBuffer(item)) ||
            item instanceof Uint8Array
        ) {
            return Buffer.from(item).toString("base64")
        }
        if (typeof item === "object") {
            return JSON.stringify(item, (_key, value) =>
                typeof value === "bigint" ? value.toString() : value,
            )
        }
        return item
    })
}
