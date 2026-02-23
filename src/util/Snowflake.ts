/**
 * Formats an array of query parameters for the Snowflake Node.js SDK.
 *
 * Snowflake binds expect scalar values (string, number, boolean, Date, or null).
 * This function normalises common JavaScript types:
 * - `undefined` / `null` → `null`
 * - `Date` → passed through unchanged
 * - `Buffer` / `Uint8Array` → base64-encoded string
 * - `BigInt` → string (Snowflake accepts numeric strings for BIGINT columns)
 * - `NaN` / `Infinity` / `-Infinity` → `null` (not valid SQL numbers)
 * - `Map` / `Set` → JSON-stringified after conversion to plain objects/arrays
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
        // NaN / Infinity are not valid SQL bind values
        if (typeof item === "number" && !Number.isFinite(item)) {
            return null
        }
        if (
            (typeof Buffer !== "undefined" && Buffer.isBuffer(item)) ||
            item instanceof Uint8Array
        ) {
            // Use Uint8Array check first for cross-runtime (Deno/Bun) safety.
            // Buffer.from(Uint8Array) works in Node; in non-Node runtimes
            // Buffer may not exist so we fall back to manual base64.
            if (typeof Buffer !== "undefined") {
                return Buffer.from(
                    item.buffer,
                    item.byteOffset,
                    item.byteLength,
                ).toString("base64")
            }
            // Fallback for non-Node environments
            let binary = ""
            for (let i = 0; i < item.length; i++) {
                binary += String.fromCharCode(item[i])
            }
            return btoa(binary)
        }
        if (typeof item === "object") {
            // Convert Map/Set to JSON-serializable forms
            let target = item
            if (item instanceof Map) {
                target = Object.fromEntries(item)
            } else if (item instanceof Set) {
                target = Array.from(item)
            }
            try {
                return JSON.stringify(target, (_key, value) =>
                    typeof value === "bigint" ? value.toString() : value,
                )
            } catch {
                // Circular reference or other stringify failure —
                // return the toString() representation rather than crashing.
                return String(item)
            }
        }
        return item
    })
}
