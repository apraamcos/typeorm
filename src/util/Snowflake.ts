// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const formatSnowflakeParameter = (arr: any[]): any[] => {
    return arr.map((obj) =>
        Object.fromEntries(
            Object.entries(obj).map(([key, value]) => [
                key,
                value === undefined
                    ? null
                    : Object.prototype.toString.call(value) ===
                          "[object Object]" || Array.isArray(value)
                    ? JSON.stringify(value)
                    : value,
            ]),
        ),
    )
}
