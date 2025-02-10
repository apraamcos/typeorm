// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const formatSnowflakeParameter = (arr: any[]): any[] => {
    return arr.map((obj) =>
        obj && Object.entries(obj).length > 0 // Check if obj is non-null and non-empty
            ? Object.fromEntries(
                  Object.entries(obj).map(([key, value]) => [
                      key,
                      value === undefined
                          ? null
                          : typeof value === "object" && value !== null
                          ? JSON.stringify(value)
                          : value,
                  ]),
              )
            : obj,
    )
}
