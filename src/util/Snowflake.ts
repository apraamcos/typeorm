// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const formatSnowflakeParameter = (arr?: any[]): any[] | undefined => {
    return arr
        ? arr.map((item) => {
              if (typeof item === "undefined" || item === null) {
                  return null
              }
              if (item instanceof Date) {
                  return item
              }
              if (typeof item === "object") {
                  return JSON.stringify(item)
              }
              return item
          })
        : undefined
}
