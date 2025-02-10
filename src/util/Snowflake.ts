// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const formatSnowflakeParameter = (arr?: any[]): any[] | undefined => {
    return arr
        ? arr.map((item) => (typeof item === "undefined" ? null : item))
        : undefined
}
