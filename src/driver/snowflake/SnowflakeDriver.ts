import type { Connection, SnowflakeError } from "snowflake-sdk"
import { createConnection, configure } from "snowflake-sdk"
import { ObjectLiteral } from "../../common/ObjectLiteral"
import { DataSource } from "../../data-source"
import { ColumnMetadata } from "../../metadata/ColumnMetadata"
import { EntityMetadata } from "../../metadata/EntityMetadata"
import { QueryRunner } from "../../query-runner/QueryRunner"
import { RdbmsSchemaBuilder } from "../../schema-builder/RdbmsSchemaBuilder"
import { Table } from "../../schema-builder/table/Table"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { View } from "../../schema-builder/view/View"
import { ApplyValueTransformers } from "../../util/ApplyValueTransformers"
import { DateUtils } from "../../util/DateUtils"
import { InstanceChecker } from "../../util/InstanceChecker"
import { OrmUtils } from "../../util/OrmUtils"
import { Driver } from "../Driver"
import { ColumnType } from "../types/ColumnTypes"
import { CteCapabilities } from "../types/CteCapabilities"
import { DataTypeDefaults } from "../types/DataTypeDefaults"
import { MappedColumnTypes } from "../types/MappedColumnTypes"
import { UpsertType } from "../types/UpsertType"
import { SnowflakeConnectionOptions } from "./SnowflakeConnectionOptions"
import { SnowflakeQueryRunner } from "./SnowflakeQueryRunner"

export class SnowflakeDriver implements Driver {
    // -------------------------------------------------------------------------
    // Public Properties
    // -------------------------------------------------------------------------

    /**
     * Connection used by driver.
     */
    connection: DataSource

    /**
     * Real database connection with snowflake database.
     */
    databaseConnection: any

    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    /**
     * Connection options.
     */
    options: SnowflakeConnectionOptions

    /**
     * Database name used to perform all write queries.
     */
    database?: string

    /**
     * Schema name used to perform all write queries.
     */
    schema?: string

    /**
     * We store all created query runners because we need to release them.
     */
    connectedQueryRunners: QueryRunner[] = []

    /**
     * Schema that's used internally by Postgres for object resolution.
     *
     * Because we never set this we have to track it in separately from the `schema` so
     * we know when we have to specify the full schema or not.
     *
     * In most cases this will be `public`.
     */
    searchSchema?: string

    /**
     * Indicates if replication is enabled.
     */
    isReplicated: boolean = false

    /**
     * Indicates if tree tables are supported by this driver.
     */
    treeSupport = true

    /**
     * Represent transaction support by this driver
     */
    transactionSupport = "nested" as const

    /**
     * Gets list of supported column data types by a driver.
     *
     * @seehttps://docs.snowflake.com/en/sql-reference/intro-summary-data-types
     */
    supportedDataTypes: ColumnType[] = [
        "number",
        "decimal",
        "numeric",
        "int",
        "integer",
        "bigint",
        "smallint",
        "tinyint",
        "byteint",
        "float",
        "float4",
        "float8",
        "double",
        "double precision",
        "real",
        "varchar",
        "char",
        "character",
        "string",
        "text",
        "binary",
        "varbinary",
        "boolean",
        "date",
        "datetime",
        "time",
        "timestamp",
        "timestamp_ltz",
        "timestamp_ntz",
        "timestamp_tz",
        "timestamp with time zone",
        "jsonb",
        "variant",
        "object",
        "array",
        "geometry",
        "geography",
        "character varying",
    ]

    /**
     * Returns type of upsert supported by driver if any
     */
    supportedUpsertTypes: UpsertType[] = []

    /**
     * Gets list of spatial column data types.
     */
    spatialTypes: ColumnType[] = ["geometry", "geography"]

    /**
     * Gets list of column data types that support length by a driver.
     */
    withLengthColumnTypes: ColumnType[] = [
        "varchar",
        "char",
        "character",
        "string",
        "text",
        "binary",
        "varbinary",
    ]

    /**
     * Gets list of column data types that support precision by a driver.
     */
    withPrecisionColumnTypes: ColumnType[] = [
        "numeric",
        "decimal",
        "timestamp with local time zone",
        "timestamp without time zone",
        "timestamp with time zone",
    ]

    /**
     * Gets list of column data types that support scale by a driver.
     */
    withScaleColumnTypes: ColumnType[] = ["numeric", "decimal"]

    /**
     * Orm has special columns and we need to know what database column types should be for those types.
     * Column types are driver dependant.
     */
    mappedDataTypes: MappedColumnTypes = {
        createDate: "timestamp",
        createDateDefault: "now()",
        updateDate: "timestamp",
        updateDateDefault: "now()",
        deleteDate: "timestamp",
        deleteDateNullable: true,
        version: "int4",
        treeLevel: "int4",
        migrationId: "int4",
        migrationName: "varchar",
        migrationTimestamp: "int8",
        cacheId: "int4",
        cacheIdentifier: "varchar",
        cacheTime: "int8",
        cacheDuration: "int4",
        cacheQuery: "text",
        cacheResult: "text",
        metadataType: "varchar",
        metadataDatabase: "varchar",
        metadataSchema: "varchar",
        metadataTable: "varchar",
        metadataName: "varchar",
        metadataValue: "text",
    }

    /**
     * The prefix used for the parameters
     */
    parametersPrefix: string = ":"

    /**
     * Default values of length, precision and scale depends on column data type.
     * Used in the cases when length/precision/scale is not specified by user.
     */
    dataTypeDefaults: DataTypeDefaults = {
        character: { length: 1 },
        bit: { length: 1 },
        interval: { precision: 6 },
        timestamp_ltz: { precision: 6 },
        timestamp_ntz: { precision: 6 },
        timestamp_tz: { precision: 6 },
    }

    cteCapabilities: CteCapabilities = {
        enabled: true,
        writable: true,
        requiresRecursiveHint: true,
        materializedHint: true,
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(connection?: DataSource) {
        if (!connection) {
            return
        }

        this.connection = connection
        this.options = connection.options as SnowflakeConnectionOptions

        // load postgres package
        // this.loadDependencies()

        this.database = this.options.database
        this.schema = this.options.schema
    }

    // -------------------------------------------------------------------------
    // Public Implemented Methods
    // -------------------------------------------------------------------------

    async createSnowflakeConnection(isRetry?: boolean): Promise<Connection> {
        const { logger } = this.connection
        try {
            return await new Promise((resolve, reject) => {
                return createConnection(this.options).connect(
                    (err: SnowflakeError | undefined, conn: Connection) =>
                        err ? reject(err) : resolve(conn),
                )
            })
        } catch (err) {
            if (
                err.errorMessage?.includes(
                    "Network error. Could not reach Snowflake.",
                ) &&
                !isRetry
            ) {
                console.info("createSnowflakeConnection Retry", err.message)
                return await this.createSnowflakeConnection(true)
            }
            logger.log("warn", err)
            throw err
        }
    }
    /**
     * Performs connection to the database.
     * TODO: Add pool connection
     */
    async connect(): Promise<void> {
        configure({
            logLevel: "ERROR",
        })
    }

    /**
     * Closes connection with database.
     */
    async disconnect(): Promise<void> {}

    /**
     * Creates a query runner used to execute database queries.
     */
    createQueryRunner(): QueryRunner {
        return new SnowflakeQueryRunner(this)
    }

    /**
     * Makes any action after connection (e.g. create extensions in Postgres driver).
     */
    async afterConnect(): Promise<void> {}

    /**
     * Creates a schema builder used to build and sync a schema.
     */
    createSchemaBuilder() {
        return new RdbmsSchemaBuilder(this.connection)
    }

    /**
     * Replaces parameters in the given sql with special escaping character
     * and an array of parameter names to be passed to a query.
     */
    escapeQueryWithParameters(
        sql: string,
        parameters: ObjectLiteral,
        nativeParameters: ObjectLiteral,
    ): [string, any[]] {
        const escapedParameters: any[] = Object.keys(nativeParameters).map(
            (key) => nativeParameters[key],
        )
        if (!parameters || !Object.keys(parameters).length)
            return [sql, escapedParameters]

        const parameterIndexMap = new Map<string, number>()
        sql = sql.replace(
            /:(\.\.\.)?([A-Za-z0-9_.]+)/g,
            (full, isArray: string, key: string): string => {
                if (!parameters.hasOwnProperty(key)) {
                    return full
                }

                if (parameterIndexMap.has(key)) {
                    return this.parametersPrefix + parameterIndexMap.get(key)
                }

                const value: any = parameters[key]

                if (isArray) {
                    return value
                        .map((v: any) => {
                            escapedParameters.push(v)
                            return this.createParameter(
                                key,
                                escapedParameters.length - 1,
                            )
                        })
                        .join(", ")
                }

                if (typeof value === "function") {
                    return value()
                }

                escapedParameters.push(value)
                parameterIndexMap.set(key, escapedParameters.length)
                return this.createParameter(key, escapedParameters.length - 1)
            },
        ) // todo: make replace only in value statements, otherwise problems

        return [sql, escapedParameters]
    }
    /**
     * Creates an escaped parameter.
     */
    createParameter(parameterName: string, index: number): string {
        return this.parametersPrefix + (index + 1)
    }

    /**
     * Escapes a column name.
     */
    escape(columnName: string): string {
        return '"' + columnName + '"'
    }

    /**
     * Build full table name with schema name and table name.
     * E.g. myDB.mySchema.myTable
     */
    buildTableName(tableName: string, schema?: string): string {
        const tablePath = [tableName]

        if (schema) {
            tablePath.unshift(schema)
        }

        if (this.database) {
            tablePath.unshift(this.database)
        }

        return tablePath.join(".")
    }

    /**
     * Parse a target table name or other types and return a normalized table definition.
     */
    parseTableName(
        target: EntityMetadata | Table | View | TableForeignKey | string,
    ): { database?: string; schema?: string; tableName: string } {
        const driverDatabase = this.database
        const driverSchema = this.schema

        if (InstanceChecker.isTable(target) || InstanceChecker.isView(target)) {
            const parsed = this.parseTableName(target.name)

            return {
                database: target.database || parsed.database || driverDatabase,
                schema: target.schema || parsed.schema || driverSchema,
                tableName: parsed.tableName,
            }
        }

        if (InstanceChecker.isTableForeignKey(target)) {
            const parsed = this.parseTableName(target.referencedTableName)

            return {
                database:
                    target.referencedDatabase ||
                    parsed.database ||
                    driverDatabase,
                schema:
                    target.referencedSchema || parsed.schema || driverSchema,
                tableName: parsed.tableName,
            }
        }

        if (InstanceChecker.isEntityMetadata(target)) {
            // EntityMetadata tableName is never a path

            return {
                database: target.database || driverDatabase,
                schema: target.schema || driverSchema,
                tableName: target.tableName,
            }
        }

        const parts = target.split(".")

        return {
            database: driverDatabase,
            schema: (parts.length > 1 ? parts[0] : undefined) || driverSchema,
            tableName: parts.length > 1 ? parts[1] : parts[0],
        }
    }

    /**
     * Prepares given value to a value to be persisted, based on its column type and metadata.
     */
    preparePersistentValue(value: any, columnMetadata: ColumnMetadata): any {
        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformTo(
                columnMetadata.transformer,
                value,
            )

        if (value === null || value === undefined) return value

        if (columnMetadata.type === Boolean) {
            return value === true ? 1 : 0
        } else if (columnMetadata.type === "date") {
            return DateUtils.mixedDateToDateString(value)
        } else if (columnMetadata.type === "time") {
            return DateUtils.mixedDateToTimeString(value)
        } else if (
            columnMetadata.type === "datetime" ||
            columnMetadata.type === Date ||
            columnMetadata.type === "timestamp" ||
            columnMetadata.type === "timestamp with time zone" ||
            columnMetadata.type === "timestamp without time zone"
        ) {
            return DateUtils.mixedDateToDate(value)
        } else if (
            ["json", "jsonb", "variant", ...this.spatialTypes].indexOf(
                columnMetadata.type,
            ) >= 0
        ) {
            return JSON.stringify(value)
        } else if (columnMetadata.type === "hstore") {
            if (typeof value === "string") {
                return value
            } else {
                // https://www.postgresql.org/docs/9.0/hstore.html
                const quoteString = (value: unknown) => {
                    // If a string to be quoted is `null` or `undefined`, we return a literal unquoted NULL.
                    // This way, NULL values can be stored in the hstore object.
                    if (value === null || typeof value === "undefined") {
                        return "NULL"
                    }
                    // Convert non-null values to string since HStore only stores strings anyway.
                    // To include a double quote or a backslash in a key or value, escape it with a backslash.
                    return `"${`${value}`.replace(/(?=["\\])/g, "\\")}"`
                }
                return Object.keys(value)
                    .map(
                        (key) =>
                            quoteString(key) + "=>" + quoteString(value[key]),
                    )
                    .join(",")
            }
        } else if (columnMetadata.type === "simple-array") {
            return DateUtils.simpleArrayToString(value)
        } else if (columnMetadata.type === "simple-json") {
            return DateUtils.simpleJsonToString(value)
        } else if (columnMetadata.type === "cube") {
            if (columnMetadata.isArray) {
                return `{${value
                    .map((cube: number[]) => `"(${cube.join(",")})"`)
                    .join(",")}}`
            }
            return `(${value.join(",")})`
        } else if (columnMetadata.type === "ltree") {
            return value
                .split(".")
                .filter(Boolean)
                .join(".")
                .replace(/[\s]+/g, "_")
        } else if (
            (columnMetadata.type === "enum" ||
                columnMetadata.type === "simple-enum") &&
            !columnMetadata.isArray
        ) {
            return "" + value
        }

        return value
    }

    /**
     * Prepares given value to a value to be persisted, based on its column type or metadata.
     */
    prepareHydratedValue(value: any, columnMetadata: ColumnMetadata): any {
        if (value === null || value === undefined)
            return columnMetadata.transformer
                ? ApplyValueTransformers.transformFrom(
                      columnMetadata.transformer,
                      value,
                  )
                : value

        if (columnMetadata.type === Boolean) {
            value = value ? true : false
        } else if (
            columnMetadata.type === "datetime" ||
            columnMetadata.type === Date ||
            columnMetadata.type === "timestamp" ||
            columnMetadata.type === "timestamp with time zone" ||
            columnMetadata.type === "timestamp without time zone"
        ) {
            value = DateUtils.normalizeHydratedDate(value)
        } else if (columnMetadata.type === "date") {
            value = DateUtils.mixedDateToDateString(value)
        } else if (columnMetadata.type === "time") {
            value = DateUtils.mixedTimeToString(value)
        } else if (columnMetadata.type === "hstore") {
            if (columnMetadata.hstoreType === "object") {
                const unescapeString = (str: string) =>
                    str.replace(/\\./g, (m) => m[1])
                const regexp =
                    /"([^"\\]*(?:\\.[^"\\]*)*)"=>(?:(NULL)|"([^"\\]*(?:\\.[^"\\]*)*)")(?:,|$)/g
                const object: ObjectLiteral = {}
                ;`${value}`.replace(
                    regexp,
                    (_, key, nullValue, stringValue) => {
                        object[unescapeString(key)] = nullValue
                            ? null
                            : unescapeString(stringValue)
                        return ""
                    },
                )
                value = object
            }
        } else if (columnMetadata.type === "simple-array") {
            value = DateUtils.stringToSimpleArray(value)
        } else if (columnMetadata.type === "simple-json") {
            value = DateUtils.stringToSimpleJson(value)
        } else if (columnMetadata.type === "cube") {
            value = value.replace(/[\(\)\s]+/g, "") // remove whitespace
            if (columnMetadata.isArray) {
                /**
                 * Strips these groups from `{"1,2,3","",NULL}`:
                 * 1. ["1,2,3", undefined]  <- cube of arity 3
                 * 2. ["", undefined]         <- cube of arity 0
                 * 3. [undefined, "NULL"]     <- NULL
                 */
                const regexp = /(?:\"((?:[\d\s\.,])*)\")|(?:(NULL))/g
                const unparsedArrayString = value

                value = []
                let cube: RegExpExecArray | null = null
                // Iterate through all regexp matches for cubes/null in array
                while ((cube = regexp.exec(unparsedArrayString)) !== null) {
                    if (cube[1] !== undefined) {
                        value.push(
                            cube[1].split(",").filter(Boolean).map(Number),
                        )
                    } else {
                        value.push(undefined)
                    }
                }
            } else {
                value = value.split(",").filter(Boolean).map(Number)
            }
        } else if (
            columnMetadata.type === "enum" ||
            columnMetadata.type === "simple-enum"
        ) {
            if (columnMetadata.isArray) {
                if (value === "{}") return []

                // manually convert enum array to array of values (pg does not support, see https://github.com/brianc/node-pg-types/issues/56)
                value = (value as string)
                    .substring(1, (value as string).length - 1)
                    .split(",")
                    .map((val) => {
                        // replace double quotes from the beginning and from the end
                        if (val.startsWith(`"`) && val.endsWith(`"`))
                            val = val.slice(1, -1)
                        // replace double escaped backslash to single escaped e.g. \\\\ -> \\
                        val = val.replace(/(\\\\)/g, "\\")
                        // replace escaped double quotes to non-escaped e.g. \"asd\" -> "asd"
                        return val.replace(/(\\")/g, '"')
                    })

                // convert to number if that exists in possible enum options
                value = value.map((val: string) => {
                    return !isNaN(+val) &&
                        columnMetadata.enum!.indexOf(parseInt(val)) >= 0
                        ? parseInt(val)
                        : val
                })
            } else {
                // convert to number if that exists in possible enum options
                value =
                    !isNaN(+value) &&
                    columnMetadata.enum!.indexOf(parseInt(value)) >= 0
                        ? parseInt(value)
                        : value
            }
        } else if (columnMetadata.type === Number) {
            // convert to number if number
            value = !isNaN(+value) ? parseInt(value) : value
        }

        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformFrom(
                columnMetadata.transformer,
                value,
            )
        return value
    }

    /**
     * Creates a database type from a given column metadata.
     */
    normalizeType(column: {
        type?: ColumnType
        length?: number | string
        precision?: number | null
        scale?: number
        isArray?: boolean
    }): string {
        if (
            column.type === Number ||
            column.type === "int" ||
            column.type === "int4"
        ) {
            return "integer"
        } else if (column.type === String || column.type === "varchar") {
            return "character varying"
        } else if (column.type === Date || column.type === "timestamp") {
            return "timestamp_ntz"
        } else if (column.type === "timestamptz") {
            return "timestamp_ltz"
        } else if (column.type === "time") {
            return "timestamp_ntz"
        } else if (column.type === "timetz") {
            return "timestamp_ltz"
        } else if (column.type === Boolean || column.type === "bool") {
            return "boolean"
        } else if (column.type === "simple-array") {
            return "text"
        } else if (column.type === "simple-json") {
            return "text"
        } else if (column.type === "simple-enum") {
            return "enum"
        } else if (column.type === "int2") {
            return "smallint"
        } else if (column.type === "int8") {
            return "bigint"
        } else if (column.type === "decimal") {
            return "numeric"
        } else if (column.type === "float8" || column.type === "float") {
            return "double precision"
        } else if (column.type === "float4") {
            return "real"
        } else if (column.type === "char") {
            return "character"
        } else {
            return (column.type as string) || ""
        }
    }

    /**
     * Normalizes "default" value of the column.
     */
    normalizeDefault(columnMetadata: ColumnMetadata): string | undefined {
        console.log("normalizeDefault: columnMetadata = ", columnMetadata)
        const defaultValue = columnMetadata.default

        if (defaultValue === null) {
            return undefined
        }

        if (columnMetadata.isArray && Array.isArray(defaultValue)) {
            return `'{${defaultValue
                .map((val: string) => `${val}`)
                .join(",")}}'`
        }

        if (
            (columnMetadata.type === "enum" ||
                columnMetadata.type === "simple-enum" ||
                typeof defaultValue === "number" ||
                typeof defaultValue === "string") &&
            defaultValue !== undefined
        ) {
            return `'${defaultValue}'`
        }

        if (typeof defaultValue === "boolean") {
            return defaultValue ? "true" : "false"
        }

        if (typeof defaultValue === "function") {
            const value = defaultValue()

            return this.normalizeDatetimeFunction(value)
        }

        if (typeof defaultValue === "object") {
            return `'${JSON.stringify(defaultValue)}'`
        }

        if (defaultValue === undefined) {
            return undefined
        }

        return `${defaultValue}`
    }

    /**
     * Normalizes "isUnique" value of the column.
     */
    normalizeIsUnique(column: ColumnMetadata): boolean {
        return column.entityMetadata.uniques.some(
            (uq) => uq.columns.length === 1 && uq.columns[0] === column,
        )
    }

    /**
     * Returns default column lengths, which is required on column creation.
     */
    getColumnLength(column: ColumnMetadata): string {
        return column.length ? column.length.toString() : ""
    }

    /**
     * Creates column type definition including length, precision and scale
     */
    createFullType(column: TableColumn): string {
        let type = column.type

        if (column.length) {
            type += "(" + column.length + ")"
        } else if (
            column.precision !== null &&
            column.precision !== undefined &&
            column.scale !== null &&
            column.scale !== undefined
        ) {
            type += "(" + column.precision + "," + column.scale + ")"
        } else if (
            column.precision !== null &&
            column.precision !== undefined
        ) {
            type += "(" + column.precision + ")"
        }

        if (column.type === "time without time zone") {
            type =
                "TIME" +
                (column.precision !== null && column.precision !== undefined
                    ? "(" + column.precision + ")"
                    : "")
        } else if (column.type === "time with time zone") {
            type =
                "TIME" +
                (column.precision !== null && column.precision !== undefined
                    ? "(" + column.precision + ")"
                    : "") +
                " WITH TIME ZONE"
        } else if (column.type === "timestamp without time zone") {
            type =
                "TIMESTAMP" +
                (column.precision !== null && column.precision !== undefined
                    ? "(" + column.precision + ")"
                    : "")
        } else if (column.type === "timestamp with time zone") {
            type =
                "TIMESTAMP" +
                (column.precision !== null && column.precision !== undefined
                    ? "(" + column.precision + ")"
                    : "") +
                " WITH TIME ZONE"
        } else if (this.spatialTypes.indexOf(column.type as ColumnType) >= 0) {
            if (column.spatialFeatureType != null && column.srid != null) {
                type = `${column.type}(${column.spatialFeatureType},${column.srid})`
            } else if (column.spatialFeatureType != null) {
                type = `${column.type}(${column.spatialFeatureType})`
            } else {
                type = column.type
            }
        }

        if (column.isArray) type += " array"

        return type
    }

    /**
     * Creates generated map of values generated or returned by database after INSERT query.
     *
     * todo: slow. optimize Object.keys(), OrmUtils.mergeDeep and column.createValueMap parts
     */
    createGeneratedMap(metadata: EntityMetadata, insertResult: ObjectLiteral) {
        if (!insertResult) return undefined

        return Object.keys(insertResult).reduce((map, key) => {
            const column = metadata.findColumnWithDatabaseName(key)
            if (column) {
                OrmUtils.mergeDeep(
                    map,
                    column.createValueMap(insertResult[key]),
                )
                // OrmUtils.mergeDeep(map, column.createValueMap(this.prepareHydratedValue(insertResult[key], column))); // TODO: probably should be like there, but fails on enums, fix later
            }
            return map
        }, {} as ObjectLiteral)
    }

    /**
     * Obtains a new database connection to a master server.
     * Used for replication.
     * If replication is not setup then returns default connection's database connection.
     */
    obtainMasterConnection(): Promise<any> {
        return Promise.resolve()
    }

    /**
     * Obtains a new database connection to a slave server.
     * Used for replication.
     * If replication is not setup then returns master (default) connection's database connection.
     */
    obtainSlaveConnection(): Promise<any> {
        return Promise.resolve()
    }

    /**
     * Differentiate columns of this table and columns from the given column metadatas columns
     * and returns only changed.
     */
    findChangedColumns(
        tableColumns: TableColumn[],
        columnMetadatas: ColumnMetadata[],
    ): ColumnMetadata[] {
        return columnMetadatas.filter((columnMetadata) => {
            const tableColumn = tableColumns.find(
                (c) => c.name === columnMetadata.databaseName,
            )
            if (!tableColumn) return false // we don't need new columns, we only need exist and changed

            const isColumnChanged =
                tableColumn.name !== columnMetadata.databaseName ||
                tableColumn.type !== this.normalizeType(columnMetadata) ||
                tableColumn.length !== columnMetadata.length ||
                tableColumn.isArray !== columnMetadata.isArray ||
                tableColumn.precision !== columnMetadata.precision ||
                (columnMetadata.scale !== undefined &&
                    tableColumn.scale !== columnMetadata.scale) ||
                tableColumn.comment !==
                    this.escapeComment(columnMetadata.comment) ||
                (!tableColumn.isGenerated &&
                    !this.defaultEqual(columnMetadata, tableColumn)) || // we included check for generated here, because generated columns already can have default values
                tableColumn.isPrimary !== columnMetadata.isPrimary ||
                tableColumn.isNullable !== columnMetadata.isNullable ||
                tableColumn.isUnique !==
                    this.normalizeIsUnique(columnMetadata) ||
                tableColumn.enumName !== columnMetadata.enumName ||
                (tableColumn.enum &&
                    columnMetadata.enum &&
                    !OrmUtils.isArraysEqual(
                        tableColumn.enum,
                        columnMetadata.enum.map((val) => val + ""),
                    )) || // enums in postgres are always strings
                tableColumn.isGenerated !== columnMetadata.isGenerated ||
                (tableColumn.spatialFeatureType || "").toLowerCase() !==
                    (columnMetadata.spatialFeatureType || "").toLowerCase() ||
                tableColumn.srid !== columnMetadata.srid ||
                tableColumn.generatedType !== columnMetadata.generatedType ||
                (tableColumn.asExpression || "").trim() !==
                    (columnMetadata.asExpression || "").trim()

            return isColumnChanged
        })
    }

    /**
     * Returns true if driver supports RETURNING / OUTPUT statement.
     */
    isReturningSqlSupported(): boolean {
        return false
    }

    /**
     * Returns true if driver supports uuid values generation on its own.
     */
    isUUIDGenerationSupported(): boolean {
        return true
    }

    /**
     * Returns true if driver supports fulltext indices.
     */
    isFullTextColumnTypeSupported(): boolean {
        return false
    }

    private lowerDefaultValueIfNecessary(value: string | undefined) {
        // Postgres saves function calls in default value as lowercase #2733
        if (!value) {
            return value
        }
        return value
            .split(`'`)
            .map((v, i) => {
                return i % 2 === 1 ? v : v.toLowerCase()
            })
            .join(`'`)
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * If parameter is a datetime function, e.g. "CURRENT_TIMESTAMP", normalizes it.
     * Otherwise returns original input.
     */
    protected normalizeDatetimeFunction(value: string) {
        // check if input is datetime function
        const upperCaseValue = value.toUpperCase()
        const isDatetimeFunction =
            upperCaseValue.indexOf("CURRENT_TIMESTAMP") !== -1 ||
            upperCaseValue.indexOf("CURRENT_DATE") !== -1 ||
            upperCaseValue.indexOf("CURRENT_TIME") !== -1 ||
            upperCaseValue.indexOf("LOCALTIMESTAMP") !== -1 ||
            upperCaseValue.indexOf("LOCALTIME") !== -1

        if (isDatetimeFunction) {
            // extract precision, e.g. "(3)"
            const precision = value.match(/\(\d+\)/)

            if (upperCaseValue.indexOf("CURRENT_TIMESTAMP") !== -1) {
                return precision
                    ? `('now'::text)::timestamp${precision[0]} with time zone`
                    : "now()"
            } else if (upperCaseValue === "CURRENT_DATE") {
                return "('now'::text)::date"
            } else if (upperCaseValue.indexOf("CURRENT_TIME") !== -1) {
                return precision
                    ? `('now'::text)::time${precision[0]} with time zone`
                    : "('now'::text)::time with time zone"
            } else if (upperCaseValue.indexOf("LOCALTIMESTAMP") !== -1) {
                return precision
                    ? `('now'::text)::timestamp${precision[0]} without time zone`
                    : "('now'::text)::timestamp without time zone"
            } else if (upperCaseValue.indexOf("LOCALTIME") !== -1) {
                return precision
                    ? `('now'::text)::time${precision[0]} without time zone`
                    : "('now'::text)::time without time zone"
            }
        }

        return value
    }

    /**
     * Escapes a given comment.
     */
    protected escapeComment(comment?: string) {
        if (!comment) return comment

        comment = comment.replace(/\u0000/g, "") // Null bytes aren't allowed in comments

        return comment
    }

    /**
     * Compares "default" value of the column.
     * Postgres sorts json values before it is saved, so in that case a deep comparison has to be performed to see if has changed.
     */
    private defaultEqual(
        columnMetadata: ColumnMetadata,
        tableColumn: TableColumn,
    ): boolean {
        if (
            ["json", "jsonb"].includes(columnMetadata.type as string) &&
            !["function", "undefined"].includes(typeof columnMetadata.default)
        ) {
            const tableColumnDefault =
                typeof tableColumn.default === "string"
                    ? JSON.parse(
                          tableColumn.default.substring(
                              1,
                              tableColumn.default.length - 1,
                          ),
                      )
                    : tableColumn.default

            return OrmUtils.deepCompare(
                columnMetadata.default,
                tableColumnDefault,
            )
        }

        const columnDefault = this.lowerDefaultValueIfNecessary(
            this.normalizeDefault(columnMetadata),
        )
        return columnDefault === tableColumn.default
    }
}
