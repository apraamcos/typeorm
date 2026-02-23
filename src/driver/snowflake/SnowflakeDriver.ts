import type {
    Connection,
    ConnectionOptions as SfConnectionOptions,
} from "snowflake-sdk"
import { createPool, configure } from "snowflake-sdk"
import type { Pool } from "generic-pool"
import { ObjectLiteral } from "../../common/ObjectLiteral"
import { DataSource } from "../../data-source"
import { TypeORMError } from "../../error"
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
import { ReplicationMode } from "../types/ReplicationMode"
import { UpsertType } from "../types/UpsertType"
import { SnowflakeConnectionOptions } from "./SnowflakeConnectionOptions"
import { SnowflakeQueryRunner } from "./SnowflakeQueryRunner"
import crypto from "crypto"

/**
 * Snowflake database driver.
 *
 * Uses snowflake-sdk's built-in connection pooling (backed by generic-pool).
 */
export class SnowflakeDriver implements Driver {
    // -------------------------------------------------------------------------
    // Public Properties
    // -------------------------------------------------------------------------

    /**
     * Connection used by driver.
     */
    connection: DataSource

    /**
     * Snowflake connection pool (backed by generic-pool via snowflake-sdk's createPool).
     */
    pool: Pool<Connection>

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
     * Indicates if replication is enabled.
     */
    isReplicated: boolean = false

    /**
     * Indicates if tree tables are supported by this driver.
     */
    treeSupport = true

    /**
     * Snowflake supports flat transactions only (BEGIN/COMMIT/ROLLBACK).
     * No savepoints, no nested transactions.
     */
    transactionSupport = "simple" as const

    /**
     * Gets list of supported column data types by a driver.
     *
     * @see https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
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
        "timestamptz",
        "timestamp with time zone",
        "timestamp without time zone",
        "variant",
        "object",
        "array",
        "geometry",
        "geography",
    ]

    /**
     * Returns type of upsert supported by driver if any
     */
    supportedUpsertTypes: UpsertType[] = ["merge-into"]

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
        "binary",
        "varbinary",
    ]

    /**
     * Gets list of column data types that support precision by a driver.
     */
    withPrecisionColumnTypes: ColumnType[] = [
        "number",
        "numeric",
        "decimal",
        "time",
        "timestamp",
        "datetime",
        "timestamp_ltz",
        "timestamp_ntz",
        "timestamp_tz",
        "timestamptz",
        "timestamp with time zone",
        "timestamp without time zone",
    ]

    /**
     * Gets list of column data types that support scale by a driver.
     */
    withScaleColumnTypes: ColumnType[] = ["number", "numeric", "decimal"]

    /**
     * Orm has special columns and we need to know what database column types should be for those types.
     * Column types are driver dependant.
     */
    mappedDataTypes: MappedColumnTypes = {
        createDate: "timestamp",
        createDateDefault: "CURRENT_TIMESTAMP",
        updateDate: "timestamp",
        updateDateDefault: "CURRENT_TIMESTAMP",
        deleteDate: "timestamp",
        deleteDateNullable: true,
        version: "integer",
        treeLevel: "integer",
        migrationId: "integer",
        migrationName: "varchar",
        migrationTimestamp: "bigint",
        cacheId: "integer",
        cacheIdentifier: "varchar",
        cacheTime: "bigint",
        cacheDuration: "integer",
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
     * Snowflake identifiers (including aliases) can be up to 255 characters.
     * @see https://docs.snowflake.com/en/sql-reference/identifiers-syntax
     */
    maxAliasLength = 255

    /**
     * Default values of length, precision and scale depends on column data type.
     * Used in the cases when length/precision/scale is not specified by user.
     */
    dataTypeDefaults: DataTypeDefaults = {
        character: { length: 1 },
        varchar: { length: 16777216 },
        number: { precision: 38, scale: 0 },
        numeric: { precision: 38, scale: 0 },
        decimal: { precision: 38, scale: 0 },
        time: { precision: 9 },
        timestamp: { precision: 6 },
        datetime: { precision: 6 },
        timestamp_ltz: { precision: 6 },
        timestamp_ntz: { precision: 6 },
        timestamp_tz: { precision: 6 },
    }

    cteCapabilities: CteCapabilities = {
        enabled: true,
        writable: false,
        requiresRecursiveHint: true,
        materializedHint: false,
    }

    /**
     * Pre-computed set of spatial types for fast lookup in `createFullType`.
     */
    private readonly spatialTypeSet: ReadonlySet<ColumnType> =
        new Set<ColumnType>(this.spatialTypes)

    /**
     * Pre-computed set of types that should be JSON-stringified on persist.
     * Avoids creating a temporary array on every `preparePersistentValue` call.
     */
    private readonly jsonStringifyTypes: ReadonlySet<ColumnType> =
        new Set<ColumnType>([
            "variant",
            "object",
            "array",
            ...this.spatialTypes,
        ])

    /**
     * Pre-computed set of types that should be treated as date/timestamp on persist and hydrate.
     */
    private readonly dateTypes: ReadonlySet<ColumnType> = new Set<ColumnType>([
        "datetime",
        Date,
        "timestamp",
        "timestamp with time zone",
        "timestamp without time zone",
        "timestamptz",
        "timestamp_ltz",
        "timestamp_ntz",
        "timestamp_tz",
    ])

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(connection?: DataSource) {
        if (!connection) {
            return
        }

        this.connection = connection
        // Shallow-clone options so we don't mutate the shared object when
        // processing privateKey below.  Nested objects (e.g. `extra`) are still
        // shared references — avoid mutating them directly.
        this.options = Object.assign(
            {},
            connection.options as SnowflakeConnectionOptions,
        )

        if (this.options.privateKey || this.options.privateKeyPath) {
            // snowflake-sdk uses SNOWFLAKE_JWT for key-pair authentication.
            // Preserve an explicitly configured authenticator.
            this.options.authenticator =
                this.options.authenticator ?? "SNOWFLAKE_JWT"
        }

        if (this.options.privateKey) {
            // Support either PEM or base64-encoded PEM input.
            const raw = this.options.privateKey.trim()
            const looksLikePem = raw.includes("BEGIN") && raw.includes("KEY")

            let pemKey = raw
            if (!looksLikePem) {
                // Best-effort base64 decode. Buffer.from(str, "base64") never
                // throws — it silently ignores non-base64 chars — so we just
                // check the result for a PEM header.
                const decoded = Buffer.from(raw, "base64").toString("utf8")
                if (decoded.includes("BEGIN") && decoded.includes("KEY")) {
                    pemKey = decoded
                }
            }

            const passphrase =
                this.options.privateKeyPass ?? this.options.password

            try {
                this.options.privateKey = crypto
                    .createPrivateKey({
                        key: pemKey,
                        format: "pem",
                        ...(passphrase ? { passphrase } : {}),
                    })
                    .export({
                        format: "pem",
                        type: "pkcs8",
                    }) as string
            } catch (err) {
                throw new TypeORMError(
                    `Failed to parse Snowflake private key. Ensure the key is ` +
                        `a valid PEM or base64-encoded PEM, and that the passphrase ` +
                        `(privateKeyPass / password) is correct. ` +
                        `Original error: ${
                            err instanceof Error ? err.message : err
                        }`,
                )
            }
        }

        this.database = this.options.database
        this.schema = this.options.schema
    }

    // -------------------------------------------------------------------------
    // Public Implemented Methods
    // -------------------------------------------------------------------------

    /**
     * Performs connection to the database.
     * Creates a connection pool using snowflake-sdk's createPool with best-practice defaults.
     *
     * @returns Resolves when the pool is created and connectivity is verified.
     */
    async connect(): Promise<void> {
        // snowflake-sdk configuration is global for the process.
        // Always call configure() so that a second DataSource with a
        // different sdkLogLevel still takes effect.
        configure({
            logLevel: this.options.sdkLogLevel ?? "ERROR",
        })

        // Build SDK connection options by extracting all SDK-relevant
        // properties from the TypeORM options. We explicitly list them
        // to avoid leaking TypeORM-only keys (type, pool, extra, etc.)
        // into the SDK.
        const connectionOptions: SfConnectionOptions = {
            account: this.options.account,
            username: this.options.username,
            password: this.options.password,
            database: this.options.database,
            schema: this.options.schema,
            warehouse: this.options.warehouse,
            role: this.options.role,
            application: this.options.application,
            authenticator: this.options.authenticator,
            token: this.options.token,
            privateKey: this.options.privateKey,
            privateKeyPath: this.options.privateKeyPath,
            privateKeyPass: this.options.privateKeyPass,
            region: this.options.region,
            timeout: this.options.timeout,
            jsTreatIntegerAsBigInt: this.options.jsTreatIntegerAsBigInt,
            // Off by default — the heartbeat timer prevents Lambda process freeze.
            // Long-running servers should set clientSessionKeepAlive: true.
            clientSessionKeepAlive:
                this.options.clientSessionKeepAlive ?? false,
            clientSessionKeepAliveHeartbeatFrequency:
                this.options.clientSessionKeepAliveHeartbeatFrequency ?? 900,
            // Network / proxy
            accessUrl: this.options.accessUrl,
            host: this.options.host,
            proxyHost: this.options.proxyHost,
            proxyPort: this.options.proxyPort,
            proxyProtocol: this.options.proxyProtocol,
            proxyUser: this.options.proxyUser,
            proxyPassword: this.options.proxyPassword,
            noProxy: this.options.noProxy,
            // Query / result tuning
            queryTag: this.options.queryTag,
            fetchAsString: this.options.fetchAsString,
            arrayBindingThreshold: this.options.arrayBindingThreshold,
            resultPrefetch: this.options.resultPrefetch,
            retryTimeout: this.options.retryTimeout,
            // Authentication extras
            clientRequestMFAToken: this.options.clientRequestMFAToken,
            clientStoreTemporaryCredential:
                this.options.clientStoreTemporaryCredential,
            credentialCacheDir: this.options.credentialCacheDir,
            passcode: this.options.passcode,
            passcodeInPassword: this.options.passcodeInPassword,
            browserActionTimeout: this.options.browserActionTimeout,
            disableConsoleLogin: this.options.disableConsoleLogin,
            validateDefaultParameters: this.options.validateDefaultParameters,
            // OAuth options
            oauthClientId: this.options.oauthClientId,
            oauthClientSecret: this.options.oauthClientSecret,
            oauthAuthorizationUrl: this.options.oauthAuthorizationUrl,
            oauthTokenRequestUrl: this.options.oauthTokenRequestUrl,
            oauthScope: this.options.oauthScope,
            oauthRedirectUri: this.options.oauthRedirectUri,
            oauthChallengeMethod: this.options.oauthChallengeMethod,
            oauthEnableSingleUseRefreshTokens:
                this.options.oauthEnableSingleUseRefreshTokens,
            // Workload Identity options
            workloadIdentityProvider: this.options.workloadIdentityProvider,
            workloadIdentityImpersonationPath:
                this.options.workloadIdentityImpersonationPath,
            workloadIdentityAzureEntraIdResource:
                this.options.workloadIdentityAzureEntraIdResource,
            workloadIdentityAzureClientId:
                this.options.workloadIdentityAzureClientId,
            // CRL validation options
            certRevocationCheckMode: this.options.certRevocationCheckMode,
            crlAllowCertificatesWithoutCrlURL:
                this.options.crlAllowCertificatesWithoutCrlURL,
            crlInMemoryCache: this.options.crlInMemoryCache,
            crlOnDiskCache: this.options.crlOnDiskCache,
        }

        // Remove undefined values so snowflake-sdk doesn't choke on them
        const opts = connectionOptions as Record<string, any>
        for (const key of Object.keys(opts)) {
            if (opts[key] === undefined) {
                delete opts[key]
            }
        }

        // generic-pool defaults (max:1, min:0, testOnBorrow:false,
        // evictionRunIntervalMillis:0, idleTimeoutMillis:30000) are already
        // Lambda-friendly, so we only override acquireTimeoutMillis (default
        // is null = wait forever, which would cause Lambda to hang).
        // All other user-specified pool options are passed through as-is.
        const userPoolOpts = this.options.pool ?? {}
        const poolOptions = {
            ...userPoolOpts,
            acquireTimeoutMillis: userPoolOpts.acquireTimeoutMillis ?? 30000,
        }

        const pool = createPool(connectionOptions, poolOptions)

        // Verify pool is functional by acquiring and releasing a connection.
        // Only assign to this.pool after verification succeeds to avoid leaking
        // an undrainable pool if the test connection fails.
        try {
            const testConn = await pool.acquire()
            await pool.release(testConn)
        } catch (err) {
            await pool.drain().catch(() => {})
            await pool.clear().catch(() => {})
            throw err
        }
        this.pool = pool
    }

    /**
     * Closes connection with database.
     * Drains the pool and destroys all connections.
     *
     * @returns Resolves when all connections have been released and the pool is drained.
     */
    async disconnect(): Promise<void> {
        if (!this.pool) {
            return
        }

        // Release all tracked query runners in parallel.
        // Snapshot the array first — release() splices from the live array.
        // Use allSettled so one failed release doesn't skip the rest.
        const queryRunners = [...this.connectedQueryRunners]
        await Promise.allSettled(
            queryRunners.map((qr) =>
                qr.isReleased ? Promise.resolve() : qr.release(),
            ),
        )

        await this.pool.drain()
        await this.pool.clear()
        this.pool = undefined as any
    }

    /**
     * Creates a query runner used to execute database queries.
     *
     * @param mode - Replication mode. Snowflake does not support replication,
     *               so this is effectively ignored. Defaults to `"master"`.
     * @returns A new {@link SnowflakeQueryRunner} instance.
     */
    createQueryRunner(mode?: ReplicationMode): QueryRunner {
        return new SnowflakeQueryRunner(this, mode || "master")
    }

    /**
     * Makes any action after connection (e.g. run session-level setup for Snowflake).
     *
     * @returns Resolves immediately (no-op for Snowflake).
     */
    async afterConnect(): Promise<void> {}

    /**
     * Creates a schema builder used to build and sync a schema.
     *
     * @returns A new {@link RdbmsSchemaBuilder} for this connection.
     */
    createSchemaBuilder() {
        return new RdbmsSchemaBuilder(this.connection)
    }

    /**
     * Replaces named parameter placeholders (`:paramName`) in the given SQL with
     * positional bind markers (`:1`, `:2`, …) and collects the corresponding
     * values into an array suitable for the Snowflake SDK.
     *
     * @param sql - The SQL string containing named parameter placeholders.
     * @param parameters - Map of parameter names to values from the QueryBuilder.
     * @param nativeParameters - Additional native parameters that are appended as-is.
     * @returns A tuple of `[processedSql, parameterValues]`.
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

        const parameterIndexMap = new Map<string, number | string>()
        sql = sql.replace(
            /:(\.\.\.)?([A-Za-z0-9_.]+)/g,
            (full, isArray: string, key: string): string => {
                if (!parameters.hasOwnProperty(key)) {
                    return full
                }

                const cached = parameterIndexMap.get(key)
                if (cached !== undefined) {
                    // For spread params the cached value is the expanded string;
                    // for scalar params it's the 1-based parameter index.
                    return typeof cached === "string"
                        ? cached
                        : this.parametersPrefix + cached
                }

                const value: any = parameters[key]

                if (isArray) {
                    const expanded = value
                        .map((v: any) => {
                            escapedParameters.push(v)
                            return this.createParameter(
                                key,
                                escapedParameters.length - 1,
                            )
                        })
                        .join(", ")
                    parameterIndexMap.set(key, expanded)
                    return expanded
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
     * Creates a positional bind-parameter placeholder string.
     *
     * @param parameterName - The logical parameter name (unused — Snowflake uses positional binds).
     * @param index - Zero-based parameter index; converted to 1-based `:N` string.
     * @returns A string like `":1"`, `":2"`, etc.
     */
    createParameter(parameterName: string, index: number): string {
        return this.parametersPrefix + (index + 1)
    }

    /**
     * Escapes a column name for use as a quoted identifier.
     * Doubles any embedded double-quote characters per SQL standard.
     *
     * @param columnName - The raw column (or identifier) name to escape.
     * @returns The escaped identifier wrapped in double quotes.
     */
    escape(columnName: string): string {
        return '"' + columnName.replace(/"/g, '""') + '"'
    }

    /**
     * Build full table name with schema name and table name.
     * E.g. `myDB.mySchema.myTable`
     *
     * @param tableName - The table name.
     * @param schema - Optional schema name to prepend.
     * @param database - Optional database name to prepend.
     * @returns A dot-separated qualified table name string.
     */
    buildTableName(
        tableName: string,
        schema?: string,
        database?: string,
    ): string {
        const tablePath = [tableName]

        if (schema) {
            tablePath.unshift(schema)
        }

        if (database) {
            tablePath.unshift(database)
        }

        return tablePath.join(".")
    }

    /**
     * Parse a target table name or other types and return a normalized table definition.
     *
     * @param target - A string (dot-separated), {@link Table}, {@link View},
     *                 {@link TableForeignKey}, or {@link EntityMetadata} representing the table.
     * @returns An object with optional `database`, optional `schema`, and required `tableName`.
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

        if (parts.length === 3) {
            return {
                database: parts[0],
                schema: parts[1],
                tableName: parts[2],
            }
        }

        return {
            database: driverDatabase,
            schema: (parts.length > 1 ? parts[0] : undefined) || driverSchema,
            tableName: parts.length > 1 ? parts[1] : parts[0],
        }
    }

    /**
     * Prepares given value to a value to be persisted, based on its column type and metadata.
     *
     * @param value - The JavaScript value to transform for storage.
     * @param columnMetadata - The column metadata describing the target column's type.
     * @returns The transformed value ready for the Snowflake SDK bind parameter.
     */
    preparePersistentValue(value: any, columnMetadata: ColumnMetadata): any {
        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformTo(
                columnMetadata.transformer,
                value,
            )

        if (value === null || value === undefined) return value

        if (columnMetadata.type === Boolean) {
            return !!value
        } else if (columnMetadata.type === "date") {
            return DateUtils.mixedDateToDateString(value)
        } else if (columnMetadata.type === "time") {
            return DateUtils.mixedDateToTimeString(value)
        } else if (this.dateTypes.has(columnMetadata.type)) {
            return DateUtils.mixedDateToDate(value)
        } else if (this.jsonStringifyTypes.has(columnMetadata.type)) {
            return JSON.stringify(value)
        } else if (columnMetadata.type === "simple-array") {
            return DateUtils.simpleArrayToString(value)
        } else if (columnMetadata.type === "simple-json") {
            return DateUtils.simpleJsonToString(value)
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
     * Prepares given value to a value to be hydrated, based on its column type or metadata.
     *
     * @param value - The raw database value to transform.
     * @param columnMetadata - The column metadata describing the source column's type.
     * @returns The hydrated JavaScript value suitable for entity assignment.
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
            value = !!value
        } else if (this.dateTypes.has(columnMetadata.type)) {
            value = DateUtils.normalizeHydratedDate(value)
        } else if (columnMetadata.type === "date") {
            value = DateUtils.mixedDateToDateString(value)
        } else if (columnMetadata.type === "time") {
            value = DateUtils.mixedTimeToString(value)
        } else if (columnMetadata.type === "simple-array") {
            value = DateUtils.stringToSimpleArray(value)
        } else if (columnMetadata.type === "simple-json") {
            value = DateUtils.stringToSimpleJson(value)
        } else if (
            columnMetadata.type === "enum" ||
            columnMetadata.type === "simple-enum"
        ) {
            if (columnMetadata.isArray) {
                // Snowflake does not support native ENUM arrays, but if stored
                // as comma-separated string we parse it here.
                if (typeof value === "string") {
                    if (value === "") return []
                    value = value.split(",")
                }
                // convert to number if that exists in possible enum options
                value = (Array.isArray(value) ? value : [value]).map(
                    (val: string) => {
                        return columnMetadata.enum &&
                            !isNaN(+val) &&
                            columnMetadata.enum.indexOf(parseInt(val)) >= 0
                            ? parseInt(val)
                            : val
                    },
                )
            } else {
                // convert to number if that exists in possible enum options
                value =
                    columnMetadata.enum &&
                    !isNaN(+value) &&
                    columnMetadata.enum.indexOf(parseInt(value)) >= 0
                        ? parseInt(value)
                        : value
            }
        } else if (columnMetadata.type === Number) {
            // convert to number if number — use parseFloat to preserve fractional values
            value = !isNaN(+value) ? parseFloat(value) : value
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
     * Maps TypeORM / JavaScript types and aliases to canonical Snowflake SQL type names.
     *
     * @param column - An object describing the column type and optional length/precision/scale.
     * @returns The canonical Snowflake SQL type name (e.g. `"varchar"`, `"timestamp_ntz"`).
     */
    normalizeType(column: {
        type?: ColumnType
        length?: number | string
        precision?: number | null
        scale?: number
        isArray?: boolean
    }): string {
        if (column.type === Number || column.type === "int") {
            return "integer"
        } else if (
            column.type === String ||
            column.type === "varchar" ||
            column.type === "character varying" ||
            column.type === "text" ||
            column.type === "string"
        ) {
            return "varchar"
        } else if (
            column.type === Date ||
            column.type === "datetime" ||
            column.type === "timestamp" ||
            column.type === "timestamp without time zone"
        ) {
            return "timestamp_ntz"
        } else if (
            column.type === "timestamptz" ||
            column.type === "timestamp with time zone"
        ) {
            return "timestamp_tz"
        } else if (column.type === "time") {
            return "time"
        } else if (column.type === Boolean || column.type === "bool") {
            return "boolean"
        } else if (column.type === "simple-array") {
            return "varchar"
        } else if (column.type === "simple-json") {
            return "varchar"
        } else if (column.type === "simple-enum") {
            return "varchar"
        } else if (column.type === "decimal" || column.type === "numeric") {
            // Snowflake canonical form for DECIMAL/NUMERIC/NUMBER is "number"
            // (INFORMATION_SCHEMA returns "FIXED" which loadTables maps to "number").
            return "number"
        } else if (
            column.type === "float" ||
            column.type === "double" ||
            column.type === "double precision" ||
            column.type === "real"
        ) {
            // Snowflake canonical form for FLOAT/DOUBLE/REAL is "float"
            // (INFORMATION_SCHEMA returns "FLOAT" which loadTables keeps as "float").
            return "float"
        } else if (column.type === "char") {
            return "character"
        } else {
            return (column.type as string) || ""
        }
    }

    /**
     * Normalizes "default" value of the column into its SQL-string representation
     * suitable for a `DEFAULT` clause in DDL.
     *
     * @param columnMetadata - The column metadata containing the default value to normalize.
     * @returns The SQL default expression string, or `undefined` if no default is defined.
     */
    normalizeDefault(columnMetadata: ColumnMetadata): string | undefined {
        const defaultValue = columnMetadata.default

        if (defaultValue === null || defaultValue === undefined) {
            return undefined
        }

        if (columnMetadata.isArray && Array.isArray(defaultValue)) {
            return `ARRAY_CONSTRUCT(${defaultValue
                .map((val: any) =>
                    val === undefined || val === null
                        ? "NULL"
                        : typeof val === "string"
                        ? `'${String(val).replace(/'/g, "''")}'`
                        : `${val}`,
                )
                .join(",")})`
        }

        if (
            columnMetadata.type === "enum" ||
            columnMetadata.type === "simple-enum" ||
            typeof defaultValue === "string"
        ) {
            // Check if the string is a datetime function name (e.g. "CURRENT_TIMESTAMP")
            // before quoting it as a string literal.
            if (typeof defaultValue === "string") {
                const normalized = this.normalizeDatetimeFunction(defaultValue)
                if (normalized !== defaultValue) {
                    return normalized
                }
            }
            return `'${String(defaultValue).replace(/'/g, "''")}'`
        }

        if (typeof defaultValue === "number") {
            return `${defaultValue}`
        }

        if (typeof defaultValue === "boolean") {
            return defaultValue ? "true" : "false"
        }

        if (typeof defaultValue === "function") {
            const value = defaultValue()

            return this.normalizeDatetimeFunction(value)
        }

        if (typeof defaultValue === "object") {
            const jsonStr = JSON.stringify(defaultValue).replace(/'/g, "''")
            // VARIANT/object columns need PARSE_JSON() so Snowflake stores
            // the value as a native semi-structured type, not a plain string.
            if (
                columnMetadata.type === "variant" ||
                columnMetadata.type === "object"
            ) {
                return `PARSE_JSON('${jsonStr}')`
            }
            return `'${jsonStr}'`
        }

        return `${defaultValue}`
    }

    /**
     * Normalizes "isUnique" value of the column.
     * Returns `true` if the column is the sole member of any unique constraint on its entity.
     *
     * @param column - The column metadata to inspect.
     * @returns `true` if the column is uniquely constrained by itself.
     */
    normalizeIsUnique(column: ColumnMetadata): boolean {
        return column.entityMetadata.uniques.some(
            (uq) => uq.columns.length === 1 && uq.columns[0] === column,
        )
    }

    /**
     * Returns default column lengths, which is required on column creation.
     *
     * @param column - The column metadata to inspect.
     * @returns The column's explicit length as a string, or an empty string if none is defined.
     */
    getColumnLength(column: ColumnMetadata): string {
        return column.length ? column.length.toString() : ""
    }

    /**
     * Creates column type definition including length, precision and scale.
     * Handles Snowflake-specific canonical forms for timestamp and spatial types.
     *
     * @param column - The {@link TableColumn} whose full SQL type string to build.
     * @returns The complete SQL type string (e.g. `"varchar(255)"`, `"numeric(18,2)"`).
     */
    createFullType(column: TableColumn): string {
        // Snowflake ARRAY type — check early before appending precision/length.
        if (column.isArray) return "ARRAY"

        let type = column.type

        // Normalize long-form Postgres-style timestamp aliases to Snowflake canonical forms
        // BEFORE appending precision/length, so the canonical name is used from the start.
        if (column.type === "timestamp without time zone") {
            type = "timestamp_ntz"
        } else if (column.type === "timestamp with time zone") {
            type = "timestamp_tz"
        } else if (this.spatialTypeSet.has(column.type as ColumnType)) {
            // Snowflake spatial types (GEOMETRY, GEOGRAPHY) don't accept
            // sub-type or SRID parameters — just emit the bare type name.
            return column.type as string
        }

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

        return type
    }

    /**
     * Creates generated map of values generated or returned by database after INSERT query.
     *
     * @param metadata - The entity metadata for the inserted entity.
     * @param insertResult - The raw result object returned by the Snowflake SDK after INSERT.
     * @returns A partial entity value map, or `undefined` if `insertResult` is falsy.
     *
     * @todo Slow. Optimize `Object.keys()`, `OrmUtils.mergeDeep` and `column.createValueMap` parts.
     */
    createGeneratedMap(metadata: EntityMetadata, insertResult: ObjectLiteral) {
        if (!insertResult) return undefined

        return Object.keys(insertResult).reduce((map, key) => {
            // Snowflake returns column names in UPPERCASE by default.
            // Try exact match first, then case-insensitive fallback.
            let column = metadata.findColumnWithDatabaseName(key)
            if (!column) {
                const lowerKey = key.toLowerCase()
                column = metadata.columns.find(
                    (col) => col.databaseName.toLowerCase() === lowerKey,
                )
            }
            if (column) {
                OrmUtils.mergeDeep(
                    map,
                    column.createValueMap(insertResult[key]),
                )
            }
            return map
        }, {} as ObjectLiteral)
    }

    /**
     * Obtains a new database connection to a master server.
     * Acquires a connection from the pool.
     *
     * @returns A pooled Snowflake SDK connection.
     * @throws {TypeORMError} If the connection pool has not been created.
     */
    async obtainMasterConnection(): Promise<Connection> {
        if (!this.pool) {
            throw new TypeORMError(
                "Connection pool is not created. Call connect() first.",
            )
        }
        return this.pool.acquire()
    }

    /**
     * Obtains a new database connection to a slave server.
     * Snowflake does not support replication, so this delegates to
     * {@link obtainMasterConnection}.
     *
     * @returns A pooled Snowflake SDK connection (same as master).
     */
    async obtainSlaveConnection(): Promise<Connection> {
        return this.obtainMasterConnection()
    }

    /**
     * Releases a connection back to the pool.
     *
     * @param connection - The Snowflake SDK connection to release.
     * @returns Resolves when the connection has been returned to the pool.
     */
    async releaseConnection(connection: Connection): Promise<void> {
        if (this.pool) {
            await this.pool.release(connection)
        }
    }

    /**
     * Differentiate columns of this table and columns from the given column metadata
     * and returns only the columns whose schema properties have changed.
     *
     * @param tableColumns - The current database table columns.
     * @param columnMetadatas - The entity column metadata to compare against.
     * @returns An array of {@link ColumnMetadata} entries whose properties differ
     *          from their corresponding {@link TableColumn}.
     */
    findChangedColumns(
        tableColumns: TableColumn[],
        columnMetadatas: ColumnMetadata[],
    ): ColumnMetadata[] {
        const tableColumnMap = new Map(
            tableColumns.map((c) => [c.name, c] as const),
        )
        return columnMetadatas.filter((columnMetadata) => {
            const tableColumn = tableColumnMap.get(columnMetadata.databaseName)
            if (!tableColumn) return false // we don't need new columns, we only need exist and changed

            const isColumnChanged =
                tableColumn.type !== this.normalizeType(columnMetadata) ||
                tableColumn.length !== this.getColumnLength(columnMetadata) ||
                tableColumn.isArray !== columnMetadata.isArray ||
                tableColumn.precision !== columnMetadata.precision ||
                (columnMetadata.scale !== undefined &&
                    tableColumn.scale !== columnMetadata.scale) ||
                tableColumn.comment !==
                    this.escapeComment(columnMetadata.comment) ||
                (!tableColumn.isGenerated &&
                    !this.defaultEqual(columnMetadata, tableColumn)) ||
                tableColumn.isPrimary !== columnMetadata.isPrimary ||
                tableColumn.isNullable !== columnMetadata.isNullable ||
                tableColumn.isUnique !==
                    this.normalizeIsUnique(columnMetadata) ||
                tableColumn.isGenerated !== columnMetadata.isGenerated ||
                tableColumn.generatedType !== columnMetadata.generatedType ||
                (tableColumn.asExpression || "").trim() !==
                    (columnMetadata.asExpression || "").trim()

            return isColumnChanged
        })
    }

    /**
     * Returns true if driver supports RETURNING / OUTPUT statement.
     * Snowflake does not support this.
     *
     * @returns Always `false`.
     */
    isReturningSqlSupported(): boolean {
        return false
    }

    /**
     * Returns true if driver supports uuid values generation on its own.
     *
     * @returns Always `true` — Snowflake has built-in UUID generation.
     */
    isUUIDGenerationSupported(): boolean {
        return true
    }

    /**
     * Returns true if driver supports fulltext indices.
     * Snowflake does not support fulltext index column types.
     *
     * @returns Always `false`.
     */
    isFullTextColumnTypeSupported(): boolean {
        return false
    }

    /**
     * Lowercases the non-string-literal portions of a default value expression.
     * Parts inside single quotes are preserved as-is (case-sensitive string literals),
     * while everything else is lowercased to allow case-insensitive comparison.
     *
     * @param value - The default value expression, or `undefined`.
     * @returns The normalized string, or `undefined` if the input was falsy.
     */
    private lowerDefaultValueIfNecessary(value: string | undefined) {
        if (!value) {
            return value
        }
        // Split on string-literal boundaries while correctly handling
        // escaped single quotes ('') inside literals.  The regex matches
        // either a quoted string (group 1: '...') or a non-quoted segment
        // (group 2).  Only non-quoted segments are lowercased.
        return value.replace(
            /('(?:[^']|'')*')|([^']+)/g,
            (
                _match,
                quoted: string | undefined,
                unquoted: string | undefined,
            ) => (quoted ? quoted : unquoted!.toLowerCase()),
        )
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * If parameter is a datetime function, e.g. `"CURRENT_TIMESTAMP"`, normalizes it
     * to the Snowflake equivalent function call form (with parentheses).
     * Otherwise returns original input.
     *
     * @param value - The default value string to inspect.
     * @returns The normalized datetime function call (e.g. `"CURRENT_TIMESTAMP()"`)
     *          or the original value if it is not a datetime function.
     */
    protected normalizeDatetimeFunction(value: string) {
        const upperCaseValue = value.toUpperCase().trim()

        // Check for CURRENT_TIMESTAMP/LOCALTIMESTAMP/NOW() first (most common)
        if (
            upperCaseValue.indexOf("CURRENT_TIMESTAMP") !== -1 ||
            upperCaseValue.indexOf("LOCALTIMESTAMP") !== -1 ||
            upperCaseValue === "NOW()"
        ) {
            return "CURRENT_TIMESTAMP()"
        }

        if (upperCaseValue.indexOf("CURRENT_DATE") !== -1) {
            return "CURRENT_DATE()"
        }

        // Use word boundary check to avoid matching CURRENT_TIMESTAMP.
        // CURRENT_TIME must NOT be followed by "S" (as in TIMESTAMP).
        if (
            /\bCURRENT_TIME\b(?!S)/i.test(upperCaseValue) ||
            /\bLOCALTIME\b(?!S)/i.test(upperCaseValue)
        ) {
            return "CURRENT_TIME()"
        }

        return value
    }

    /**
     * Escapes a given comment by stripping null bytes that are not allowed
     * in Snowflake DDL comment clauses.
     *
     * @param comment - The raw comment string, or `undefined`.
     * @returns The sanitized comment string, or `undefined` if the input was falsy.
     */
    protected escapeComment(comment?: string) {
        if (comment === undefined || comment === null || comment === "")
            return undefined

        comment = comment.replace(/\u0000/g, "") // Null bytes aren't allowed in comments

        return comment
    }

    /**
     * Compares the default value from column metadata against the one stored in the database.
     * Handles VARIANT/JSON defaults via deep comparison, and falls back to
     * case-insensitive string comparison via {@link lowerDefaultValueIfNecessary}.
     *
     * @param columnMetadata - The ORM column metadata with the expected default.
     * @param tableColumn - The current database column definition with the actual default.
     * @returns `true` if the defaults are considered equal.
     */
    private defaultEqual(
        columnMetadata: ColumnMetadata,
        tableColumn: TableColumn,
    ): boolean {
        if (
            ["variant", "object"].includes(columnMetadata.type as string) &&
            !["function", "undefined"].includes(typeof columnMetadata.default)
        ) {
            try {
                const raw =
                    typeof tableColumn.default === "string"
                        ? tableColumn.default
                        : undefined
                // Snowflake may store VARIANT defaults as a quoted JSON string
                // e.g. '{"key":"value"}' — strip outer quotes and parse.
                // If the format is unexpected, fall through to string comparison.
                if (
                    raw !== undefined &&
                    raw.startsWith("'") &&
                    raw.endsWith("'")
                ) {
                    const tableColumnDefault = JSON.parse(
                        raw.substring(1, raw.length - 1),
                    )
                    return OrmUtils.deepCompare(
                        columnMetadata.default,
                        tableColumnDefault,
                    )
                }
            } catch {
                // JSON parse failed — fall through to string comparison below
            }
        }

        const columnDefault = this.lowerDefaultValueIfNecessary(
            this.normalizeDefault(columnMetadata),
        )
        const tableDefault = this.lowerDefaultValueIfNecessary(
            typeof tableColumn.default === "string"
                ? tableColumn.default
                : undefined,
        )
        return columnDefault === tableDefault
    }
}
