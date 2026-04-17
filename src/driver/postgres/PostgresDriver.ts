import { ObjectLiteral } from "../../common/ObjectLiteral"
import { DataSource } from "../../data-source/DataSource"
import { ConnectionIsNotSetError } from "../../error/ConnectionIsNotSetError"
import { ColumnMetadata } from "../../metadata/ColumnMetadata"
import { EntityMetadata } from "../../metadata/EntityMetadata"
import { PlatformTools } from "../../platform/PlatformTools"
import { QueryRunner } from "../../query-runner/QueryRunner"
import { RdbmsSchemaBuilder } from "../../schema-builder/RdbmsSchemaBuilder"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { ApplyValueTransformers } from "../../util/ApplyValueTransformers"
import { DateUtils } from "../../util/DateUtils"
import { OrmUtils } from "../../util/OrmUtils"
import { Driver } from "../Driver"
import { ColumnType } from "../types/ColumnTypes"
import { CteCapabilities } from "../types/CteCapabilities"
import { DataTypeDefaults } from "../types/DataTypeDefaults"
import { MappedColumnTypes } from "../types/MappedColumnTypes"
import { ReplicationMode } from "../types/ReplicationMode"
import { VersionUtils } from "../../util/VersionUtils"
import { PostgresConnectionCredentialsOptions } from "./PostgresConnectionCredentialsOptions"
import { PostgresConnectionOptions } from "./PostgresConnectionOptions"
import { PostgresQueryRunner } from "./PostgresQueryRunner"
import { DriverUtils } from "../DriverUtils"
import { TypeORMError } from "../../error"
import { Table } from "../../schema-builder/table/Table"
import { View } from "../../schema-builder/view/View"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { InstanceChecker } from "../../util/InstanceChecker"
import { UpsertType } from "../types/UpsertType"
import { Pool, PoolClient, PoolConfig } from "pg"
import { sleep } from "./sleep"
import { getRetryDeadline, withRetryDeadline } from "./retryContext"

/**
 * Walk an error object and every nested cause / aggregate / wrapper it can
 * find, collecting every `code` and `message` seen along the way. This makes
 * {@link classifyError} robust to:
 *
 *   - Node 16+ `Error.cause` chains (pg sometimes wraps socket errors)
 *   - `AggregateError.errors` (DNS resolution returning multiple addresses,
 *     all of which failed with ECONNREFUSED)
 *   - TypeORM's own `QueryFailedError.driverError` wrapping
 *   - Legacy `originalError` / `innerError` wrappers some drivers use
 *   - Errors where the `code` got stringified into `message` and the
 *     structured `code` field was lost
 *
 * Depth and visited-set guards prevent pathological cycles or huge trees
 * from blocking the event loop.
 */
function collectErrorFingerprints(
    err: any,
): { codes: Set<string>; messages: string[] } {
    const codes = new Set<string>()
    const messages: string[] = []
    const seen = new Set<any>()

    const visit = (e: any, depth: number): void => {
        if (e === null || e === undefined) return
        if (depth > 8) return
        if (typeof e !== "object" && typeof e !== "string") return

        if (typeof e === "string") {
            messages.push(e)
            return
        }

        if (seen.has(e)) return
        seen.add(e)

        if (typeof e.code === "string") codes.add(e.code)
        if (typeof e.errno === "string") codes.add(e.errno)
        if (typeof e.sqlState === "string") codes.add(e.sqlState)
        if (typeof e.message === "string") messages.push(e.message)

        // Error.cause (Node >= 16.9)
        if ("cause" in e) visit((e as any).cause, depth + 1)

        // AggregateError.errors
        if (Array.isArray((e as any).errors)) {
            for (const inner of (e as any).errors) visit(inner, depth + 1)
        }

        // TypeORM / driver wrapper conventions
        visit((e as any).driverError, depth + 1)
        visit((e as any).originalError, depth + 1)
        visit((e as any).innerError, depth + 1)
        visit((e as any).previous, depth + 1) // some libraries
    }

    visit(err, 0)
    return { codes, messages }
}

/**
 * Network / transient error codes that should trigger tier2 retry regardless
 * of where in the error graph they appear.
 */
const TIER2_NETWORK_CODES = new Set([
    "ECONNREFUSED",
    "ECONNRESET",
    "ETIMEDOUT",
    "EHOSTUNREACH",
    "ENETUNREACH",
    "ENETDOWN",
    "EPIPE",
    "EAI_AGAIN", // transient DNS failure
])

/**
 * Postgres SQLSTATE codes that indicate a transient server-side condition:
 *   40001 — serialization_failure
 *   57014 — query_canceled
 *   57P03 — cannot_connect_now
 *   58P01 — undefined_file (seen during failover)
 */
const TIER2_PG_SQLSTATES = new Set(["40001", "57014", "57P03", "58P01"])

/**
 * Classify a Postgres error into a retry tier.
 *
 *   - tier1: "Connection terminated unexpectedly"
 *            → retry indefinitely with 500ms sleep
 *   - tier2: ECONNREFUSED, ECONNRESET, ETIMEDOUT, PG codes (40001, 58P01,
 *            57014, 57P03), recovery mode, replica conflict, etc.
 *            → retry up to maxRetryDuration with 5000ms sleep
 *   - null:  non-retryable → throw immediately
 *
 * Recognition is performed across the entire error graph (cause chains,
 * AggregateError members, driver wrappers) and matches both structured
 * `code` fields AND substring occurrences inside messages — so errors
 * where the code has been stringified into a message are still caught.
 */
export function classifyError(err: any): "tier1" | "tier2" | null {
    if (err === null || err === undefined) return null

    const { codes, messages } = collectErrorFingerprints(err)
    const anyMessage = messages.join("\n")
    const lowerMessage = anyMessage.toLowerCase()

    // tier1 — a single broken connection, pool itself is likely fine.
    if (anyMessage.includes("Connection terminated unexpectedly")) {
        return "tier1"
    }

    // tier2 — network-level or server-side transient failures.
    for (const c of TIER2_NETWORK_CODES) {
        if (codes.has(c)) return "tier2"
    }
    for (const c of TIER2_PG_SQLSTATES) {
        if (codes.has(c)) return "tier2"
    }

    // Some wrappers stringify the code into the message and drop the
    // structured field. Fall back to substring search — use word-boundary-ish
    // delimiters to avoid accidentally matching "eCONNREFUSEDByPolicy" etc.
    const MESSAGE_CODE_PATTERNS = [
        /\bECONNREFUSED\b/,
        /\bECONNRESET\b/,
        /\bETIMEDOUT\b/,
        /\bEHOSTUNREACH\b/,
        /\bENETUNREACH\b/,
        /\bENETDOWN\b/,
        /\bEPIPE\b/,
        /\bEAI_AGAIN\b/,
    ]
    for (const re of MESSAGE_CODE_PATTERNS) {
        if (re.test(anyMessage)) return "tier2"
    }

    if (
        anyMessage.includes(
            "Connection terminated due to connection timeout",
        ) ||
        anyMessage.includes("timeout exceeded when trying to connect") ||
        anyMessage.includes("Client has encountered a connection error") ||
        anyMessage.includes("the database system is in recovery mode") ||
        anyMessage.includes("the database system is starting up") ||
        anyMessage.includes("the database system is shutting down") ||
        lowerMessage.includes(
            "query might have conflicted with replica reconnect",
        ) ||
        lowerMessage.includes(
            "canceling statement due to conflict with recovery",
        )
    ) {
        return "tier2"
    }

    return null
}

/**
 * Organizes communication with PostgreSQL DBMS.
 */
export class PostgresDriver implements Driver {
    // -------------------------------------------------------------------------
    // Public Properties
    // -------------------------------------------------------------------------

    /**
     * Connection used by driver.
     */
    connection: DataSource

    /**
     * Postgres underlying library.
     */
    postgres: any

    /**
     * Pool for master database.
     */
    master?: Pool

    /**
     * Pool for slave databases.
     * Used in replication.
     */
    slaves: any[] = []

    /**
     * We store all created query runners because we need to release them.
     */
    connectedQueryRunners: QueryRunner[] = []

    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    /**
     * Connection options.
     */
    options: PostgresConnectionOptions

    /**
     * Version of Postgres. Requires a SQL query to the DB, so it is not always set
     */
    version?: string

    /**
     * Database name used to perform all write queries.
     */
    database?: string

    /**
     * Schema name used to perform all write queries.
     */
    schema?: string

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
     * @see https://www.postgresql.org/docs/current/datatype.html
     */
    supportedDataTypes: ColumnType[] = [
        "int",
        "int2",
        "int4",
        "int8",
        "smallint",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "real",
        "float",
        "float4",
        "float8",
        "double precision",
        "money",
        "character varying",
        "varchar",
        "character",
        "char",
        "text",
        "citext",
        "hstore",
        "bytea",
        "bit",
        "varbit",
        "bit varying",
        "timetz",
        "timestamptz",
        "timestamp",
        "timestamp without time zone",
        "timestamp with time zone",
        "date",
        "time",
        "time without time zone",
        "time with time zone",
        "interval",
        "bool",
        "boolean",
        "enum",
        "point",
        "line",
        "lseg",
        "box",
        "path",
        "polygon",
        "circle",
        "cidr",
        "inet",
        "macaddr",
        "macaddr8",
        "tsvector",
        "tsquery",
        "uuid",
        "xml",
        "json",
        "jsonb",
        "jsonpath",
        "int4range",
        "int8range",
        "numrange",
        "tsrange",
        "tstzrange",
        "daterange",
        "int4multirange",
        "int8multirange",
        "nummultirange",
        "tsmultirange",
        "tstzmultirange",
        "datemultirange",
        "geometry",
        "geography",
        "cube",
        "ltree",
        "vector",
        "halfvec",
    ]

    /**
     * Returns type of upsert supported by driver if any
     */
    supportedUpsertTypes: UpsertType[] = ["on-conflict-do-update"]

    /**
     * Gets list of spatial column data types.
     */
    spatialTypes: ColumnType[] = ["geometry", "geography"]

    /**
     * Gets list of column data types that support length by a driver.
     */
    withLengthColumnTypes: ColumnType[] = [
        "character varying",
        "varchar",
        "character",
        "char",
        "bit",
        "varbit",
        "bit varying",
        "vector",
        "halfvec",
    ]

    /**
     * Gets list of column data types that support precision by a driver.
     */
    withPrecisionColumnTypes: ColumnType[] = [
        "numeric",
        "decimal",
        "interval",
        "time without time zone",
        "time with time zone",
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
    parametersPrefix: string = "$"

    /**
     * Default values of length, precision and scale depends on column data type.
     * Used in the cases when length/precision/scale is not specified by user.
     */
    dataTypeDefaults: DataTypeDefaults = {
        character: { length: 1 },
        bit: { length: 1 },
        interval: { precision: 6 },
        "time without time zone": { precision: 6 },
        "time with time zone": { precision: 6 },
        "timestamp without time zone": { precision: 6 },
        "timestamp with time zone": { precision: 6 },
    }

    /**
     * Max length allowed by Postgres for aliases.
     * @see https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
     */
    maxAliasLength = 63

    isGeneratedColumnsSupported: boolean = false

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
        this.options = connection.options as PostgresConnectionOptions
        this.isReplicated = this.options.replication ? true : false
        if (this.options.useUTC) {
            process.env.PGTZ = "UTC"
        }
        // load postgres package
        this.loadDependencies()

        this.database = DriverUtils.buildDriverOptions(
            this.options.replication
                ? this.options.replication.master
                : this.options,
        ).database
        this.schema = DriverUtils.buildDriverOptions(this.options).schema

        // ObjectUtils.assign(this.options, DriverUtils.buildDriverOptions(connection.options)); // todo: do it better way
        // validate options to make sure everything is set
        // todo: revisit validation with replication in mind
        // if (!this.options.host)
        //     throw new DriverOptionNotSetError("host");
        // if (!this.options.username)
        //     throw new DriverOptionNotSetError("username");
        // if (!this.options.database)
        //     throw new DriverOptionNotSetError("database");
    }

    // -------------------------------------------------------------------------
    // Public Implemented Methods
    // -------------------------------------------------------------------------

    maxRetryDuration = 5 * 60 * 1000

    /**
     * Number of consecutive tier2 (e.g. ECONNREFUSED) failures in
     * obtainMasterConnection before tearing down and rebuilding the pg Pool.
     * The pool is usually self-healing, but if it has entered a wedged
     * state, rebuilding is the only reliable recovery.
     */
    poolResetAfterConsecutiveTier2 = 3

    /**
     * Exposes the module-level {@link classifyError} as an instance method so
     * layers above the driver (e.g. EntityManager.transaction) can discover
     * retry eligibility without taking a hard dependency on this module.
     */
    classifyError(err: any): "tier1" | "tier2" | null {
        return classifyError(err)
    }

    /**
     * Runs `fn` under a shared retry deadline. Nested retry sites
     * (e.g. QueryRunner.query, obtainMasterConnection) will honor the
     * same deadline instead of stacking independent budgets on top.
     *
     * Exposed on the driver so EntityManager.transaction can participate
     * in the same mechanism without a hard dependency on the postgres
     * module.
     */
    withRetryDeadline<T>(budgetMs: number, fn: () => Promise<T>): Promise<T> {
        return withRetryDeadline(budgetMs, fn)
    }

    /**
     * Reads the currently active retry deadline, if any.
     */
    getRetryDeadline(): number | undefined {
        return getRetryDeadline()
    }
    /**
     * Performs connection to the database.
     * Based on pooling options, it can either create connection immediately,
     * either create a pool and create connection when needed.
     */
    async connect(): Promise<void> {
        // Honor an ambient retry deadline if one is already active; otherwise
        // scope our own. Sharing prevents stacked retry budgets across layers.
        return withRetryDeadline(this.maxRetryDuration, () =>
            this.connectWithRetry(),
        )
    }

    private async connectWithRetry(): Promise<void> {
        while (true) {
            let queryRunner: PostgresQueryRunner | undefined
            try {
                // Only create a new pool if we don't already have one.
                // On retry after a probe failure the pool itself may be fine.
                if (!this.master) {
                    this.master = await this.createPool(
                        this.options,
                        this.options,
                    )
                }

                queryRunner = this.createQueryRunner(
                    "master",
                ) as PostgresQueryRunner

                this.version = await queryRunner.getVersion()

                if (!this.database) {
                    this.database = await queryRunner.getCurrentDatabase()
                }

                if (!this.searchSchema) {
                    this.searchSchema = await queryRunner.getCurrentSchema()
                }

                await queryRunner.release()

                if (!this.schema) {
                    this.schema = this.searchSchema
                }

                return
            } catch (err) {
                // Always release the probe queryRunner to return the client
                // to the pool (or destroy it if the connection is broken).
                if (queryRunner) {
                    try {
                        await queryRunner.release()
                    } catch (_) {}
                }

                // If the probe or pool acquisition failed, decide whether
                // to destroy the pool or keep it for the next attempt.
                // tier-1 ("Connection terminated unexpectedly") means one
                // connection died but the pool itself is likely fine — keep it.
                // tier-2 or unknown errors suggest the pool may be broken.
                const tier = classifyError(err)
                if (this.master && tier !== "tier1") {
                    try {
                        this.master.removeAllListeners()
                        await this.master.end()
                    } catch (_) {}
                    this.master = undefined
                }

                if (tier === "tier1") {
                    await sleep(500)
                } else if (tier === "tier2") {
                    const deadline = getRetryDeadline()
                    if (deadline !== undefined && Date.now() > deadline) {
                        this.connection.logger.log(
                            "warn",
                            `Retry deadline exceeded in connect`,
                        )
                        throw err
                    }
                    await sleep(5000)
                    const deadlineAfter = getRetryDeadline()
                    if (
                        deadlineAfter !== undefined &&
                        Date.now() > deadlineAfter
                    ) {
                        this.connection.logger.log(
                            "warn",
                            `Retry deadline exceeded in connect after sleep`,
                        )
                        throw err
                    }
                } else {
                    throw err
                }
            }
        }
    }

    /**
     * Makes any action after connection (e.g. create extensions in Postgres driver).
     */
    async afterConnect(): Promise<void> {
        try {
            const extensionsMetadata = await this.checkMetadataForExtensions()
            const [connection, release] = await this.obtainMasterConnection()
            const installExtensions =
                this.options.installExtensions === undefined ||
                this.options.installExtensions
            if (installExtensions && extensionsMetadata.hasExtensions) {
                await this.enableExtensions(extensionsMetadata, connection)
            }

            this.isGeneratedColumnsSupported = VersionUtils.isGreaterOrEqual(
                this.version,
                "12.0",
            )
            await release()
        } catch (error) {
            console.info("PostgresDriver afterConnect release error", error)
            throw error
        }
    }

    protected async enableExtensions(extensionsMetadata: any, connection: any) {
        const { logger } = this.connection

        const {
            hasUuidColumns,
            hasCitextColumns,
            hasHstoreColumns,
            hasCubeColumns,
            hasGeometryColumns,
            hasLtreeColumns,
            hasVectorColumns,
            hasExclusionConstraints,
        } = extensionsMetadata

        if (hasUuidColumns)
            try {
                await this.executeQuery(
                    connection,
                    `CREATE EXTENSION IF NOT EXISTS "${
                        this.options.uuidExtension || "uuid-ossp"
                    }"`,
                )
            } catch (_) {
                logger.log(
                    "warn",
                    `At least one of the entities has uuid column, but the '${
                        this.options.uuidExtension || "uuid-ossp"
                    }' extension cannot be installed automatically. Please install it manually using superuser rights, or select another uuid extension.`,
                )
            }
        if (hasCitextColumns)
            try {
                await this.executeQuery(
                    connection,
                    `CREATE EXTENSION IF NOT EXISTS "citext"`,
                )
            } catch (_) {
                logger.log(
                    "warn",
                    "At least one of the entities has citext column, but the 'citext' extension cannot be installed automatically. Please install it manually using superuser rights",
                )
            }
        if (hasHstoreColumns)
            try {
                await this.executeQuery(
                    connection,
                    `CREATE EXTENSION IF NOT EXISTS "hstore"`,
                )
            } catch (_) {
                logger.log(
                    "warn",
                    "At least one of the entities has hstore column, but the 'hstore' extension cannot be installed automatically. Please install it manually using superuser rights",
                )
            }
        if (hasGeometryColumns)
            try {
                await this.executeQuery(
                    connection,
                    `CREATE EXTENSION IF NOT EXISTS "postgis"`,
                )
            } catch (_) {
                logger.log(
                    "warn",
                    "At least one of the entities has a geometry column, but the 'postgis' extension cannot be installed automatically. Please install it manually using superuser rights",
                )
            }
        if (hasCubeColumns)
            try {
                await this.executeQuery(
                    connection,
                    `CREATE EXTENSION IF NOT EXISTS "cube"`,
                )
            } catch (_) {
                logger.log(
                    "warn",
                    "At least one of the entities has a cube column, but the 'cube' extension cannot be installed automatically. Please install it manually using superuser rights",
                )
            }
        if (hasLtreeColumns)
            try {
                await this.executeQuery(
                    connection,
                    `CREATE EXTENSION IF NOT EXISTS "ltree"`,
                )
            } catch (_) {
                logger.log(
                    "warn",
                    "At least one of the entities has a ltree column, but the 'ltree' extension cannot be installed automatically. Please install it manually using superuser rights",
                )
            }
        if (hasVectorColumns)
            try {
                await this.executeQuery(
                    connection,
                    `CREATE EXTENSION IF NOT EXISTS "vector"`,
                )
            } catch (_) {
                logger.log(
                    "warn",
                    "At least one of the entities has a vector column, but the 'vector' extension (pgvector) cannot be installed automatically. Please install it manually using superuser rights",
                )
            }
        if (hasExclusionConstraints)
            try {
                // The btree_gist extension provides operator support in PostgreSQL exclusion constraints
                await this.executeQuery(
                    connection,
                    `CREATE EXTENSION IF NOT EXISTS "btree_gist"`,
                )
            } catch (_) {
                logger.log(
                    "warn",
                    "At least one of the entities has an exclusion constraint, but the 'btree_gist' extension cannot be installed automatically. Please install it manually using superuser rights",
                )
            }
    }

    protected async checkMetadataForExtensions() {
        const hasUuidColumns = this.connection.entityMetadatas.some(
            (metadata) => {
                return (
                    metadata.generatedColumns.filter(
                        (column) => column.generationStrategy === "uuid",
                    ).length > 0
                )
            },
        )
        const hasCitextColumns = this.connection.entityMetadatas.some(
            (metadata) => {
                return (
                    metadata.columns.filter(
                        (column) => column.type === "citext",
                    ).length > 0
                )
            },
        )
        const hasHstoreColumns = this.connection.entityMetadatas.some(
            (metadata) => {
                return (
                    metadata.columns.filter(
                        (column) => column.type === "hstore",
                    ).length > 0
                )
            },
        )
        const hasCubeColumns = this.connection.entityMetadatas.some(
            (metadata) => {
                return (
                    metadata.columns.filter((column) => column.type === "cube")
                        .length > 0
                )
            },
        )
        const hasGeometryColumns = this.connection.entityMetadatas.some(
            (metadata) => {
                return (
                    metadata.columns.filter(
                        (column) => this.spatialTypes.indexOf(column.type) >= 0,
                    ).length > 0
                )
            },
        )
        const hasLtreeColumns = this.connection.entityMetadatas.some(
            (metadata) => {
                return (
                    metadata.columns.filter((column) => column.type === "ltree")
                        .length > 0
                )
            },
        )
        const hasVectorColumns = this.connection.entityMetadatas.some(
            (metadata) => {
                return metadata.columns.some(
                    (column) =>
                        column.type === "vector" || column.type === "halfvec",
                )
            },
        )
        const hasExclusionConstraints = this.connection.entityMetadatas.some(
            (metadata) => {
                return metadata.exclusions.length > 0
            },
        )

        return {
            hasUuidColumns,
            hasCitextColumns,
            hasHstoreColumns,
            hasCubeColumns,
            hasGeometryColumns,
            hasLtreeColumns,
            hasVectorColumns,
            hasExclusionConstraints,
            hasExtensions:
                hasUuidColumns ||
                hasCitextColumns ||
                hasHstoreColumns ||
                hasGeometryColumns ||
                hasCubeColumns ||
                hasLtreeColumns ||
                hasVectorColumns ||
                hasExclusionConstraints,
        }
    }

    /**
     * Closes connection with database.
     */
    async disconnect(): Promise<void> {
        if (!this.master)
            return Promise.reject(new ConnectionIsNotSetError("postgres"))

        await this.closePool(this.master)
        this.master = undefined
    }

    /**
     * Creates a schema builder used to build and sync a schema.
     */
    createSchemaBuilder() {
        return new RdbmsSchemaBuilder(this.connection)
    }

    /**
     * Creates a query runner used to execute database queries.
     */
    createQueryRunner(mode: ReplicationMode): PostgresQueryRunner {
        return new PostgresQueryRunner(this, mode)
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
            ["json", "jsonb", ...this.spatialTypes].indexOf(
                columnMetadata.type,
            ) >= 0
        ) {
            return JSON.stringify(value)
        } else if (
            columnMetadata.type === "vector" ||
            columnMetadata.type === "halfvec"
        ) {
            if (Array.isArray(value)) {
                return `[${value.join(",")}]`
            } else {
                return value
            }
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
        } else if (
            columnMetadata.type === "vector" ||
            columnMetadata.type === "halfvec"
        ) {
            if (
                typeof value === "string" &&
                value.startsWith("[") &&
                value.endsWith("]")
            ) {
                if (value === "[]") return []
                return value.slice(1, -1).split(",").map(Number)
            }
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
            value = value.replace(/[()\s]+/g, "") // remove whitespace
            if (columnMetadata.isArray) {
                /**
                 * Strips these groups from `{"1,2,3","",NULL}`:
                 * 1. ["1,2,3", undefined]  <- cube of arity 3
                 * 2. ["", undefined]         <- cube of arity 0
                 * 3. [undefined, "NULL"]     <- NULL
                 */
                const regexp = /(?:"((?:[\d\s.,])*)")|(?:(NULL))/g
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
                    .slice(1, -1)
                    .split(",")
                    .map((val) => {
                        // replace double quotes from the beginning and from the end
                        if (val.startsWith(`"`) && val.endsWith(`"`))
                            val = val.slice(1, -1)
                        // replace escaped backslash and double quotes
                        return val.replace(/\\(\\|")/g, "$1")
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

    isEnum(obj: any) {
        if (Array.isArray(obj)) {
            return false
        }
        if (typeof obj === "object" && obj !== null) {
            return Object.entries(obj).every(
                ([key, value]) =>
                    typeof key === "string" &&
                    (typeof value === "string" || typeof value === "number"),
            )
        }
        return false
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
            return "timestamp without time zone"
        } else if (column.type === "timestamptz") {
            return "timestamp with time zone"
        } else if (column.type === "time") {
            return "time without time zone"
        } else if (column.type === "timetz") {
            return "time with time zone"
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
        } else if (column.type === "varbit") {
            return "bit varying"
        } else if (this.isEnum(column.type)) {
            return "character varying"
        } else {
            return (column.type as string) || ""
        }
    }

    /**
     * Normalizes "default" value of the column.
     */
    normalizeDefault(columnMetadata: ColumnMetadata): string | undefined {
        const defaultValue = columnMetadata.default

        if (defaultValue === null || defaultValue === undefined) {
            return undefined
        }

        if (columnMetadata.isArray && Array.isArray(defaultValue)) {
            return `'{${defaultValue.map((val) => String(val)).join(",")}}'`
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

        return `${defaultValue}`
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
        } else if (column.type === "vector" || column.type === "halfvec") {
            type =
                column.type + (column.length ? "(" + column.length + ")" : "")
        }

        if (column.isArray) type += " array"

        return type
    }

    /**
     * Obtains a new database connection to a master server.
     * Used for replication.
     * If replication is not setup then returns default connection's database connection.
     *
     * Retries transient failures (ECONNREFUSED, ECONNRESET, ETIMEDOUT, etc.) so
     * that callers such as QueryRunner.connect() and Driver.afterConnect() do
     * not surface brief DB outages as fatal errors. Retry budgets mirror the
     * ones in Driver.connect() / QueryRunner.query():
     *
     *   - tier1 ("Connection terminated unexpectedly"): 500 ms sleep, no cap
     *   - tier2 (ECONNREFUSED et al.): 5000 ms sleep, capped at
     *     {@link maxRetryDuration}
     *
     * If tier2 failures persist past {@link poolResetAfterConsecutiveTier2},
     * the pool itself is torn down and recreated. This handles the edge case
     * where the pg Pool's internal state has become wedged. Pool recreation
     * is guarded against concurrent callers by comparing the captured pool
     * reference against {@link master}.
     *
     * If {@link master} is transiently empty when this is called — e.g. a
     * prior rebuild failed, or a concurrent caller has torn the pool down
     * and is mid-rebuild — we do NOT immediately throw
     * ConnectionIsNotSetError. Doing so would mask a retryable outage
     * (ECONNREFUSED) as an unrecoverable configuration error, defeating the
     * retry budget. Instead, the initial-connect path (Driver.connect) is the
     * sole authority that decides connectivity is fundamentally unset.
     */
    async obtainMasterConnection(): Promise<[PoolClient, Function]> {
        // Honor an ambient retry deadline set by an outer layer (e.g.
        // EntityManager.transaction or QueryRunner.query). If none is active,
        // establish one scoped to this call so the retry budget is bounded.
        return withRetryDeadline(this.maxRetryDuration, () =>
            this.obtainMasterConnectionWithRetry(),
        )
    }

    private async obtainMasterConnectionWithRetry(): Promise<
        [PoolClient, Function]
    > {
        let consecutiveTier2 = 0

        while (true) {
            // Capture the current pool reference so we can detect whether
            // another concurrent caller has already rebuilt it.
            let pool = this.master

            // If master is currently empty, try to rebuild it in-place (within
            // the retry budget) rather than giving up. This covers two cases:
            //   (a) a previous iteration tore the pool down and createPool()
            //       failed with a retryable error;
            //   (b) a concurrent caller is mid-rebuild.
            // We attempt at most one rebuild per iteration; if it fails
            // retryably we fall through to the standard tier2 sleep+retry.
            if (!pool) {
                const deadline = getRetryDeadline()
                if (deadline !== undefined && Date.now() > deadline) {
                    this.connection.logger.log(
                        "warn",
                        `Retry deadline exceeded while master pool is unavailable`,
                    )
                    throw new ConnectionIsNotSetError("postgres")
                }
                try {
                    const rebuilt = await this.createPool(
                        this.options,
                        this.options,
                    )
                    if (!this.master) {
                        this.master = rebuilt
                    } else {
                        // Another caller already rebuilt; discard ours.
                        try {
                            await rebuilt.end()
                        } catch {
                            /* noop */
                        }
                    }
                    pool = this.master
                    if (!pool) {
                        // Extremely unlikely, but stay defensive.
                        await sleep(500)
                        continue
                    }
                } catch (rebuildErr) {
                    const rebuildTier = classifyError(rebuildErr)
                    if (rebuildTier === null) {
                        throw rebuildErr
                    }
                    await sleep(rebuildTier === "tier2" ? 5000 : 500)
                    continue
                }
            }

            try {
                // pg Pool handles timeouts internally via connectionTimeoutMillis.
                // No artificial setTimeout wrapper — avoids spurious
                // "Connection failed" errors and timer/client leaks.
                const client = await pool.connect()

                const safeRelease = (err: any) => {
                    try {
                        if (client && typeof client.release === "function") {
                            client.release(err)
                        }
                    } catch (releaseError) {
                        this.connection.logger.log(
                            "warn",
                            `Error releasing connection: ${releaseError}`,
                        )
                    }
                }
                return [client, safeRelease]
            } catch (err) {
                const tier = classifyError(err)

                if (tier === null) {
                    throw err
                }

                if (tier === "tier2") {
                    // Check deadline before deciding to retry.
                    const deadline = getRetryDeadline()
                    if (deadline !== undefined && Date.now() > deadline) {
                        this.connection.logger.log(
                            "warn",
                            `Retry deadline exceeded in obtainMasterConnection`,
                        )
                        throw err
                    }

                    consecutiveTier2 += 1

                    // If the pool has been failing for a while, it may be in
                    // a wedged state. Rebuild it. Guarded so only one
                    // concurrent caller actually performs the rebuild.
                    //
                    // Ordering matters:
                    //   1. Build the replacement pool into a local variable.
                    //   2. Only after success, atomically swap `this.master`
                    //      and tear down the old pool.
                    // This avoids two hazards:
                    //   (a) transient `this.master === undefined` windows that
                    //       would cause concurrent callers to see an empty
                    //       pool, and that would downgrade a retryable
                    //       ECONNREFUSED into an unrecoverable
                    //       ConnectionIsNotSetError in a later iteration;
                    //   (b) tearing down the old pool before a replacement
                    //       exists, which would leave callers with a closed
                    //       pool if createPool() then fails.
                    if (
                        consecutiveTier2 >=
                            this.poolResetAfterConsecutiveTier2 &&
                        this.master === pool
                    ) {
                        try {
                            const rebuilt = await this.createPool(
                                this.options,
                                this.options,
                            )
                            if (this.master === pool) {
                                this.master = rebuilt
                                // Tear down the old, wedged pool only after
                                // the replacement is in place.
                                try {
                                    pool.removeAllListeners()
                                    await pool.end()
                                } catch (cleanupErr) {
                                    this.connection.logger.log(
                                        "warn",
                                        `Error ending old pool during retry: ${cleanupErr}`,
                                    )
                                }
                            } else {
                                // Another caller swapped in a new pool while
                                // we were building; discard ours.
                                try {
                                    await rebuilt.end()
                                } catch {
                                    /* noop */
                                }
                            }
                        } catch (recreateErr) {
                            // Rebuild failed. Keep the old pool reference in
                            // place (still potentially usable) and let the
                            // retry loop continue. Non-retryable errors
                            // surface immediately.
                            const recreateTier = classifyError(recreateErr)
                            if (recreateTier === null) {
                                throw recreateErr
                            }
                            // fall through to sleep + retry
                        }
                        consecutiveTier2 = 0
                    }

                    // Sleep, then re-check the deadline before continuing —
                    // so we don't perform an extra attempt after expiry.
                    await sleep(5000)
                    const deadlineAfter = getRetryDeadline()
                    if (
                        deadlineAfter !== undefined &&
                        Date.now() > deadlineAfter
                    ) {
                        this.connection.logger.log(
                            "warn",
                            `Retry deadline exceeded in obtainMasterConnection after sleep`,
                        )
                        throw err
                    }
                    continue
                }

                // tier1: indefinite retry with short sleep
                consecutiveTier2 = 0
                await sleep(500)
                continue
            }
        }
    }

    /**
     * Used for replication.
     * If replication is not setup then returns master (default) connection's database connection.
     */
    async obtainSlaveConnection(): Promise<[any, Function]> {
        return new Promise((ok, fail) => {})
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
                    (columnMetadata.asExpression || "").trim() ||
                tableColumn.collation !== columnMetadata.collation

            // DEBUG SECTION
            // if (isColumnChanged) {
            //     console.log("table:", columnMetadata.entityMetadata.tableName)
            //     console.log(
            //         "name:",
            //         tableColumn.name,
            //         columnMetadata.databaseName,
            //     )
            //     console.log(
            //         "type:",
            //         tableColumn.type,
            //         this.normalizeType(columnMetadata),
            //     )
            //     console.log(
            //         "length:",
            //         tableColumn.length,
            //         columnMetadata.length,
            //     )
            //     console.log(
            //         "isArray:",
            //         tableColumn.isArray,
            //         columnMetadata.isArray,
            //     )
            //     console.log(
            //         "precision:",
            //         tableColumn.precision,
            //         columnMetadata.precision,
            //     )
            //     console.log("scale:", tableColumn.scale, columnMetadata.scale)
            //     console.log(
            //         "comment:",
            //         tableColumn.comment,
            //         this.escapeComment(columnMetadata.comment),
            //     )
            //     console.log(
            //         "enumName:",
            //         tableColumn.enumName,
            //         columnMetadata.enumName,
            //     )
            //     console.log(
            //         "enum:",
            //         tableColumn.enum &&
            //             columnMetadata.enum &&
            //             !OrmUtils.isArraysEqual(
            //                 tableColumn.enum,
            //                 columnMetadata.enum.map((val) => val + ""),
            //             ),
            //     )
            //     console.log(
            //         "isPrimary:",
            //         tableColumn.isPrimary,
            //         columnMetadata.isPrimary,
            //     )
            //     console.log(
            //         "isNullable:",
            //         tableColumn.isNullable,
            //         columnMetadata.isNullable,
            //     )
            //     console.log(
            //         "isUnique:",
            //         tableColumn.isUnique,
            //         this.normalizeIsUnique(columnMetadata),
            //     )
            //     console.log(
            //         "isGenerated:",
            //         tableColumn.isGenerated,
            //         columnMetadata.isGenerated,
            //     )
            //     console.log(
            //         "generatedType:",
            //         tableColumn.generatedType,
            //         columnMetadata.generatedType,
            //     )
            //     console.log(
            //         "asExpression:",
            //         (tableColumn.asExpression || "").trim(),
            //         (columnMetadata.asExpression || "").trim(),
            //     )
            //     console.log(
            //         "collation:",
            //         tableColumn.collation,
            //         columnMetadata.collation,
            //     )
            //     console.log(
            //         "isGenerated 2:",
            //         !tableColumn.isGenerated &&
            //             this.lowerDefaultValueIfNecessary(
            //                 this.normalizeDefault(columnMetadata),
            //             ) !== tableColumn.default,
            //     )
            //     console.log(
            //         "spatialFeatureType:",
            //         (tableColumn.spatialFeatureType || "").toLowerCase(),
            //         (columnMetadata.spatialFeatureType || "").toLowerCase(),
            //     )
            //     console.log("srid", tableColumn.srid, columnMetadata.srid)
            //     console.log("==========================================")
            // }

            return isColumnChanged
        })
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

    /**
     * Returns true if driver supports RETURNING / OUTPUT statement.
     */
    isReturningSqlSupported(): boolean {
        return true
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

    get uuidGenerator(): string {
        return this.options.uuidExtension === "pgcrypto"
            ? "gen_random_uuid()"
            : "uuid_generate_v4()"
    }

    /**
     * Creates an escaped parameter.
     */
    createParameter(parameterName: string, index: number): string {
        return this.parametersPrefix + (index + 1)
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Loads postgres query stream package.
     */
    loadStreamDependency() {
        try {
            return PlatformTools.load("pg-query-stream")
        } catch {
            // todo: better error for browser env
            throw new TypeORMError(
                `To use streams you should install pg-query-stream package. Please run "npm i pg-query-stream".`,
            )
        }
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * If driver dependency is not given explicitly, then try to load it via "require".
     */
    protected loadDependencies(): void {}

    /**
     * Creates a new connection pool for a given database credentials.
     */
    protected async createPool(
        options: PostgresConnectionOptions,
        credentials: PostgresConnectionCredentialsOptions,
    ): Promise<any> {
        const { logger } = this.connection
        credentials = Object.assign({}, credentials)

        const connectionOptions: PoolConfig = {
            connectionString: credentials.url,
            host: credentials.host,
            user: credentials.username,
            password: credentials.password,
            database: credentials.database,
            port: credentials.port,
            ssl: credentials.ssl,
            connectionTimeoutMillis: options.connectTimeoutMS || 10000,
            application_name:
                options.applicationName ?? credentials.applicationName,
            // TCP keepalive prevents AWS NAT Gateway / Security Groups from
            // silently dropping idle connections (~350s). Without this, the
            // pool hands out dead sockets on Lambda warm invocations.
            keepAlive: true,
            keepAliveInitialDelayMillis: 10000,
            ...(options.extra || {}),
        }

        let pool: Pool | undefined = undefined

        try {
            pool = new Pool(connectionOptions)

            pool.on("error", (error: Error) => {
                logger.log("warn", `Postgres pool error: ${error}`)
            })

            pool.on("connect", (client) => {
                // Belt-and-suspenders: force TCP keepAlive on the raw socket.
                // pg's keepAlive pool option should handle this, but setting
                // it directly on the socket guarantees it regardless of pg
                // version or internal timing.
                const stream = (client as any).connection?.stream
                if (stream && typeof stream.setKeepAlive === "function") {
                    stream.setKeepAlive(true, 10000)
                }

                client.on("error", (err: Error) => {
                    logger.log("warn", `Postgres client error: ${err}`)
                })

                const originalRelease = client.release
                client.release = function (err?: Error) {
                    client.removeAllListeners("error")
                    return originalRelease.call(this, err)
                }
            })

            // pg Pool handles connection timeouts via connectionTimeoutMillis.
            // No artificial setTimeout — avoids spurious timeout errors.
            const client = await pool.connect()

            try {
                await client.query("SELECT 1")
            } finally {
                client.release()
            }

            return pool
        } catch (error) {
            if (pool) {
                try {
                    pool.removeAllListeners()
                    await pool.end()
                } catch (cleanupError) {
                    logger.log(
                        "warn",
                        `Error cleaning up pool: ${cleanupError}`,
                    )
                }
            }

            throw error
        }
    }

    /**
     * Closes connection pool.
     */
    protected async closePool(pool: any): Promise<void> {
        while (this.connectedQueryRunners.length) {
            await this.connectedQueryRunners[0].release()
        }

        return new Promise<void>((ok, fail) => {
            pool.end((err: any) => (err ? fail(err) : ok()))
        })
    }

    /**
     * Executes given query.
     */
    protected executeQuery(connection: any, query: string) {
        this.connection.logger.logQuery(query)

        return new Promise((ok, fail) => {
            connection.query(query, (err: any, result: any) =>
                err ? fail(err) : ok(result),
            )
        })
    }

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
}
