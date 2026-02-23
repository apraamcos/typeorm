import type snowflake from "snowflake-sdk"
import type { SnowflakeError } from "snowflake-sdk"
import { QueryFailedError, TypeORMError } from "../../error"
import { QueryRunnerAlreadyReleasedError } from "../../error/QueryRunnerAlreadyReleasedError"
import { TransactionNotStartedError } from "../../error/TransactionNotStartedError"
import { ReadStream } from "../../platform/PlatformTools"
import { BaseQueryRunner } from "../../query-runner/BaseQueryRunner"
import { QueryRunner } from "../../query-runner/QueryRunner"
import { QueryResult } from "../../query-runner/QueryResult"
import { Table } from "../../schema-builder/table/Table"
import { TableCheck } from "../../schema-builder/table/TableCheck"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { TableExclusion } from "../../schema-builder/table/TableExclusion"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { TableIndex } from "../../schema-builder/table/TableIndex"
import { TableUnique } from "../../schema-builder/table/TableUnique"
import { View } from "../../schema-builder/view/View"
import { Broadcaster } from "../../subscriber/Broadcaster"
import { BroadcasterResult } from "../../subscriber/BroadcasterResult"
import { InstanceChecker } from "../../util/InstanceChecker"
import { OrmUtils } from "../../util/OrmUtils"
import { Query } from "../Query"
import { ColumnType } from "../types/ColumnTypes"
import { IsolationLevel } from "../types/IsolationLevel"
import { MetadataTableType } from "../types/MetadataTableType"
import { ReplicationMode } from "../types/ReplicationMode"
import { SnowflakeDriver } from "./SnowflakeDriver"
import { formatSnowflakeParameter } from "../../util/Snowflake"
import { ObjectLiteral } from "../../common/ObjectLiteral"

/**
 * Runs queries on a single Snowflake database connection.
 */
export class SnowflakeQueryRunner
    extends BaseQueryRunner
    implements QueryRunner
{
    /**
     * Escapes a string literal for safe embedding in SQL.
     * Prefer binds when possible, but Snowflake SHOW statements don't support binds.
     *
     * @param value - The raw string value to escape.
     * @returns The escaped string with single quotes doubled.
     */
    protected escapeLiteral(value: string): string {
        return value.replace(/'/g, "''")
    }

    /**
     * Escapes an identifier for safe use in SQL.
     * (e.g. schema/table/database/column names)
     *
     * @param value - The raw identifier name.
     * @returns The identifier wrapped in double quotes with embedded `"` doubled.
     */
    protected escapeIdentifier(value: string): string {
        return `"${value.replace(/"/g, '""')}"`
    }

    /**
     * Escapes and joins multiple column names into a comma-separated SQL list.
     *
     * @param names - Array of raw column names.
     * @returns Comma-separated escaped identifiers, e.g. `"col1", "col2"`.
     */
    protected quoteColumns(names: string[]): string {
        return names.map((n) => this.escapeIdentifier(n)).join(", ")
    }

    /**
     * Resolves the primary key constraint name for a table.
     *
     * Uses the explicit constraint name from the first primary column if set,
     * otherwise falls back to the naming strategy.
     *
     * @param table - The table (used by naming strategy for name generation).
     * @param primaryColumns - The primary columns of the table.
     * @returns The resolved primary key constraint name.
     */
    protected resolvePrimaryKeyName(
        table: Table,
        primaryColumns: TableColumn[],
    ): string {
        if (primaryColumns.length === 0) {
            throw new TypeORMError(
                `Cannot resolve primary key name for table "${table.name}" — no primary columns provided.`,
            )
        }
        const explicit = primaryColumns[0]?.primaryKeyConstraintName
        if (explicit) return explicit
        return this.connection.namingStrategy.primaryKeyName(
            table,
            primaryColumns.map((c) => c.name),
        )
    }

    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    /**
     * Database driver used by connection.
     */
    driver: SnowflakeDriver

    // -------------------------------------------------------------------------
    // Protected Properties
    // -------------------------------------------------------------------------

    /**
     * Promise used to obtain a database connection for the first time.
     * Acts as a dedup guard so multiple concurrent `connect()` calls
     * don't acquire multiple pool connections.
     */
    protected databaseConnectionPromise: Promise<any> | undefined

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(driver: SnowflakeDriver, mode: ReplicationMode) {
        super()
        this.driver = driver
        this.connection = driver.connection
        this.broadcaster = new Broadcaster(this)
        this.mode = mode
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Creates/uses database connection from the connection pool to perform further operations.
     *
     * @returns The acquired database connection (Snowflake SDK connection object).
     */
    async connect(): Promise<any> {
        if (this.databaseConnection) {
            return this.databaseConnection
        }

        if (this.databaseConnectionPromise) {
            return this.databaseConnectionPromise
        }

        this.databaseConnectionPromise = (async () => {
            try {
                const connection = await this.driver.obtainMasterConnection()
                this.driver.connectedQueryRunners.push(this)
                this.databaseConnection = connection
                return connection
            } catch (error) {
                this.databaseConnectionPromise = undefined
                throw error
            }
        })()

        return this.databaseConnectionPromise
    }

    /**
     * Releases used database connection.
     * You cannot use query runner methods once its released.
     *
     * @returns Resolves when the underlying connection is returned to the pool.
     */
    async release(): Promise<void> {
        if (this.isReleased) {
            return
        }

        this.isReleased = true

        const index = this.driver.connectedQueryRunners.indexOf(this)
        if (index !== -1) {
            this.driver.connectedQueryRunners.splice(index, 1)
        }

        // If connect() was called but hasn't resolved yet, await it first
        // to avoid leaking the connection.
        if (!this.databaseConnection && this.databaseConnectionPromise) {
            try {
                this.databaseConnection = await this.databaseConnectionPromise
            } catch {
                // Connection failed — nothing to release.
                this.databaseConnectionPromise = undefined
                return
            }
        }

        if (this.databaseConnection) {
            await this.driver.releaseConnection(this.databaseConnection)
            this.databaseConnection = undefined
            this.databaseConnectionPromise = undefined
        }
    }

    /**
     * Starts transaction.
     * Snowflake supports flat transactions only (no savepoints)
     * and only READ COMMITTED isolation level.
     *
     * @param isolationLevel - Requested isolation level. Only `"READ COMMITTED"`
     *                         is supported; other values are logged as warnings and ignored.
     * @returns Resolves when the `BEGIN` statement has been executed.
     */
    async startTransaction(isolationLevel?: IsolationLevel): Promise<void> {
        if (isolationLevel && isolationLevel !== "READ COMMITTED") {
            this.driver.connection.logger.log(
                "warn",
                `Snowflake only supports READ COMMITTED isolation level. ` +
                    `Requested "${isolationLevel}" will be ignored.`,
            )
        }
        this.isTransactionActive = true
        try {
            await this.broadcaster.broadcast("BeforeTransactionStart")
        } catch (err) {
            this.isTransactionActive = false
            throw err
        }

        try {
            await this.query("BEGIN")
        } catch (err) {
            this.isTransactionActive = false
            throw err
        }

        await this.broadcaster.broadcast("AfterTransactionStart")
    }

    /**
     * Commits transaction.
     *
     * @returns Resolves when the `COMMIT` statement has been executed.
     * @throws {TransactionNotStartedError} If no transaction is active.
     */
    async commitTransaction(): Promise<void> {
        if (!this.isTransactionActive) throw new TransactionNotStartedError()

        await this.broadcaster.broadcast("BeforeTransactionCommit")

        await this.query("COMMIT")
        this.isTransactionActive = false

        await this.broadcaster.broadcast("AfterTransactionCommit")
    }

    /**
     * Rollbacks transaction.
     *
     * @returns Resolves when the `ROLLBACK` statement has been executed.
     * @throws {TransactionNotStartedError} If no transaction is active.
     */
    async rollbackTransaction(): Promise<void> {
        if (!this.isTransactionActive) throw new TransactionNotStartedError()

        await this.broadcaster.broadcast("BeforeTransactionRollback")

        await this.query("ROLLBACK")
        this.isTransactionActive = false

        await this.broadcaster.broadcast("AfterTransactionRollback")
    }

    /**
     * Executes a given SQL query against the Snowflake database.
     *
     * @param query - The SQL string to execute.
     * @param parameters - Optional positional bind parameters.
     * @param useStructuredResult - If `true`, returns a {@link QueryResult} wrapper
     *                              instead of the raw rows array.
     * @returns The query result rows, or a {@link QueryResult} if `useStructuredResult` is set.
     */
    async query(
        query: string,
        parameters?: any[],
        useStructuredResult: boolean = false,
    ): Promise<any> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        const databaseConnection = await this.connect()

        this.driver.connection.logger.logQuery(query, parameters, this)
        await this.broadcaster.broadcast("BeforeQuery", query, parameters)

        const broadcasterResult = new BroadcasterResult()
        const queryStartTime = Date.now()

        try {
            parameters = parameters
                ? formatSnowflakeParameter(parameters)
                : undefined

            const { rows, stmt } = await new Promise<{
                rows: any[]
                stmt: snowflake.RowStatement
            }>((resolve, reject) =>
                databaseConnection.execute({
                    sqlText: query,
                    binds: parameters,
                    complete: (
                        err: SnowflakeError | undefined,
                        stmt: snowflake.RowStatement,
                        rows: any,
                    ) =>
                        err ? reject(err) : resolve({ rows: rows || [], stmt }),
                }),
            )

            // log slow queries
            const maxQueryExecutionTime =
                this.driver.options.maxQueryExecutionTime
            const queryEndTime = Date.now()
            const queryExecutionTime = queryEndTime - queryStartTime

            if (
                maxQueryExecutionTime &&
                queryExecutionTime > maxQueryExecutionTime
            )
                this.driver.connection.logger.logQuerySlow(
                    queryExecutionTime,
                    query,
                    parameters,
                    this,
                )

            const result = new QueryResult()
            result.raw = rows
            result.records = rows
            result.affected = stmt.getNumUpdatedRows() ?? stmt.getNumRows()

            this.broadcaster.broadcastAfterQueryEvent(
                broadcasterResult,
                query,
                parameters,
                true,
                queryExecutionTime,
                rows,
                undefined,
            )

            if (useStructuredResult) {
                return result
            } else {
                return result.records
            }
        } catch (err) {
            this.driver.connection.logger.logQueryError(
                err,
                query,
                parameters,
                this,
            )

            this.broadcaster.broadcastAfterQueryEvent(
                broadcasterResult,
                query,
                parameters,
                false,
                undefined,
                undefined,
                err,
            )

            throw new QueryFailedError(query, parameters, err)
        } finally {
            await broadcasterResult.wait()
        }
    }

    /**
     * Returns raw data stream for the given query.
     *
     * @param query - The SQL string to execute in streaming mode.
     * @param parameters - Optional positional bind parameters.
     * @param onEnd - Optional callback invoked when the stream ends.
     * @param onError - Optional callback invoked on stream error.
     * @param onData - Optional callback invoked for each row of data.
     * @returns A readable stream of query result rows.
     */
    async stream(
        query: string,
        parameters?: any[],
        onEnd?: Function,
        onError?: Function,
        onData?: (rowData: any) => void,
    ): Promise<ReadStream> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        this.driver.connection.logger.logQuery(query, parameters, this)
        parameters = parameters
            ? formatSnowflakeParameter(parameters)
            : undefined
        const connection = await this.connect()
        return new Promise((resolve, reject) =>
            connection.execute({
                streamResult: true,
                sqlText: query,
                binds: parameters,
                complete: (
                    err: SnowflakeError | undefined,
                    stmt: snowflake.RowStatement,
                    rows: any,
                ) => {
                    if (err) {
                        this.driver.connection.logger.logQueryError(
                            err,
                            query,
                            parameters,
                            this,
                        )
                        return reject(
                            new QueryFailedError(query, parameters, err),
                        )
                    }
                    const readStream = stmt.streamRows()
                    readStream.on("data", onData ?? (() => {}))

                    if (onError) {
                        readStream.on("error", onError as any)
                    }

                    if (onEnd) {
                        readStream.on("end", onEnd as any)
                    }

                    // The promise represents stream acquisition only.
                    // Stream errors are reported via `onError` and the stream's `error` event.
                    resolve(readStream as any)
                },
            }),
        )
    }

    /**
     * Returns all available database names including system databases.
     *
     * @returns An array of database name strings.
     */
    async getDatabases(): Promise<string[]> {
        const results = await this.query(`SHOW DATABASES`)
        return results.map((row: any) => row["name"])
    }

    /**
     * Returns all available schema names including system schemas.
     * If database parameter specified, returns schemas of that database.
     *
     * @param database - Optional database name to scope the schema listing.
     * @returns An array of schema name strings.
     */
    async getSchemas(database?: string): Promise<string[]> {
        const sql = database
            ? `SHOW SCHEMAS IN DATABASE ${this.escapeIdentifier(database)}`
            : `SHOW SCHEMAS`
        const results = await this.query(sql)
        return results.map((row: any) => row["name"])
    }

    /**
     * Checks if database with the given name exist.
     *
     * @param database - The database name to check.
     * @returns `true` if the database exists.
     */
    async hasDatabase(database: string): Promise<boolean> {
        // Snowflake LIKE patterns treat % and _ as wildcards.
        // Escape them so e.g. "test_db" doesn't match "testXdb".
        const escaped = this.escapeLiteral(database)
            .replace(/%/g, "\\%")
            .replace(/_/g, "\\_")
        const result = await this.query(`SHOW DATABASES LIKE '${escaped}'`)
        return result.length > 0
    }

    /**
     * Loads currently using database.
     *
     * @returns The current database name string.
     */
    async getCurrentDatabase(): Promise<string> {
        const result = await this.query(
            `SELECT CURRENT_DATABASE() AS "CURRENT_DATABASE"`,
        )
        return result[0]["CURRENT_DATABASE"]
    }

    /**
     * Checks if schema with the given name exist.
     *
     * @param schema - The schema name to check.
     * @returns `true` if the schema exists.
     */
    async hasSchema(schema: string): Promise<boolean> {
        const result = await this.query(
            `SELECT * FROM "INFORMATION_SCHEMA"."SCHEMATA" WHERE "SCHEMA_NAME" = :1`,
            [schema],
        )
        return result.length > 0
    }

    /**
     * Loads currently using database schema.
     *
     * @returns The current schema name string.
     */
    async getCurrentSchema(): Promise<string> {
        const result = await this.query(
            `SELECT CURRENT_SCHEMA() AS "CURRENT_SCHEMA"`,
        )
        return result[0]["CURRENT_SCHEMA"]
    }

    /**
     * Checks if table with the given name exist in the database.
     *
     * @param tableOrName - A {@link Table} instance or table name string to check.
     * @returns `true` if the table exists.
     */
    async hasTable(tableOrName: Table | string): Promise<boolean> {
        const parsedTableName = this.driver.parseTableName(tableOrName)

        if (!parsedTableName.schema) {
            parsedTableName.schema = await this.getCurrentSchema()
        }

        const result = await this.query(
            `SELECT * FROM "INFORMATION_SCHEMA"."TABLES" WHERE "TABLE_SCHEMA" = :1 AND "TABLE_NAME" = :2`,
            [parsedTableName.schema, parsedTableName.tableName],
        )
        return result.length > 0
    }

    /**
     * Checks if column with the given name exist in the given table.
     *
     * @param tableOrName - A {@link Table} instance or table name string.
     * @param columnName - The column name to check for.
     * @returns `true` if the column exists in the table.
     */
    async hasColumn(
        tableOrName: Table | string,
        columnName: string,
    ): Promise<boolean> {
        const parsedTableName = this.driver.parseTableName(tableOrName)

        if (!parsedTableName.schema) {
            parsedTableName.schema = await this.getCurrentSchema()
        }

        const result = await this.query(
            `SELECT * FROM "INFORMATION_SCHEMA"."COLUMNS" WHERE "TABLE_SCHEMA" = :1 AND "TABLE_NAME" = :2 AND "COLUMN_NAME" = :3`,
            [parsedTableName.schema, parsedTableName.tableName, columnName],
        )
        return result.length > 0
    }

    /**
     * Creates a new database.
     *
     * @param database - The database name to create.
     * @param ifNotExist - If `true`, adds `IF NOT EXISTS` to the DDL.
     * @returns Resolves when the database has been created.
     */
    async createDatabase(
        database: string,
        ifNotExist?: boolean,
    ): Promise<void> {
        const up = ifNotExist
            ? `CREATE DATABASE IF NOT EXISTS ${this.escapeIdentifier(database)}`
            : `CREATE DATABASE ${this.escapeIdentifier(database)}`
        const down = `DROP DATABASE ${this.escapeIdentifier(database)}`
        await this.executeQueries(new Query(up), new Query(down))
    }

    /**
     * Drops database.
     *
     * @param database - The database name to drop.
     * @param ifExist - If `true`, adds `IF EXISTS` to the DDL.
     * @returns Resolves when the database has been dropped.
     */
    async dropDatabase(database: string, ifExist?: boolean): Promise<void> {
        const up = ifExist
            ? `DROP DATABASE IF EXISTS ${this.escapeIdentifier(database)}`
            : `DROP DATABASE ${this.escapeIdentifier(database)}`
        const down = `CREATE DATABASE ${this.escapeIdentifier(database)}`
        await this.executeQueries(new Query(up), new Query(down))
    }

    /**
     * Creates a new table schema.
     *
     * @param schemaPath - The schema name, optionally qualified as `"database.schema"`.
     * @param ifNotExist - If `true`, adds `IF NOT EXISTS` to the DDL.
     * @returns Resolves when the schema has been created.
     */
    async createSchema(
        schemaPath: string,
        ifNotExist?: boolean,
    ): Promise<void> {
        const schema =
            schemaPath.indexOf(".") === -1
                ? schemaPath
                : schemaPath.split(".")[1]

        const up = ifNotExist
            ? `CREATE SCHEMA IF NOT EXISTS ${this.escapeIdentifier(schema)}`
            : `CREATE SCHEMA ${this.escapeIdentifier(schema)}`
        const down = `DROP SCHEMA ${this.escapeIdentifier(schema)}`
        await this.executeQueries(new Query(up), new Query(down))
    }

    /**
     * Drops table schema.
     *
     * @param schemaPath - The schema name, optionally qualified as `"database.schema"`.
     * @param ifExist - If `true`, adds `IF EXISTS` to the DDL.
     * @param isCascade - If `true`, adds `CASCADE` to the DDL.
     * @returns Resolves when the schema has been dropped.
     */
    async dropSchema(
        schemaPath: string,
        ifExist?: boolean,
        isCascade?: boolean,
    ): Promise<void> {
        const schema =
            schemaPath.indexOf(".") === -1
                ? schemaPath
                : schemaPath.split(".")[1]

        const cascade = isCascade ? " CASCADE" : ""
        const up = ifExist
            ? `DROP SCHEMA IF EXISTS ${this.escapeIdentifier(schema)}${cascade}`
            : `DROP SCHEMA ${this.escapeIdentifier(schema)}${cascade}`
        const down = `CREATE SCHEMA ${this.escapeIdentifier(schema)}`
        await this.executeQueries(new Query(up), new Query(down))
    }

    /**
     * Creates a new table.
     *
     * @param table - The {@link Table} definition to create.
     * @param ifNotExist - If `true`, skips creation when the table already exists.
     * @param createForeignKeys - If `true`, also creates foreign key constraints.
     * @param createIndices - If `true`, also creates indices (no-op for Snowflake).
     * @returns Resolves when the table and associated constraints have been created.
     */
    async createTable(
        table: Table,
        ifNotExist: boolean = false,
        createForeignKeys: boolean = true,
        createIndices: boolean = true,
    ): Promise<void> {
        if (ifNotExist) {
            const isTableExist = await this.hasTable(table)
            if (isTableExist) return
        }
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        // if table have column with generated type, we must add the expression to the metadata table
        const generatedColumns = table.columns.filter(
            (column) =>
                column.generatedType === "STORED" && column.asExpression,
        )

        // CREATE TABLE must come first — metadata INSERTs reference the table.
        upQueries.push(this.createTableSql(table, createForeignKeys))

        // Down queries: FK drops must come BEFORE DROP TABLE (can't drop FKs
        // on a table that no longer exists).
        if (createForeignKeys)
            table.foreignKeys.forEach((foreignKey) =>
                downQueries.push(this.dropForeignKeySql(table, foreignKey)),
            )
        downQueries.push(this.dropTableSql(table))

        if (generatedColumns.length > 0) {
            const { schema, tableName } = await this.parseTableNameWithSchema(
                table.name,
            )
            for (const column of generatedColumns) {
                const insertQuery = this.insertTypeormMetadataSql({
                    database: this.driver.database,
                    schema,
                    table: tableName,
                    type: MetadataTableType.GENERATED_COLUMN,
                    name: column.name,
                    value: column.asExpression,
                })

                const deleteQuery = this.deleteTypeormMetadataSql({
                    database: this.driver.database,
                    schema,
                    table: tableName,
                    type: MetadataTableType.GENERATED_COLUMN,
                    name: column.name,
                })

                upQueries.push(insertQuery)
                downQueries.push(deleteQuery)
            }
        }

        // Add column comments as separate statements (Snowflake SDK doesn't support multi-statement)
        table.columns
            .filter((column) => column.comment)
            .forEach((column) => {
                upQueries.push(
                    new Query(
                        `COMMENT ON COLUMN ${this.escapePath(
                            table,
                        )}.${this.escapeIdentifier(
                            column.name,
                        )} IS ${this.escapeComment(column.comment)}`,
                    ),
                )
            })

        // Snowflake does not support user-created indexes, so skip createIndices

        await this.executeQueries(upQueries, downQueries)
    }

    /**
     * Drops the table.
     *
     * @param target - The {@link Table} instance or table name string to drop.
     * @param ifExist - If `true`, does nothing when the table does not exist.
     * @param dropForeignKeys - If `true`, drops associated foreign key constraints first.
     * @param dropIndices - If `true`, drops indices first (no-op for Snowflake).
     * @returns Resolves when the table has been dropped.
     */
    async dropTable(
        target: Table | string,
        ifExist?: boolean,
        dropForeignKeys: boolean = true,
        dropIndices: boolean = true,
    ): Promise<void> {
        if (ifExist) {
            const isTableExist = await this.hasTable(target)
            if (!isTableExist) return
        }

        const createForeignKeys: boolean = dropForeignKeys
        const tablePath = this.getTablePath(target)
        const table = await this.getCachedTable(tablePath)
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        // Snowflake does not support user-created indexes, so skip dropIndices

        if (dropForeignKeys)
            table.foreignKeys.forEach((foreignKey) =>
                upQueries.push(this.dropForeignKeySql(table, foreignKey)),
            )

        upQueries.push(this.dropTableSql(table))
        downQueries.push(this.createTableSql(table, createForeignKeys))

        // Add column comments as separate down queries (Snowflake SDK doesn't support multi-statement)
        table.columns
            .filter((column) => column.comment)
            .forEach((column) => {
                downQueries.push(
                    new Query(
                        `COMMENT ON COLUMN ${this.escapePath(
                            table,
                        )}.${this.escapeIdentifier(
                            column.name,
                        )} IS ${this.escapeComment(column.comment)}`,
                    ),
                )
            })

        // if table had columns with generated type, we must remove the expression from the metadata table
        const generatedColumns = table.columns.filter(
            (column) =>
                column.generatedType === "STORED" && column.asExpression,
        )
        for (const column of generatedColumns) {
            const { schema, tableName } = await this.parseTableNameWithSchema(
                table.name,
            )

            const deleteQuery = this.deleteTypeormMetadataSql({
                database: this.driver.database,
                schema,
                table: tableName,
                type: MetadataTableType.GENERATED_COLUMN,
                name: column.name,
            })

            const insertQuery = this.insertTypeormMetadataSql({
                database: this.driver.database,
                schema,
                table: tableName,
                type: MetadataTableType.GENERATED_COLUMN,
                name: column.name,
                value: column.asExpression,
            })

            upQueries.push(deleteQuery)
            downQueries.push(insertQuery)
        }

        await this.executeQueries(upQueries, downQueries)
    }

    /**
     * Creates a new view.
     *
     * @param view - The {@link View} definition to create.
     * @param syncWithMetadata - If `true`, records the view definition in the typeorm metadata table.
     * @returns Resolves when the view has been created.
     */
    async createView(
        view: View,
        syncWithMetadata: boolean = false,
    ): Promise<void> {
        const upQueries: Query[] = []
        const downQueries: Query[] = []
        upQueries.push(this.createViewSql(view))
        if (syncWithMetadata)
            upQueries.push(await this.insertViewDefinitionSql(view))
        downQueries.push(this.dropViewSql(view))
        if (syncWithMetadata)
            downQueries.push(await this.deleteViewDefinitionSql(view))
        await this.executeQueries(upQueries, downQueries)
    }

    /**
     * Drops the view.
     *
     * @param target - The {@link View} instance or view name string to drop.
     * @returns Resolves when the view has been dropped.
     */
    async dropView(target: View | string): Promise<void> {
        const viewName = InstanceChecker.isView(target) ? target.name : target
        const view = await this.getCachedView(viewName)

        const upQueries: Query[] = []
        const downQueries: Query[] = []
        // Drop the view first, then delete metadata (view must exist for
        // metadata delete to be logically ordered — and if DROP fails, we
        // don't want stale metadata deleted).
        upQueries.push(this.dropViewSql(view))
        upQueries.push(await this.deleteViewDefinitionSql(view))
        downQueries.push(this.createViewSql(view))
        downQueries.push(await this.insertViewDefinitionSql(view))
        await this.executeQueries(upQueries, downQueries)
    }

    /**
     * Renames the given table.
     *
     * @param oldTableOrName - The existing {@link Table} instance or table name string.
     * @param newTableName - The new table name.
     * @returns Resolves when the table has been renamed and all dependent constraints updated.
     */
    async renameTable(
        oldTableOrName: Table | string,
        newTableName: string,
    ): Promise<void> {
        const upQueries: Query[] = []
        const downQueries: Query[] = []
        const oldTable = InstanceChecker.isTable(oldTableOrName)
            ? oldTableOrName
            : await this.getCachedTable(oldTableOrName)
        const newTable = oldTable.clone()

        const { schema: schemaName, tableName: oldTableName } =
            this.driver.parseTableName(oldTable)

        newTable.name = schemaName
            ? `${schemaName}.${newTableName}`
            : newTableName

        upQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    oldTable,
                )} RENAME TO ${this.escapeIdentifier(newTableName)}`,
            ),
        )
        downQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    newTable,
                )} RENAME TO ${this.escapeIdentifier(oldTableName)}`,
            ),
        )

        // rename column primary key constraint if it has default constraint name
        if (
            newTable.primaryColumns.length > 0 &&
            !newTable.primaryColumns[0].primaryKeyConstraintName
        ) {
            const columnNames = newTable.primaryColumns.map(
                (column) => column.name,
            )

            const oldPkName = this.connection.namingStrategy.primaryKeyName(
                oldTable,
                columnNames,
            )

            const newPkName = this.connection.namingStrategy.primaryKeyName(
                newTable,
                columnNames,
            )

            // Snowflake does not support RENAME CONSTRAINT — drop old + create new
            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} DROP CONSTRAINT ${this.escapeIdentifier(oldPkName)}`,
                ),
            )
            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} ADD CONSTRAINT ${this.escapeIdentifier(
                        newPkName,
                    )} PRIMARY KEY (${this.quoteColumns(columnNames)})`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} DROP CONSTRAINT ${this.escapeIdentifier(newPkName)}`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} ADD CONSTRAINT ${this.escapeIdentifier(
                        oldPkName,
                    )} PRIMARY KEY (${this.quoteColumns(columnNames)})`,
                ),
            )
        }

        // Snowflake uses AUTOINCREMENT — no sequences to rename

        // rename unique constraints
        newTable.uniques.forEach((unique) => {
            const oldUniqueName =
                this.connection.namingStrategy.uniqueConstraintName(
                    oldTable,
                    unique.columnNames,
                )

            // Skip renaming if Unique has user defined constraint name
            if (unique.name !== oldUniqueName) return

            // build new constraint name
            const newUniqueName =
                this.connection.namingStrategy.uniqueConstraintName(
                    newTable,
                    unique.columnNames,
                )

            // Snowflake does not support RENAME CONSTRAINT — drop old + create new
            const uniqueColumnsSql = this.quoteColumns(unique.columnNames)
            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} DROP CONSTRAINT ${this.escapeIdentifier(unique.name)}`,
                ),
            )
            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} ADD CONSTRAINT ${this.escapeIdentifier(
                        newUniqueName,
                    )} UNIQUE (${uniqueColumnsSql})`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} DROP CONSTRAINT ${this.escapeIdentifier(newUniqueName)}`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} ADD CONSTRAINT ${this.escapeIdentifier(
                        unique.name,
                    )} UNIQUE (${uniqueColumnsSql})`,
                ),
            )

            // replace constraint name
            unique.name = newUniqueName
        })

        // Snowflake does not support user-created indexes — skip index renaming

        // rename foreign key constraints
        newTable.foreignKeys.forEach((foreignKey) => {
            const oldForeignKeyName =
                this.connection.namingStrategy.foreignKeyName(
                    oldTable,
                    foreignKey.columnNames,
                    this.getTablePath(foreignKey),
                    foreignKey.referencedColumnNames,
                )

            // Skip renaming if foreign key has user defined constraint name
            if (foreignKey.name !== oldForeignKeyName) return

            // build new constraint name
            const newForeignKeyName =
                this.connection.namingStrategy.foreignKeyName(
                    newTable,
                    foreignKey.columnNames,
                    this.getTablePath(foreignKey),
                    foreignKey.referencedColumnNames,
                )

            // Snowflake does not support RENAME CONSTRAINT — drop old + create new
            const fkColumnsSql = this.quoteColumns(foreignKey.columnNames)
            const fkRefColumnsSql = this.quoteColumns(
                foreignKey.referencedColumnNames,
            )
            let fkConstraint = `CONSTRAINT ${this.escapeIdentifier(
                newForeignKeyName,
            )} FOREIGN KEY (${fkColumnsSql}) REFERENCES ${this.escapePath(
                this.getTablePath(foreignKey),
            )} (${fkRefColumnsSql})`
            if (foreignKey.onDelete)
                fkConstraint += ` ON DELETE ${foreignKey.onDelete}`
            let fkConstraintOld = `CONSTRAINT ${this.escapeIdentifier(
                foreignKey.name!,
            )} FOREIGN KEY (${fkColumnsSql}) REFERENCES ${this.escapePath(
                this.getTablePath(foreignKey),
            )} (${fkRefColumnsSql})`
            if (foreignKey.onDelete)
                fkConstraintOld += ` ON DELETE ${foreignKey.onDelete}`

            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} DROP CONSTRAINT ${this.escapeIdentifier(
                        foreignKey.name!,
                    )}`,
                ),
            )
            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} ADD ${fkConstraint}`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} DROP CONSTRAINT ${this.escapeIdentifier(
                        newForeignKeyName,
                    )}`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        newTable,
                    )} ADD ${fkConstraintOld}`,
                ),
            )

            // replace constraint name
            foreignKey.name = newForeignKeyName
        })

        // Snowflake has no user-defined ENUM types — skip ENUM renaming

        // Update generated column metadata in typeorm_metadata table for the renamed table.
        const generatedColumns = newTable.columns.filter(
            (column) =>
                column.generatedType === "STORED" && column.asExpression,
        )
        if (generatedColumns.length > 0) {
            const { schema: oldSchema, tableName: oldTblName } =
                this.driver.parseTableName(oldTable)
            const { schema: newSchema, tableName: newTblName } =
                this.driver.parseTableName(newTable)
            const effectiveOldSchema =
                oldSchema || this.driver.options.schema || ""
            const effectiveNewSchema =
                newSchema || this.driver.options.schema || ""

            for (const column of generatedColumns) {
                // Delete old metadata entry
                upQueries.push(
                    this.deleteTypeormMetadataSql({
                        database: this.driver.database,
                        schema: effectiveOldSchema,
                        table: oldTblName,
                        type: MetadataTableType.GENERATED_COLUMN,
                        name: column.name,
                    }),
                )
                // Insert new metadata entry with new table name
                upQueries.push(
                    this.insertTypeormMetadataSql({
                        database: this.driver.database,
                        schema: effectiveNewSchema,
                        table: newTblName,
                        type: MetadataTableType.GENERATED_COLUMN,
                        name: column.name,
                        value: column.asExpression,
                    }),
                )
                // Down: reverse the operation
                downQueries.push(
                    this.deleteTypeormMetadataSql({
                        database: this.driver.database,
                        schema: effectiveNewSchema,
                        table: newTblName,
                        type: MetadataTableType.GENERATED_COLUMN,
                        name: column.name,
                    }),
                )
                downQueries.push(
                    this.insertTypeormMetadataSql({
                        database: this.driver.database,
                        schema: effectiveOldSchema,
                        table: oldTblName,
                        type: MetadataTableType.GENERATED_COLUMN,
                        name: column.name,
                        value: column.asExpression,
                    }),
                )
            }
        }

        await this.executeQueries(upQueries, downQueries)
    }

    /**
     * Creates a new column from the column in the table.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param column - The {@link TableColumn} to add.
     * @returns Resolves when the column has been added and any associated constraints updated.
     */
    async addColumn(
        tableOrName: Table | string,
        column: TableColumn,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const clonedTable = table.clone()
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        upQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    table,
                )} ADD ${this.buildCreateColumnSql(table, column)}`,
            ),
        )
        downQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    table,
                )} DROP COLUMN ${this.escapeIdentifier(column.name)}`,
            ),
        )

        // create or update primary key constraint
        if (column.isPrimary) {
            const primaryColumns = clonedTable.primaryColumns
            // if table already have primary key, we must drop it and recreate again
            if (primaryColumns.length > 0) {
                const pkName = this.resolvePrimaryKeyName(
                    clonedTable,
                    primaryColumns,
                )

                const columnNames = this.quoteColumns(
                    primaryColumns.map((column) => column.name),
                )

                upQueries.push(
                    new Query(
                        `ALTER TABLE ${this.escapePath(
                            table,
                        )} DROP CONSTRAINT ${this.escapeIdentifier(pkName)}`,
                    ),
                )
                downQueries.push(
                    new Query(
                        `ALTER TABLE ${this.escapePath(
                            table,
                        )} ADD CONSTRAINT ${this.escapeIdentifier(
                            pkName,
                        )} PRIMARY KEY (${columnNames})`,
                    ),
                )
            }

            primaryColumns.push(column)
            const pkName = this.resolvePrimaryKeyName(
                clonedTable,
                primaryColumns,
            )

            const columnNames = this.quoteColumns(
                primaryColumns.map((column) => column.name),
            )

            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        table,
                    )} ADD CONSTRAINT ${this.escapeIdentifier(
                        pkName,
                    )} PRIMARY KEY (${columnNames})`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        table,
                    )} DROP CONSTRAINT ${this.escapeIdentifier(pkName)}`,
                ),
            )
        }

        // Snowflake does not support user-created indexes — skip column index creation

        // create unique constraint
        if (column.isUnique) {
            const uniqueConstraint = new TableUnique({
                name: this.connection.namingStrategy.uniqueConstraintName(
                    table,
                    [column.name],
                ),
                columnNames: [column.name],
            })
            clonedTable.uniques.push(uniqueConstraint)
            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        table,
                    )} ADD CONSTRAINT ${this.escapeIdentifier(
                        uniqueConstraint.name!,
                    )} UNIQUE (${this.escapeIdentifier(column.name)})`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        table,
                    )} DROP CONSTRAINT ${this.escapeIdentifier(
                        uniqueConstraint.name!,
                    )}`,
                ),
            )
        }

        if (column.generatedType === "STORED" && column.asExpression) {
            const { schema, tableName } = await this.parseTableNameWithSchema(
                table.name,
            )

            const insertQuery = this.insertTypeormMetadataSql({
                database: this.driver.database,
                schema,
                table: tableName,
                type: MetadataTableType.GENERATED_COLUMN,
                name: column.name,
                value: column.asExpression,
            })

            const deleteQuery = this.deleteTypeormMetadataSql({
                database: this.driver.database,
                schema,
                table: tableName,
                type: MetadataTableType.GENERATED_COLUMN,
                name: column.name,
            })

            upQueries.push(insertQuery)
            downQueries.push(deleteQuery)
        }

        // create column's comment
        if (column.comment) {
            upQueries.push(
                new Query(
                    `COMMENT ON COLUMN ${this.escapePath(
                        table,
                    )}.${this.escapeIdentifier(
                        column.name,
                    )} IS ${this.escapeComment(column.comment)}`,
                ),
            )
            downQueries.push(
                new Query(
                    `COMMENT ON COLUMN ${this.escapePath(
                        table,
                    )}.${this.escapeIdentifier(column.name)} IS NULL`,
                ),
            )
        }

        await this.executeQueries(upQueries, downQueries)

        clonedTable.addColumn(column)
        this.replaceCachedTable(table, clonedTable)
    }

    /**
     * Creates new columns from the column in the table.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param columns - Array of {@link TableColumn} definitions to add.
     * @returns Resolves when all columns have been added.
     */
    async addColumns(
        tableOrName: Table | string,
        columns: TableColumn[],
    ): Promise<void> {
        for (const column of columns) {
            await this.addColumn(tableOrName, column)
        }
    }

    /**
     * Renames column in the given table.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param oldTableColumnOrName - The existing {@link TableColumn} or column name string.
     * @param newTableColumnOrName - The new {@link TableColumn} or column name string.
     * @returns Resolves when the column has been renamed.
     */
    async renameColumn(
        tableOrName: Table | string,
        oldTableColumnOrName: TableColumn | string,
        newTableColumnOrName: TableColumn | string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)

        const oldColumn = InstanceChecker.isTableColumn(oldTableColumnOrName)
            ? oldTableColumnOrName
            : table.columns.find(
                  (column) => column.name === oldTableColumnOrName,
              )
        if (!oldColumn)
            throw new TypeORMError(
                `Column "${
                    InstanceChecker.isTableColumn(oldTableColumnOrName)
                        ? oldTableColumnOrName.name
                        : oldTableColumnOrName
                }" was not found in the "${table.name}" table.`,
            )

        let newColumn: TableColumn | undefined = undefined
        if (InstanceChecker.isTableColumn(newTableColumnOrName)) {
            newColumn = newTableColumnOrName
        } else {
            newColumn = oldColumn.clone()
            newColumn.name = newTableColumnOrName
        }

        return this.changeColumn(table, oldColumn, newColumn)
    }

    /**
     * Changes a column in the table.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param oldTableColumnOrName - The existing {@link TableColumn} or column name string.
     * @param newColumn - The new {@link TableColumn} definition to apply.
     * @returns Resolves when the column has been altered.
     */
    async changeColumn(
        tableOrName: Table | string,
        oldTableColumnOrName: TableColumn | string,
        newColumn: TableColumn,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        let clonedTable = table.clone()
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        // Clone oldColumn so we don't mutate the caller's input
        // (e.g. when renaming, we update oldColumn.name internally).
        const oldColumn = (
            InstanceChecker.isTableColumn(oldTableColumnOrName)
                ? oldTableColumnOrName
                : table.columns.find(
                      (column) => column.name === oldTableColumnOrName,
                  )
        )?.clone()
        if (!oldColumn)
            throw new TypeORMError(
                `Column "${
                    InstanceChecker.isTableColumn(oldTableColumnOrName)
                        ? oldTableColumnOrName.name
                        : oldTableColumnOrName
                }" was not found in the "${table.name}" table.`,
            )

        if (
            oldColumn.type !== newColumn.type ||
            oldColumn.length !== newColumn.length ||
            (!oldColumn.generatedType &&
                newColumn.generatedType === "STORED") ||
            (oldColumn.asExpression !== newColumn.asExpression &&
                newColumn.generatedType === "STORED") ||
            oldColumn.isGenerated !== newColumn.isGenerated
        ) {
            // To avoid data conversion, we just recreate column
            await this.dropColumn(table, oldColumn)
            await this.addColumn(table, newColumn)

            // update cloned table
            clonedTable = table.clone()
        } else {
            const escapedTable = this.escapePath(table)
            const escapedOldName = this.escapeIdentifier(oldColumn.name)
            const escapedNewName = this.escapeIdentifier(newColumn.name)

            if (oldColumn.name !== newColumn.name) {
                // rename column
                upQueries.push(
                    new Query(
                        `ALTER TABLE ${escapedTable} RENAME COLUMN ${escapedOldName} TO ${escapedNewName}`,
                    ),
                )
                downQueries.push(
                    new Query(
                        `ALTER TABLE ${escapedTable} RENAME COLUMN ${escapedNewName} TO ${escapedOldName}`,
                    ),
                )

                // Snowflake has no user-defined ENUM types — skip ENUM renaming

                // rename column primary key constraint
                if (
                    oldColumn.isPrimary === true &&
                    !oldColumn.primaryKeyConstraintName
                ) {
                    const primaryColumns = clonedTable.primaryColumns

                    // build old primary constraint name
                    const columnNames = primaryColumns.map(
                        (column) => column.name,
                    )
                    const oldPkName =
                        this.connection.namingStrategy.primaryKeyName(
                            clonedTable,
                            columnNames,
                        )

                    // replace old column name with new column name (in-place to preserve order)
                    const oldNameIdx = columnNames.indexOf(oldColumn.name)
                    if (oldNameIdx !== -1)
                        columnNames[oldNameIdx] = newColumn.name

                    // build new primary constraint name
                    const newPkName =
                        this.connection.namingStrategy.primaryKeyName(
                            clonedTable,
                            columnNames,
                        )

                    // Snowflake does not support RENAME CONSTRAINT — drop old + create new
                    const pkColumnsSql = this.quoteColumns(columnNames)
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} DROP CONSTRAINT ${this.escapeIdentifier(
                                oldPkName,
                            )}`,
                        ),
                    )
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ADD CONSTRAINT ${this.escapeIdentifier(
                                newPkName,
                            )} PRIMARY KEY (${pkColumnsSql})`,
                        ),
                    )
                    // For down: reverse column name substitution (newColumn→oldColumn, in-place)
                    const downColumnNames = [...columnNames]
                    const downNewIdx = downColumnNames.indexOf(newColumn.name)
                    if (downNewIdx !== -1)
                        downColumnNames[downNewIdx] = oldColumn.name
                    const downPkColumnsSql = this.quoteColumns(downColumnNames)
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} DROP CONSTRAINT ${this.escapeIdentifier(
                                newPkName,
                            )}`,
                        ),
                    )
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ADD CONSTRAINT ${this.escapeIdentifier(
                                oldPkName,
                            )} PRIMARY KEY (${downPkColumnsSql})`,
                        ),
                    )
                }

                // Snowflake uses AUTOINCREMENT — no sequences to rename

                // rename unique constraints
                clonedTable.findColumnUniques(oldColumn).forEach((unique) => {
                    const oldUniqueName =
                        this.connection.namingStrategy.uniqueConstraintName(
                            clonedTable,
                            unique.columnNames,
                        )

                    // Skip renaming if Unique has user defined constraint name
                    if (unique.name !== oldUniqueName) return

                    // build new constraint name (in-place replacement to preserve column order)
                    const oldUqNameIdx = unique.columnNames.indexOf(
                        oldColumn.name,
                    )
                    if (oldUqNameIdx !== -1)
                        unique.columnNames[oldUqNameIdx] = newColumn.name
                    const newUniqueName =
                        this.connection.namingStrategy.uniqueConstraintName(
                            clonedTable,
                            unique.columnNames,
                        )

                    // Snowflake does not support RENAME CONSTRAINT — drop old + create new
                    const uqColumnsSql = this.quoteColumns(unique.columnNames)
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} DROP CONSTRAINT ${this.escapeIdentifier(
                                unique.name!,
                            )}`,
                        ),
                    )
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ADD CONSTRAINT ${this.escapeIdentifier(
                                newUniqueName,
                            )} UNIQUE (${uqColumnsSql})`,
                        ),
                    )
                    // For down: use old column names (in-place replacement)
                    const downUqColumnNames = [...unique.columnNames]
                    const downUqNewIdx = downUqColumnNames.indexOf(
                        newColumn.name,
                    )
                    if (downUqNewIdx !== -1)
                        downUqColumnNames[downUqNewIdx] = oldColumn.name
                    const downUqColumnsSql =
                        this.quoteColumns(downUqColumnNames)
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} DROP CONSTRAINT ${this.escapeIdentifier(
                                newUniqueName,
                            )}`,
                        ),
                    )
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ADD CONSTRAINT ${this.escapeIdentifier(
                                unique.name!,
                            )} UNIQUE (${downUqColumnsSql})`,
                        ),
                    )

                    // replace constraint name
                    unique.name = newUniqueName
                })

                // Snowflake does not support user-created indexes — skip index renaming

                // rename foreign key constraints
                clonedTable
                    .findColumnForeignKeys(oldColumn)
                    .forEach((foreignKey) => {
                        const foreignKeyName =
                            this.connection.namingStrategy.foreignKeyName(
                                clonedTable,
                                foreignKey.columnNames,
                                this.getTablePath(foreignKey),
                                foreignKey.referencedColumnNames,
                            )

                        // Skip renaming if foreign key has user defined constraint name
                        if (foreignKey.name !== foreignKeyName) return

                        // build new constraint name (in-place replacement to preserve column order)
                        const oldFkNameIdx = foreignKey.columnNames.indexOf(
                            oldColumn.name,
                        )
                        if (oldFkNameIdx !== -1)
                            foreignKey.columnNames[oldFkNameIdx] =
                                newColumn.name
                        const newForeignKeyName =
                            this.connection.namingStrategy.foreignKeyName(
                                clonedTable,
                                foreignKey.columnNames,
                                this.getTablePath(foreignKey),
                                foreignKey.referencedColumnNames,
                            )

                        // Snowflake does not support RENAME CONSTRAINT — drop old + create new
                        const fkColsSql = this.quoteColumns(
                            foreignKey.columnNames,
                        )
                        const fkRefColsSql = this.quoteColumns(
                            foreignKey.referencedColumnNames,
                        )
                        let newFkDef = `CONSTRAINT ${this.escapeIdentifier(
                            newForeignKeyName,
                        )} FOREIGN KEY (${fkColsSql}) REFERENCES ${this.escapePath(
                            this.getTablePath(foreignKey),
                        )} (${fkRefColsSql})`
                        if (foreignKey.onDelete)
                            newFkDef += ` ON DELETE ${foreignKey.onDelete}`

                        // For down: use old column names (in-place replacement)
                        const downFkColumnNames = [...foreignKey.columnNames]
                        const downFkNewIdx = downFkColumnNames.indexOf(
                            newColumn.name,
                        )
                        if (downFkNewIdx !== -1)
                            downFkColumnNames[downFkNewIdx] = oldColumn.name
                        const downFkColsSql =
                            this.quoteColumns(downFkColumnNames)
                        let oldFkDef = `CONSTRAINT ${this.escapeIdentifier(
                            foreignKey.name!,
                        )} FOREIGN KEY (${downFkColsSql}) REFERENCES ${this.escapePath(
                            this.getTablePath(foreignKey),
                        )} (${fkRefColsSql})`
                        if (foreignKey.onDelete)
                            oldFkDef += ` ON DELETE ${foreignKey.onDelete}`

                        upQueries.push(
                            new Query(
                                `ALTER TABLE ${escapedTable} DROP CONSTRAINT ${this.escapeIdentifier(
                                    foreignKey.name!,
                                )}`,
                            ),
                        )
                        upQueries.push(
                            new Query(
                                `ALTER TABLE ${escapedTable} ADD ${newFkDef}`,
                            ),
                        )
                        downQueries.push(
                            new Query(
                                `ALTER TABLE ${escapedTable} DROP CONSTRAINT ${this.escapeIdentifier(
                                    newForeignKeyName,
                                )}`,
                            ),
                        )
                        downQueries.push(
                            new Query(
                                `ALTER TABLE ${escapedTable} ADD ${oldFkDef}`,
                            ),
                        )

                        // replace constraint name
                        foreignKey.name = newForeignKeyName
                    })

                // rename old column in the Table object
                const oldTableColumn = clonedTable.columns.find(
                    (column) => column.name === oldColumn.name,
                )
                if (!oldTableColumn) {
                    throw new TypeORMError(
                        `Column "${oldColumn.name}" was not found in table "${clonedTable.name}".`,
                    )
                }
                oldTableColumn.name = newColumn.name
                oldColumn.name = newColumn.name
            }

            if (
                newColumn.precision !== oldColumn.precision ||
                newColumn.scale !== oldColumn.scale
            ) {
                upQueries.push(
                    new Query(
                        `ALTER TABLE ${escapedTable} ALTER COLUMN ${escapedNewName} SET DATA TYPE ${this.driver.createFullType(
                            newColumn,
                        )}`,
                    ),
                )
                downQueries.push(
                    new Query(
                        `ALTER TABLE ${escapedTable} ALTER COLUMN ${escapedNewName} SET DATA TYPE ${this.driver.createFullType(
                            oldColumn,
                        )}`,
                    ),
                )
            }

            // Snowflake has no user-defined ENUM types — skip ENUM type change logic

            if (oldColumn.isNullable !== newColumn.isNullable) {
                if (newColumn.isNullable) {
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ALTER COLUMN ${escapedNewName} DROP NOT NULL`,
                        ),
                    )
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ALTER COLUMN ${escapedNewName} SET NOT NULL`,
                        ),
                    )
                } else {
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ALTER COLUMN ${escapedNewName} SET NOT NULL`,
                        ),
                    )
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ALTER COLUMN ${escapedNewName} DROP NOT NULL`,
                        ),
                    )
                }
            }

            if (oldColumn.comment !== newColumn.comment) {
                upQueries.push(
                    new Query(
                        `COMMENT ON COLUMN ${escapedTable}.${escapedNewName} IS ${this.escapeComment(
                            newColumn.comment,
                        )}`,
                    ),
                )
                downQueries.push(
                    new Query(
                        `COMMENT ON COLUMN ${escapedTable}.${escapedNewName} IS ${this.escapeComment(
                            oldColumn.comment,
                        )}`,
                    ),
                )
            }

            if (newColumn.isPrimary !== oldColumn.isPrimary) {
                const primaryColumns = clonedTable.primaryColumns

                // if primary column state changed, we must always drop existed constraint.
                if (primaryColumns.length > 0) {
                    const pkName = this.resolvePrimaryKeyName(
                        clonedTable,
                        primaryColumns,
                    )

                    const columnNamesSql = this.quoteColumns(
                        primaryColumns.map((column) => column.name),
                    )

                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} DROP CONSTRAINT ${this.escapeIdentifier(
                                pkName,
                            )}`,
                        ),
                    )
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ADD CONSTRAINT ${this.escapeIdentifier(
                                pkName,
                            )} PRIMARY KEY (${columnNamesSql})`,
                        ),
                    )
                }

                if (newColumn.isPrimary === true) {
                    primaryColumns.push(newColumn)
                    // update column in table
                    const column = clonedTable.columns.find(
                        (column) => column.name === newColumn.name,
                    )
                    column!.isPrimary = true
                    const pkName = this.resolvePrimaryKeyName(
                        clonedTable,
                        primaryColumns,
                    )

                    const columnNamesSql = this.quoteColumns(
                        primaryColumns.map((column) => column.name),
                    )

                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ADD CONSTRAINT ${this.escapeIdentifier(
                                pkName,
                            )} PRIMARY KEY (${columnNamesSql})`,
                        ),
                    )
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} DROP CONSTRAINT ${this.escapeIdentifier(
                                pkName,
                            )}`,
                        ),
                    )
                } else {
                    const primaryColumn = primaryColumns.find(
                        (c) => c.name === newColumn.name,
                    )
                    if (primaryColumn) {
                        const pcIdx = primaryColumns.indexOf(primaryColumn)
                        if (pcIdx !== -1) primaryColumns.splice(pcIdx, 1)
                    }

                    // update column in table
                    const column = clonedTable.columns.find(
                        (column) => column.name === newColumn.name,
                    )
                    column!.isPrimary = false

                    // if we have another primary keys, we must recreate constraint.
                    if (primaryColumns.length > 0) {
                        const pkName = this.resolvePrimaryKeyName(
                            clonedTable,
                            primaryColumns,
                        )

                        const columnNamesSql = this.quoteColumns(
                            primaryColumns.map((column) => column.name),
                        )

                        upQueries.push(
                            new Query(
                                `ALTER TABLE ${escapedTable} ADD CONSTRAINT ${this.escapeIdentifier(
                                    pkName,
                                )} PRIMARY KEY (${columnNamesSql})`,
                            ),
                        )
                        downQueries.push(
                            new Query(
                                `ALTER TABLE ${escapedTable} DROP CONSTRAINT ${this.escapeIdentifier(
                                    pkName,
                                )}`,
                            ),
                        )
                    }
                }
            }

            if (newColumn.isUnique !== oldColumn.isUnique) {
                if (newColumn.isUnique === true) {
                    const uniqueConstraint = new TableUnique({
                        name: this.connection.namingStrategy.uniqueConstraintName(
                            table,
                            [newColumn.name],
                        ),
                        columnNames: [newColumn.name],
                    })
                    clonedTable.uniques.push(uniqueConstraint)
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ADD CONSTRAINT ${this.escapeIdentifier(
                                uniqueConstraint.name!,
                            )} UNIQUE (${escapedNewName})`,
                        ),
                    )
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} DROP CONSTRAINT ${this.escapeIdentifier(
                                uniqueConstraint.name!,
                            )}`,
                        ),
                    )
                } else {
                    const uniqueConstraint = clonedTable.uniques.find(
                        (unique) => {
                            return (
                                unique.columnNames.length === 1 &&
                                unique.columnNames.includes(newColumn.name)
                            )
                        },
                    )
                    if (uniqueConstraint) {
                        const ucIdx =
                            clonedTable.uniques.indexOf(uniqueConstraint)
                        if (ucIdx !== -1) clonedTable.uniques.splice(ucIdx, 1)
                        upQueries.push(
                            new Query(
                                `ALTER TABLE ${escapedTable} DROP CONSTRAINT ${this.escapeIdentifier(
                                    uniqueConstraint.name!,
                                )}`,
                            ),
                        )
                        downQueries.push(
                            new Query(
                                `ALTER TABLE ${escapedTable} ADD CONSTRAINT ${this.escapeIdentifier(
                                    uniqueConstraint.name!,
                                )} UNIQUE (${escapedNewName})`,
                            ),
                        )
                    }
                }
            }

            // isGenerated changes are handled by the drop+recreate path above,
            // so this block is intentionally empty.

            if (newColumn.default !== oldColumn.default) {
                if (
                    newColumn.default !== null &&
                    newColumn.default !== undefined
                ) {
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ALTER COLUMN ${escapedNewName} SET DEFAULT ${newColumn.default}`,
                        ),
                    )

                    if (
                        oldColumn.default !== null &&
                        oldColumn.default !== undefined
                    ) {
                        downQueries.push(
                            new Query(
                                `ALTER TABLE ${escapedTable} ALTER COLUMN ${escapedNewName} SET DEFAULT ${oldColumn.default}`,
                            ),
                        )
                    } else {
                        downQueries.push(
                            new Query(
                                `ALTER TABLE ${escapedTable} ALTER COLUMN ${escapedNewName} DROP DEFAULT`,
                            ),
                        )
                    }
                } else if (
                    oldColumn.default !== null &&
                    oldColumn.default !== undefined
                ) {
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ALTER COLUMN ${escapedNewName} DROP DEFAULT`,
                        ),
                    )
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ALTER COLUMN ${escapedNewName} SET DEFAULT ${oldColumn.default}`,
                        ),
                    )
                }
            }

            if (newColumn.generatedType !== oldColumn.generatedType) {
                // Convert generated column data to normal column
                if (
                    !newColumn.generatedType ||
                    newColumn.generatedType === "VIRTUAL"
                ) {
                    // We can copy the generated data to the new column
                    const { schema, tableName } =
                        await this.parseTableNameWithSchema(table.name)

                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} RENAME COLUMN ${escapedNewName} TO ${this.escapeIdentifier(
                                `TEMP_OLD_${oldColumn.name}`,
                            )}`,
                        ),
                    )
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ADD ${this.buildCreateColumnSql(
                                table,
                                newColumn,
                            )}`,
                        ),
                    )
                    upQueries.push(
                        new Query(
                            `UPDATE ${escapedTable} SET ${escapedNewName} = ${this.escapeIdentifier(
                                `TEMP_OLD_${oldColumn.name}`,
                            )}`,
                        ),
                    )
                    upQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} DROP COLUMN ${this.escapeIdentifier(
                                `TEMP_OLD_${oldColumn.name}`,
                            )}`,
                        ),
                    )
                    upQueries.push(
                        this.deleteTypeormMetadataSql({
                            database: this.driver.database,
                            schema,
                            table: tableName,
                            type: MetadataTableType.GENERATED_COLUMN,
                            name: oldColumn.name,
                        }),
                    )
                    // However, we can't copy it back on downgrade. It needs to regenerate.
                    downQueries.push(
                        this.insertTypeormMetadataSql({
                            database: this.driver.database,
                            schema,
                            table: tableName,
                            type: MetadataTableType.GENERATED_COLUMN,
                            name: oldColumn.name,
                            value: oldColumn.asExpression,
                        }),
                    )
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} ADD ${this.buildCreateColumnSql(
                                table,
                                oldColumn,
                            )}`,
                        ),
                    )
                    downQueries.push(
                        new Query(
                            `ALTER TABLE ${escapedTable} DROP COLUMN ${escapedNewName}`,
                        ),
                    )
                }
            }
        }

        // Sync all changed properties from newColumn onto the clonedTable
        // column, so the cached table state accurately reflects the DB state
        // after the ALTER statements above.
        const syncTarget = clonedTable.columns.find(
            (c) => c.name === newColumn.name,
        )
        if (syncTarget) {
            syncTarget.type = newColumn.type
            syncTarget.length = newColumn.length
            syncTarget.precision = newColumn.precision
            syncTarget.scale = newColumn.scale
            syncTarget.isNullable = newColumn.isNullable
            syncTarget.default = newColumn.default
            syncTarget.comment = newColumn.comment
            syncTarget.collation = newColumn.collation
            syncTarget.isGenerated = newColumn.isGenerated
            syncTarget.generationStrategy = newColumn.generationStrategy
            syncTarget.generatedType = newColumn.generatedType
            syncTarget.asExpression = newColumn.asExpression
        }

        await this.executeQueries(upQueries, downQueries)
        this.replaceCachedTable(table, clonedTable)
    }

    /**
     * Changes multiple columns in the table sequentially.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param changedColumns - Array of `{ oldColumn, newColumn }` pairs describing the changes.
     * @returns Resolves when all columns have been altered.
     */
    async changeColumns(
        tableOrName: Table | string,
        changedColumns: { newColumn: TableColumn; oldColumn: TableColumn }[],
    ): Promise<void> {
        for (const { oldColumn, newColumn } of changedColumns) {
            await this.changeColumn(tableOrName, oldColumn, newColumn)
        }
    }

    /**
     * Drops the column in the table.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param columnOrName - The {@link TableColumn} or column name string to drop.
     * @returns Resolves when the column and any dependent constraints have been dropped.
     */
    async dropColumn(
        tableOrName: Table | string,
        columnOrName: TableColumn | string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const column = InstanceChecker.isTableColumn(columnOrName)
            ? columnOrName
            : table.findColumnByName(columnOrName)
        if (!column)
            throw new TypeORMError(
                `Column "${columnOrName}" was not found in table "${table.name}"`,
            )

        const clonedTable = table.clone()
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        // drop primary key constraint
        if (column.isPrimary) {
            const pkName = this.resolvePrimaryKeyName(
                clonedTable,
                clonedTable.primaryColumns,
            )

            const columnNamesSql = this.quoteColumns(
                clonedTable.primaryColumns.map(
                    (primaryColumn) => primaryColumn.name,
                ),
            )

            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        clonedTable,
                    )} DROP CONSTRAINT ${this.escapeIdentifier(pkName)}`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        clonedTable,
                    )} ADD CONSTRAINT ${this.escapeIdentifier(
                        pkName,
                    )} PRIMARY KEY (${columnNamesSql})`,
                ),
            )

            // update column in table
            const tableColumn = clonedTable.findColumnByName(column.name)
            tableColumn!.isPrimary = false

            // if primary key have multiple columns, we must recreate it without dropped column
            if (clonedTable.primaryColumns.length > 0) {
                const pkName = this.resolvePrimaryKeyName(
                    clonedTable,
                    clonedTable.primaryColumns,
                )

                const columnNamesSql = this.quoteColumns(
                    clonedTable.primaryColumns.map(
                        (primaryColumn) => primaryColumn.name,
                    ),
                )

                upQueries.push(
                    new Query(
                        `ALTER TABLE ${this.escapePath(
                            clonedTable,
                        )} ADD CONSTRAINT ${this.escapeIdentifier(
                            pkName,
                        )} PRIMARY KEY (${columnNamesSql})`,
                    ),
                )
                downQueries.push(
                    new Query(
                        `ALTER TABLE ${this.escapePath(
                            clonedTable,
                        )} DROP CONSTRAINT ${this.escapeIdentifier(pkName)}`,
                    ),
                )
            }
        }

        // Snowflake does not support user-created indexes — skip column index drop

        // drop column check
        const columnCheck = clonedTable.checks.find(
            (check) =>
                !!check.columnNames &&
                check.columnNames.length === 1 &&
                check.columnNames[0] === column.name,
        )
        if (columnCheck) {
            clonedTable.checks.splice(
                clonedTable.checks.indexOf(columnCheck),
                1,
            )
            upQueries.push(this.dropCheckConstraintSql(table, columnCheck))
            downQueries.push(this.createCheckConstraintSql(table, columnCheck))
        }

        // drop column unique
        const columnUnique = clonedTable.uniques.find(
            (unique) =>
                unique.columnNames.length === 1 &&
                unique.columnNames[0] === column.name,
        )
        if (columnUnique) {
            clonedTable.uniques.splice(
                clonedTable.uniques.indexOf(columnUnique),
                1,
            )
            upQueries.push(this.dropUniqueConstraintSql(table, columnUnique))
            downQueries.push(
                this.createUniqueConstraintSql(table, columnUnique),
            )
        }

        upQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    table,
                )} DROP COLUMN ${this.escapeIdentifier(column.name)}`,
            ),
        )
        downQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    table,
                )} ADD ${this.buildCreateColumnSql(table, column)}`,
            ),
        )

        if (column.comment) {
            downQueries.push(
                new Query(
                    `COMMENT ON COLUMN ${this.escapePath(
                        table,
                    )}.${this.escapeIdentifier(
                        column.name,
                    )} IS ${this.escapeComment(column.comment)}`,
                ),
            )
        }

        if (column.generatedType === "STORED") {
            const { schema, tableName } = await this.parseTableNameWithSchema(
                table.name,
            )
            const deleteQuery = this.deleteTypeormMetadataSql({
                database: this.driver.database,
                schema,
                table: tableName,
                type: MetadataTableType.GENERATED_COLUMN,
                name: column.name,
            })
            const insertQuery = this.insertTypeormMetadataSql({
                database: this.driver.database,
                schema,
                table: tableName,
                type: MetadataTableType.GENERATED_COLUMN,
                name: column.name,
                value: column.asExpression,
            })

            upQueries.push(deleteQuery)
            downQueries.push(insertQuery)
        }

        await this.executeQueries(upQueries, downQueries)

        clonedTable.removeColumn(column)
        this.replaceCachedTable(table, clonedTable)
    }

    /**
     * Drops the columns in the table.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param columns - Array of {@link TableColumn} instances or column name strings to drop.
     * @returns Resolves when all columns have been dropped.
     */
    async dropColumns(
        tableOrName: Table | string,
        columns: TableColumn[] | string[],
    ): Promise<void> {
        for (const column of columns) {
            await this.dropColumn(tableOrName, column)
        }
    }

    /**
     * Creates a new primary key.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param columnNames - Column names that form the primary key.
     * @param constraintName - Optional explicit constraint name.
     * @returns Resolves when the primary key has been created.
     */
    async createPrimaryKey(
        tableOrName: Table | string,
        columnNames: string[],
        constraintName?: string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const clonedTable = table.clone()

        const up = this.createPrimaryKeySql(table, columnNames, constraintName)

        // mark columns as primary, because dropPrimaryKeySql build constraint name from table primary column names.
        clonedTable.columns.forEach((column) => {
            if (columnNames.includes(column.name)) column.isPrimary = true
        })
        const down = this.dropPrimaryKeySql(clonedTable)

        await this.executeQueries(up, down)
        this.replaceCachedTable(table, clonedTable)
    }

    /**
     * Updates composite primary keys.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param columns - The {@link TableColumn} definitions that should form the new primary key.
     * @returns Resolves when the primary key has been updated.
     */
    async updatePrimaryKeys(
        tableOrName: Table | string,
        columns: TableColumn[],
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const clonedTable = table.clone()
        const columnNames = columns.map((column) => column.name)
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        // if table already have primary columns, we must drop them.
        const primaryColumns = clonedTable.primaryColumns
        if (primaryColumns.length > 0) {
            const pkName = this.resolvePrimaryKeyName(
                clonedTable,
                primaryColumns,
            )

            const columnNamesSql = this.quoteColumns(
                primaryColumns.map((column) => column.name),
            )

            upQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        table,
                    )} DROP CONSTRAINT ${this.escapeIdentifier(pkName)}`,
                ),
            )
            downQueries.push(
                new Query(
                    `ALTER TABLE ${this.escapePath(
                        table,
                    )} ADD CONSTRAINT ${this.escapeIdentifier(
                        pkName,
                    )} PRIMARY KEY (${columnNamesSql})`,
                ),
            )
        }

        // update columns in table.
        // First, clear isPrimary on ALL columns so stale flags don't remain.
        clonedTable.columns.forEach((column) => (column.isPrimary = false))
        // Then mark only the new primary columns.
        clonedTable.columns
            .filter((column) => columnNames.includes(column.name))
            .forEach((column) => (column.isPrimary = true))

        // Use the NEW columns (not oldPrimaryColumns) for the constraint name
        // so that an explicit primaryKeyConstraintName on the incoming columns
        // is respected rather than the old (now-dropped) primary key's name.
        const pkName = columns[0]?.primaryKeyConstraintName
            ? columns[0].primaryKeyConstraintName
            : this.connection.namingStrategy.primaryKeyName(
                  clonedTable,
                  columnNames,
              )

        const columnNamesSql = this.quoteColumns(columnNames)

        upQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    table,
                )} ADD CONSTRAINT ${this.escapeIdentifier(
                    pkName,
                )} PRIMARY KEY (${columnNamesSql})`,
            ),
        )
        downQueries.push(
            new Query(
                `ALTER TABLE ${this.escapePath(
                    table,
                )} DROP CONSTRAINT ${this.escapeIdentifier(pkName)}`,
            ),
        )

        await this.executeQueries(upQueries, downQueries)
        this.replaceCachedTable(table, clonedTable)
    }

    /**
     * Drops a primary key.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param constraintName - Optional constraint name to use in the down query.
     * @returns Resolves when the primary key has been dropped.
     */
    async dropPrimaryKey(
        tableOrName: Table | string,
        constraintName?: string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const clonedTable = table.clone()
        const up = this.dropPrimaryKeySql(clonedTable)
        const down = this.createPrimaryKeySql(
            clonedTable,
            clonedTable.primaryColumns.map((column) => column.name),
            constraintName,
        )
        await this.executeQueries(up, down)
        clonedTable.primaryColumns.forEach((column) => {
            column.isPrimary = false
        })
        this.replaceCachedTable(table, clonedTable)
    }

    /**
     * Creates a new unique constraint.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param uniqueConstraint - The {@link TableUnique} definition to create.
     * @returns Resolves when the unique constraint has been created.
     */
    async createUniqueConstraint(
        tableOrName: Table | string,
        uniqueConstraint: TableUnique,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)

        // new unique constraint may be passed without name. In this case we generate unique name manually.
        if (!uniqueConstraint.name)
            uniqueConstraint.name =
                this.connection.namingStrategy.uniqueConstraintName(
                    table,
                    uniqueConstraint.columnNames,
                )

        const up = this.createUniqueConstraintSql(table, uniqueConstraint)
        const down = this.dropUniqueConstraintSql(table, uniqueConstraint)
        await this.executeQueries(up, down)
        table.addUniqueConstraint(uniqueConstraint)
    }

    /**
     * Creates new unique constraints.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param uniqueConstraints - Array of {@link TableUnique} definitions to create.
     * @returns Resolves when all unique constraints have been created.
     */
    async createUniqueConstraints(
        tableOrName: Table | string,
        uniqueConstraints: TableUnique[],
    ): Promise<void> {
        for (const uniqueConstraint of uniqueConstraints) {
            await this.createUniqueConstraint(tableOrName, uniqueConstraint)
        }
    }

    /**
     * Drops unique constraint.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param uniqueOrName - The {@link TableUnique} instance or constraint name string to drop.
     * @returns Resolves when the unique constraint has been dropped.
     */
    async dropUniqueConstraint(
        tableOrName: Table | string,
        uniqueOrName: TableUnique | string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const uniqueConstraint = InstanceChecker.isTableUnique(uniqueOrName)
            ? uniqueOrName
            : table.uniques.find((u) => u.name === uniqueOrName)

        if (!uniqueConstraint)
            throw new TypeORMError(
                `Supplied unique constraint was not found in table ${table.name}`,
            )

        const up = this.dropUniqueConstraintSql(table, uniqueConstraint)
        const down = this.createUniqueConstraintSql(table, uniqueConstraint)
        await this.executeQueries(up, down)
        table.removeUniqueConstraint(uniqueConstraint)
    }

    /**
     * Drops unique constraints.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param uniqueConstraints - Array of {@link TableUnique} definitions to drop.
     * @returns Resolves when all unique constraints have been dropped.
     */
    async dropUniqueConstraints(
        tableOrName: Table | string,
        uniqueConstraints: TableUnique[],
    ): Promise<void> {
        for (const uniqueConstraint of uniqueConstraints) {
            await this.dropUniqueConstraint(tableOrName, uniqueConstraint)
        }
    }

    /**
     * Creates a new check constraint.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param checkConstraint - The {@link TableCheck} definition to create.
     * @returns Resolves when the check constraint has been created.
     */
    async createCheckConstraint(
        tableOrName: Table | string,
        checkConstraint: TableCheck,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)

        // new check constraint may be passed without name. In this case we generate check name manually.
        if (!checkConstraint.name)
            checkConstraint.name =
                this.connection.namingStrategy.checkConstraintName(
                    table,
                    checkConstraint.expression!,
                )

        const up = this.createCheckConstraintSql(table, checkConstraint)
        const down = this.dropCheckConstraintSql(table, checkConstraint)
        await this.executeQueries(up, down)
        table.addCheckConstraint(checkConstraint)
    }

    /**
     * Creates new check constraints.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param checkConstraints - Array of {@link TableCheck} definitions to create.
     * @returns Resolves when all check constraints have been created.
     */
    async createCheckConstraints(
        tableOrName: Table | string,
        checkConstraints: TableCheck[],
    ): Promise<void> {
        for (const checkConstraint of checkConstraints) {
            await this.createCheckConstraint(tableOrName, checkConstraint)
        }
    }

    /**
     * Drops check constraint.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param checkOrName - The {@link TableCheck} instance or constraint name string to drop.
     * @returns Resolves when the check constraint has been dropped.
     */
    async dropCheckConstraint(
        tableOrName: Table | string,
        checkOrName: TableCheck | string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const checkConstraint = InstanceChecker.isTableCheck(checkOrName)
            ? checkOrName
            : table.checks.find((c) => c.name === checkOrName)

        if (!checkConstraint)
            throw new TypeORMError(
                `Supplied check constraint was not found in table ${table.name}`,
            )

        const up = this.dropCheckConstraintSql(table, checkConstraint)
        const down = this.createCheckConstraintSql(table, checkConstraint)
        await this.executeQueries(up, down)
        table.removeCheckConstraint(checkConstraint)
    }

    /**
     * Drops check constraints.
     *
     * @param tableOrName - The target {@link Table} instance or table name string.
     * @param checkConstraints - Array of {@link TableCheck} definitions to drop.
     * @returns Resolves when all check constraints have been dropped.
     */
    async dropCheckConstraints(
        tableOrName: Table | string,
        checkConstraints: TableCheck[],
    ): Promise<void> {
        for (const checkConstraint of checkConstraints) {
            await this.dropCheckConstraint(tableOrName, checkConstraint)
        }
    }

    /**
     * Creates a new exclusion constraint.
     *
     * Snowflake does not support exclusion constraints.
     * This method always throws a {@link TypeORMError}.
     *
     * @param tableOrName - Unused. Present for interface compatibility.
     * @param exclusionConstraint - Unused. Present for interface compatibility.
     * @throws {TypeORMError} Always — exclusion constraints are not supported.
     */
    async createExclusionConstraint(
        _tableOrName: Table | string,
        _exclusionConstraint: TableExclusion,
    ): Promise<void> {
        throw new TypeORMError(
            `Snowflake does not support exclusion constraints.`,
        )
    }

    /**
     * Creates new exclusion constraints.
     *
     * Snowflake does not support exclusion constraints.
     * Each call delegates to {@link createExclusionConstraint} which throws.
     *
     * @param tableOrName - Unused. Present for interface compatibility.
     * @param exclusionConstraints - Unused. Present for interface compatibility.
     * @throws {TypeORMError} Always — exclusion constraints are not supported.
     */
    async createExclusionConstraints(
        _tableOrName: Table | string,
        _exclusionConstraints: TableExclusion[],
    ): Promise<void> {
        throw new TypeORMError(
            `Snowflake does not support exclusion constraints.`,
        )
    }

    /**
     * Drops exclusion constraint.
     *
     * Snowflake does not support exclusion constraints.
     * This method always throws a {@link TypeORMError}.
     *
     * @param tableOrName - Unused. Present for interface compatibility.
     * @param exclusionOrName - Unused. Present for interface compatibility.
     * @throws {TypeORMError} Always — exclusion constraints are not supported.
     */
    async dropExclusionConstraint(
        _tableOrName: Table | string,
        _exclusionOrName: TableExclusion | string,
    ): Promise<void> {
        throw new TypeORMError(
            `Snowflake does not support exclusion constraints.`,
        )
    }

    /**
     * Drops exclusion constraints.
     *
     * Snowflake does not support exclusion constraints.
     * Each call delegates to {@link dropExclusionConstraint} which throws.
     *
     * @param tableOrName - Unused. Present for interface compatibility.
     * @param exclusionConstraints - Unused. Present for interface compatibility.
     * @throws {TypeORMError} Always — exclusion constraints are not supported.
     */
    async dropExclusionConstraints(
        _tableOrName: Table | string,
        _exclusionConstraints: TableExclusion[],
    ): Promise<void> {
        throw new TypeORMError(
            `Snowflake does not support exclusion constraints.`,
        )
    }

    /**
     * Creates a new foreign key.
     *
     * @param tableOrName - Target table or its name.
     * @param foreignKey - Foreign key definition to create. If `name` is omitted it will be generated.
     * @returns Resolves when the foreign key has been created.
     */
    async createForeignKey(
        tableOrName: Table | string,
        foreignKey: TableForeignKey,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)

        // new FK may be passed without name. In this case we generate FK name manually.
        if (!foreignKey.name)
            foreignKey.name = this.connection.namingStrategy.foreignKeyName(
                table,
                foreignKey.columnNames,
                this.getTablePath(foreignKey),
                foreignKey.referencedColumnNames,
            )

        const up = this.createForeignKeySql(table, foreignKey)
        const down = this.dropForeignKeySql(table, foreignKey)
        await this.executeQueries(up, down)
        table.addForeignKey(foreignKey)
    }

    /**
     * Creates new foreign keys.
     *
     * @param tableOrName - Target table or its name.
     * @param foreignKeys - Array of foreign key definitions to create.
     * @returns Resolves when all foreign keys have been created.
     */
    async createForeignKeys(
        tableOrName: Table | string,
        foreignKeys: TableForeignKey[],
    ): Promise<void> {
        for (const foreignKey of foreignKeys) {
            await this.createForeignKey(tableOrName, foreignKey)
        }
    }

    /**
     * Drops a foreign key from the table.
     *
     * @param tableOrName - Target table or its name.
     * @param foreignKeyOrName - Foreign key instance or constraint name to drop.
     * @returns Resolves when the foreign key has been dropped.
     */
    async dropForeignKey(
        tableOrName: Table | string,
        foreignKeyOrName: TableForeignKey | string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)
        const foreignKey = InstanceChecker.isTableForeignKey(foreignKeyOrName)
            ? foreignKeyOrName
            : table.foreignKeys.find((fk) => fk.name === foreignKeyOrName)

        if (!foreignKey)
            throw new TypeORMError(
                `Supplied foreign key was not found in table ${table.name}`,
            )

        const up = this.dropForeignKeySql(table, foreignKey)
        const down = this.createForeignKeySql(table, foreignKey)
        await this.executeQueries(up, down)
        table.removeForeignKey(foreignKey)
    }

    /**
     * Drops foreign keys from the table.
     *
     * @param tableOrName - Target table or its name.
     * @param foreignKeys - Array of foreign key definitions to drop.
     * @returns Resolves when all foreign keys have been dropped.
     */
    async dropForeignKeys(
        tableOrName: Table | string,
        foreignKeys: TableForeignKey[],
    ): Promise<void> {
        for (const foreignKey of foreignKeys) {
            await this.dropForeignKey(tableOrName, foreignKey)
        }
    }

    /**
     * Creates a new index.
     * Note: Snowflake does not support user-created indexes (it uses automatic micro-partitioning).
     * This is a no-op for compatibility.
     *
     * @param tableOrName - Unused. Present for interface compatibility.
     * @param index - Unused. Present for interface compatibility.
     * @returns Resolves immediately (no-op).
     */
    async createIndex(
        tableOrName: Table | string,
        index: TableIndex,
    ): Promise<void> {
        // Snowflake does not support user-created indexes — no-op
    }

    /**
     * Creates a new view index.
     * Note: Snowflake does not support user-created indexes.
     *
     * @param viewOrName - Unused. Present for interface compatibility.
     * @param index - Unused. Present for interface compatibility.
     * @returns Resolves immediately (no-op).
     */
    async createViewIndex(
        viewOrName: View | string,
        index: TableIndex,
    ): Promise<void> {
        // Snowflake does not support user-created indexes — no-op
    }

    /**
     * Creates new indices.
     * Note: Snowflake does not support user-created indexes. No-op.
     *
     * @param tableOrName - Unused. Present for interface compatibility.
     * @param indices - Unused. Present for interface compatibility.
     * @returns Resolves immediately (no-op).
     */
    async createIndices(
        tableOrName: Table | string,
        indices: TableIndex[],
    ): Promise<void> {
        // Snowflake does not support user-created indexes — no-op
    }

    /**
     * Creates new view indices.
     * Note: Snowflake does not support user-created indexes. No-op.
     *
     * @param viewOrName - Unused. Present for interface compatibility.
     * @param indices - Unused. Present for interface compatibility.
     * @returns Resolves immediately (no-op).
     */
    async createViewIndices(
        viewOrName: View | string,
        indices: TableIndex[],
    ): Promise<void> {
        // Snowflake does not support user-created indexes — no-op
    }

    /**
     * Drops an index.
     * Note: Snowflake does not support user-created indexes. No-op.
     *
     * @param tableOrName - Unused. Present for interface compatibility.
     * @param indexOrName - Unused. Present for interface compatibility.
     * @returns Resolves immediately (no-op).
     */
    async dropIndex(
        tableOrName: Table | string,
        indexOrName: TableIndex | string,
    ): Promise<void> {
        // Snowflake does not support user-created indexes — no-op
    }

    /**
     * Drops a view index.
     * Note: Snowflake does not support user-created indexes. No-op.
     *
     * @param viewOrName - Unused. Present for interface compatibility.
     * @param indexOrName - Unused. Present for interface compatibility.
     * @returns Resolves immediately (no-op).
     */
    async dropViewIndex(
        viewOrName: View | string,
        indexOrName: TableIndex | string,
    ): Promise<void> {
        // Snowflake does not support user-created indexes — no-op
    }

    /**
     * Drops indices.
     * Note: Snowflake does not support user-created indexes. No-op.
     *
     * @param tableOrName - Unused. Present for interface compatibility.
     * @param indices - Unused. Present for interface compatibility.
     * @returns Resolves immediately (no-op).
     */
    async dropIndices(
        tableOrName: Table | string,
        indices: TableIndex[],
    ): Promise<void> {
        // Snowflake does not support user-created indexes — no-op
    }

    /**
     * Clears all table contents.
     *
     * @param tableName - Name of the table to truncate.
     * @returns Resolves when the table has been truncated.
     */
    async clearTable(tableName: string): Promise<void> {
        await this.query(`TRUNCATE TABLE ${this.escapePath(tableName)}`)
    }

    /**
     * Removes all tables from the currently connected database.
     * Uses Snowflake's INFORMATION_SCHEMA.
     *
     * @returns Resolves when all views and tables have been dropped.
     */
    async clearDatabase(): Promise<void> {
        const schemaSet = new Set<string>(
            this.connection.entityMetadatas
                .filter((metadata) => metadata.schema)
                .map((metadata) => metadata.schema!),
        )

        const currentSchema = await this.getCurrentSchema()
        schemaSet.add(this.driver.options.schema ?? currentSchema)

        const uniqueSchemas = [...schemaSet]

        const isAnotherTransactionActive = this.isTransactionActive
        if (!isAnotherTransactionActive) await this.startTransaction()
        try {
            for (const schema of uniqueSchemas) {
                const escapedSchema = this.escapeIdentifier(schema)

                // drop views
                const viewResults = await this.query(
                    `SELECT "TABLE_NAME" FROM "INFORMATION_SCHEMA"."VIEWS" WHERE "TABLE_SCHEMA" = :1`,
                    [schema],
                )
                for (const row of viewResults) {
                    await this.query(
                        `DROP VIEW IF EXISTS ${escapedSchema}.${this.escapeIdentifier(
                            row["TABLE_NAME"],
                        )}`,
                    )
                }

                // drop tables
                const tableResults = await this.query(
                    `SELECT "TABLE_NAME" FROM "INFORMATION_SCHEMA"."TABLES" WHERE "TABLE_SCHEMA" = :1 AND "TABLE_TYPE" = 'BASE TABLE'`,
                    [schema],
                )
                for (const row of tableResults) {
                    await this.query(
                        `DROP TABLE IF EXISTS ${escapedSchema}.${this.escapeIdentifier(
                            row["TABLE_NAME"],
                        )} CASCADE`,
                    )
                }
            }

            // Snowflake has no user-defined ENUM types — skip dropEnumTypes

            if (!isAnotherTransactionActive) {
                await this.commitTransaction()
            }
        } catch (error) {
            try {
                if (!isAnotherTransactionActive) {
                    await this.rollbackTransaction()
                }
            } catch (rollbackError) {
                this.driver.connection.logger.log(
                    "warn",
                    `clearDatabase rollback failed: ${
                        rollbackError instanceof Error
                            ? rollbackError.message
                            : rollbackError
                    }`,
                )
            }
            throw error
        }
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * Loads views from the database, using the typeorm metadata table.
     * Uses typeorm metadata table only.
     *
     * @param viewNames - Optional list of view names to load. Loads all views if omitted.
     * @returns Array of {@link View} instances populated from the metadata table.
     */
    protected async loadViews(viewNames?: string[]): Promise<View[]> {
        const hasTable = await this.hasTable(this.getTypeormMetadataTableName())

        if (!hasTable) return []

        if (!viewNames) {
            viewNames = []
        }

        const [{ currentDatabase, currentSchema }] = await this.query(
            `SELECT CURRENT_DATABASE() AS "currentDatabase", CURRENT_SCHEMA() AS "currentSchema"`,
        )
        const viewParams: any[] = []
        const viewsCondition =
            viewNames.length === 0
                ? "1=1"
                : viewNames
                      .map((tableName) => this.driver.parseTableName(tableName))
                      .map(({ schema, tableName }) => {
                          const effectiveSchema =
                              schema ||
                              this.driver.options.schema ||
                              currentSchema
                          viewParams.push(effectiveSchema, tableName)
                          return `("t"."schema" = :${
                              viewParams.length - 1
                          } AND "t"."name" = :${viewParams.length})`
                      })
                      .join(" OR ")

        const query =
            `SELECT "t".* FROM ${this.escapePath(
                this.getTypeormMetadataTableName(),
            )} "t" ` +
            `WHERE "t"."type" IN ('${MetadataTableType.VIEW}', '${MetadataTableType.MATERIALIZED_VIEW}') AND (${viewsCondition})`

        const dbViews = await this.query(query, viewParams)

        return dbViews.map((dbView: any) => {
            const view = new View()
            const schema =
                dbView["schema"] === currentSchema &&
                !this.driver.options.schema
                    ? undefined
                    : dbView["schema"]
            view.database = currentDatabase
            view.schema = dbView["schema"]
            view.name = this.driver.buildTableName(dbView["name"], schema)
            view.expression = dbView["value"]
            view.materialized =
                dbView["type"] === MetadataTableType.MATERIALIZED_VIEW
            // Snowflake does not support user-created indexes on views
            view.indices = []
            return view
        })
    }

    /**
     * Loads all tables (with given names) from the database and creates a Table from them.
     * Uses Snowflake INFORMATION_SCHEMA and SHOW commands.
     *
     * @param tableNames - Optional list of table names to load. Loads all tables if omitted.
     * @returns Array of {@link Table} instances fully populated with columns, constraints, and keys.
     */
    protected async loadTables(tableNames?: string[]): Promise<Table[]> {
        // if no tables given then no need to proceed
        if (tableNames && tableNames.length === 0) {
            return []
        }

        const [{ currentSchema, currentDatabase }] = await this.query(
            `SELECT CURRENT_SCHEMA() AS "currentSchema", CURRENT_DATABASE() AS "currentDatabase"`,
        )

        const dbTables: {
            TABLE_SCHEMA: string
            TABLE_NAME: string
            COMMENT: string | null
        }[] = []

        if (!tableNames) {
            const tablesSql = `SELECT "TABLE_SCHEMA", "TABLE_NAME", "COMMENT" FROM "INFORMATION_SCHEMA"."TABLES" WHERE "TABLE_TYPE" = 'BASE TABLE'`
            dbTables.push(...(await this.query(tablesSql)))
        } else {
            const tableParams: any[] = []
            const tablesCondition = tableNames
                .map((tableName) => this.driver.parseTableName(tableName))
                .map(({ schema, tableName }) => {
                    const effectiveSchema = schema || currentSchema
                    tableParams.push(effectiveSchema, tableName)
                    return `("TABLE_SCHEMA" = :${
                        tableParams.length - 1
                    } AND "TABLE_NAME" = :${tableParams.length})`
                })
                .join(" OR ")

            const tablesSql =
                `SELECT "TABLE_SCHEMA", "TABLE_NAME", "COMMENT" FROM "INFORMATION_SCHEMA"."TABLES" WHERE "TABLE_TYPE" = 'BASE TABLE' AND (` +
                tablesCondition +
                `)`
            dbTables.push(...(await this.query(tablesSql, tableParams)))
        }

        // if tables were not found in the db, no need to proceed
        if (dbTables.length === 0) {
            return []
        }

        // load columns from INFORMATION_SCHEMA.COLUMNS
        const columnsParams: any[] = []
        const columnsCondition = dbTables
            .map(({ TABLE_SCHEMA, TABLE_NAME }) => {
                columnsParams.push(TABLE_SCHEMA, TABLE_NAME)
                return `("TABLE_SCHEMA" = :${
                    columnsParams.length - 1
                } AND "TABLE_NAME" = :${columnsParams.length})`
            })
            .join(" OR ")
        const columnsSql =
            `SELECT * FROM "INFORMATION_SCHEMA"."COLUMNS" WHERE ` +
            columnsCondition +
            ` ORDER BY "ORDINAL_POSITION"`

        const dbColumns: ObjectLiteral[] = await this.query(
            columnsSql,
            columnsParams,
        )

        // load primary keys, foreign keys, and unique keys in parallel
        const [dbPrimaryKeys, dbForeignKeys, dbUniqueKeys] = await Promise.all([
            // primary keys
            Promise.all(
                dbTables.map(async (dbTable) => {
                    try {
                        const pkResults = await this.query(
                            `SHOW PRIMARY KEYS IN TABLE ${this.escapeIdentifier(
                                dbTable.TABLE_SCHEMA,
                            )}.${this.escapeIdentifier(dbTable.TABLE_NAME)}`,
                        )
                        return pkResults.map((pk: any) => ({
                            table_schema: pk["schema_name"],
                            table_name: pk["table_name"],
                            column_name: pk["column_name"],
                            constraint_name: pk["constraint_name"],
                        }))
                    } catch (e) {
                        this.driver.connection.logger.log(
                            "warn",
                            `Failed to load primary keys for ${
                                dbTable.TABLE_SCHEMA
                            }.${dbTable.TABLE_NAME}: ${
                                e instanceof Error ? e.message : e
                            }`,
                        )
                        return []
                    }
                }),
            ).then((results) => results.flat()),
            // foreign keys
            Promise.all(
                dbTables.map(async (dbTable) => {
                    try {
                        const fkResults = await this.query(
                            `SHOW IMPORTED KEYS IN TABLE ${this.escapeIdentifier(
                                dbTable.TABLE_SCHEMA,
                            )}.${this.escapeIdentifier(dbTable.TABLE_NAME)}`,
                        )
                        return fkResults.map((fk: any) => ({
                            constraint_name: fk["fk_name"],
                            table_schema: fk["fk_schema_name"],
                            table_name: fk["fk_table_name"],
                            column_name: fk["fk_column_name"],
                            referenced_table_schema: fk["pk_schema_name"],
                            referenced_table_name: fk["pk_table_name"],
                            referenced_column_name: fk["pk_column_name"],
                            on_delete: fk["delete_rule"],
                            // Snowflake does not support ON UPDATE — omit to avoid phantom diffs
                        }))
                    } catch (e) {
                        this.driver.connection.logger.log(
                            "warn",
                            `Failed to load foreign keys for ${
                                dbTable.TABLE_SCHEMA
                            }.${dbTable.TABLE_NAME}: ${
                                e instanceof Error ? e.message : e
                            }`,
                        )
                        return []
                    }
                }),
            ).then((results) => results.flat()),
            // unique keys
            Promise.all(
                dbTables.map(async (dbTable) => {
                    try {
                        const ukResults = await this.query(
                            `SHOW UNIQUE KEYS IN TABLE ${this.escapeIdentifier(
                                dbTable.TABLE_SCHEMA,
                            )}.${this.escapeIdentifier(dbTable.TABLE_NAME)}`,
                        )
                        return ukResults.map((uk: any) => ({
                            constraint_name: uk["constraint_name"],
                            table_schema: uk["schema_name"],
                            table_name: uk["table_name"],
                            column_name: uk["column_name"],
                        }))
                    } catch (e) {
                        this.driver.connection.logger.log(
                            "warn",
                            `Failed to load unique keys for ${
                                dbTable.TABLE_SCHEMA
                            }.${dbTable.TABLE_NAME}: ${
                                e instanceof Error ? e.message : e
                            }`,
                        )
                        return []
                    }
                }),
            ).then((results) => results.flat()),
        ])

        // load check constraints — Snowflake's CHECK_CONSTRAINTS doesn't have TABLE_NAME/TABLE_SCHEMA,
        // so we JOIN with TABLE_CONSTRAINTS to get the table association.
        const checkConstraintsParams: any[] = []
        const checkConstraintsCondition = dbTables
            .map(({ TABLE_SCHEMA, TABLE_NAME }) => {
                checkConstraintsParams.push(TABLE_SCHEMA, TABLE_NAME)
                return `("tc"."TABLE_SCHEMA" = :${
                    checkConstraintsParams.length - 1
                } AND "tc"."TABLE_NAME" = :${checkConstraintsParams.length})`
            })
            .join(" OR ")
        const checkConstraintsSql =
            `SELECT "cc"."CONSTRAINT_NAME", "tc"."TABLE_SCHEMA", "tc"."TABLE_NAME", "cc"."CHECK_CLAUSE" ` +
            `FROM "INFORMATION_SCHEMA"."CHECK_CONSTRAINTS" "cc" ` +
            `INNER JOIN "INFORMATION_SCHEMA"."TABLE_CONSTRAINTS" "tc" ` +
            `ON "cc"."CONSTRAINT_SCHEMA" = "tc"."CONSTRAINT_SCHEMA" ` +
            `AND "cc"."CONSTRAINT_NAME" = "tc"."CONSTRAINT_NAME" ` +
            `WHERE "tc"."CONSTRAINT_TYPE" = 'CHECK' AND (${checkConstraintsCondition})`
        let dbCheckConstraints: ObjectLiteral[] = []
        try {
            const rawChecks = await this.query(
                checkConstraintsSql,
                checkConstraintsParams,
            )
            // Filter out Snowflake's system-generated NOT NULL constraints
            dbCheckConstraints = rawChecks.filter((cc: ObjectLiteral) => {
                const clause = (cc["CHECK_CLAUSE"] || "").trim()
                // System NOT NULL constraints have CHECK_CLAUSE like: "COLUMN_NAME" IS NOT NULL
                // or: NOT ("COLUMN_NAME" IS NULL)
                return (
                    !/^\(?NOT\s+\(?"[^"]+"\s+IS\s+NULL\)?\)?$/i.test(clause) &&
                    !/"[^"]+"\s+IS\s+NOT\s+NULL/i.test(clause)
                )
            })
        } catch (e) {
            // CHECK_CONSTRAINTS view might not exist in all Snowflake editions
            this.driver.connection.logger.log(
                "warn",
                `Failed to load check constraints: ${
                    e instanceof Error ? e.message : e
                }`,
            )
        }

        // Pre-build Maps keyed by "schema\0table" for O(1) lookups per table
        const tableKey = (schema: string, table: string) =>
            `${schema}\0${table}`
        const colTableKey = (schema: string, table: string, column: string) =>
            `${schema}\0${table}\0${column}`

        const dbColumnsByTable = new Map<string, typeof dbColumns>()
        for (const c of dbColumns) {
            const k = tableKey(c["TABLE_SCHEMA"], c["TABLE_NAME"])
            const arr = dbColumnsByTable.get(k)
            if (arr) arr.push(c)
            else dbColumnsByTable.set(k, [c])
        }

        const dbPkByTable = new Map<string, typeof dbPrimaryKeys>()
        const dbPkByTableCol = new Map<string, (typeof dbPrimaryKeys)[0]>()
        for (const pk of dbPrimaryKeys) {
            const tk = tableKey(pk.table_schema, pk.table_name)
            const arr = dbPkByTable.get(tk)
            if (arr) arr.push(pk)
            else dbPkByTable.set(tk, [pk])
            dbPkByTableCol.set(
                colTableKey(pk.table_schema, pk.table_name, pk.column_name),
                pk,
            )
        }

        const dbUkByTable = new Map<string, typeof dbUniqueKeys>()
        const dbUkByTableCol = new Map<string, typeof dbUniqueKeys>()
        const dbUkByConstraint = new Map<string, typeof dbUniqueKeys>()
        for (const uk of dbUniqueKeys) {
            const tk = tableKey(uk.table_schema, uk.table_name)
            let arr = dbUkByTable.get(tk)
            if (arr) arr.push(uk)
            else dbUkByTable.set(tk, [uk])

            const ck = colTableKey(
                uk.table_schema,
                uk.table_name,
                uk.column_name,
            )
            arr = dbUkByTableCol.get(ck)
            if (arr) arr.push(uk)
            else dbUkByTableCol.set(ck, [uk])

            arr = dbUkByConstraint.get(uk.constraint_name)
            if (arr) arr.push(uk)
            else dbUkByConstraint.set(uk.constraint_name, [uk])
        }

        const dbChecksByTable = new Map<string, typeof dbCheckConstraints>()
        for (const cc of dbCheckConstraints) {
            const k = tableKey(cc["TABLE_SCHEMA"], cc["TABLE_NAME"])
            const arr = dbChecksByTable.get(k)
            if (arr) arr.push(cc)
            else dbChecksByTable.set(k, [cc])
        }

        const dbFkByTable = new Map<string, typeof dbForeignKeys>()
        const dbFkByConstraint = new Map<string, typeof dbForeignKeys>()
        for (const fk of dbForeignKeys) {
            const tk = tableKey(fk.table_schema, fk.table_name)
            let arr = dbFkByTable.get(tk)
            if (arr) arr.push(fk)
            else dbFkByTable.set(tk, [fk])

            arr = dbFkByConstraint.get(fk.constraint_name)
            if (arr) arr.push(fk)
            else dbFkByConstraint.set(fk.constraint_name, [fk])
        }

        // Load generated column expressions from typeorm_metadata table.
        // Snowflake INFORMATION_SCHEMA.COLUMNS does not have IS_GENERATED /
        // GENERATION_EXPRESSION, so we rely on our own metadata table.
        const dbGeneratedColumnsMap = new Map<string, string>()
        const hasMetadataTable = await this.hasTable(
            this.getTypeormMetadataTableName(),
        )
        if (hasMetadataTable) {
            try {
                const genColRows: ObjectLiteral[] = await this.query(
                    `SELECT "schema", "table", "name", "value" FROM ${this.escapePath(
                        this.getTypeormMetadataTableName(),
                    )} WHERE "type" = '${MetadataTableType.GENERATED_COLUMN}'`,
                )
                for (const row of genColRows) {
                    // Key matches the colTableKey used elsewhere
                    const key = `${row["schema"]}\0${row["table"]}\0${row["name"]}`
                    dbGeneratedColumnsMap.set(key, row["value"])
                }
            } catch {
                // typeorm_metadata table might not have the expected columns yet
            }
        }

        // create tables for loaded tables
        return Promise.all(
            dbTables.map(async (dbTable) => {
                const table = new Table()

                const getSchemaFromKey = (dbObject: any, key: string) => {
                    return dbObject[key] === currentSchema &&
                        (!this.driver.options.schema ||
                            this.driver.options.schema === currentSchema)
                        ? undefined
                        : dbObject[key]
                }

                const schema = getSchemaFromKey(dbTable, "TABLE_SCHEMA")
                table.database = currentDatabase
                table.schema = dbTable["TABLE_SCHEMA"]
                table.name = this.driver.buildTableName(
                    dbTable["TABLE_NAME"],
                    schema,
                )
                table.comment = dbTable["COMMENT"] || undefined

                // Fetch column comments via SHOW COLUMNS (INFORMATION_SCHEMA.COLUMNS has no COMMENT column)
                const columnCommentMap = new Map<string, string>()
                try {
                    const showColumnsResult = await this.query(
                        `SHOW COLUMNS IN TABLE ${this.escapeIdentifier(
                            dbTable["TABLE_SCHEMA"],
                        )}.${this.escapeIdentifier(dbTable["TABLE_NAME"])}`,
                    )
                    for (const row of showColumnsResult) {
                        if (row["comment"]) {
                            columnCommentMap.set(
                                row["column_name"],
                                row["comment"],
                            )
                        }
                    }
                } catch (e) {
                    // SHOW COLUMNS might fail if table was dropped concurrently
                    this.driver.connection.logger.log(
                        "warn",
                        `Failed to load column comments for ${
                            dbTable["TABLE_SCHEMA"]
                        }.${dbTable["TABLE_NAME"]}: ${
                            e instanceof Error ? e.message : e
                        }`,
                    )
                }

                // create columns from the loaded columns
                const tk = tableKey(
                    dbTable["TABLE_SCHEMA"],
                    dbTable["TABLE_NAME"],
                )
                table.columns = (dbColumnsByTable.get(tk) ?? []).map(
                    (dbColumn) => {
                        const tableColumn = new TableColumn()
                        tableColumn.name = dbColumn["COLUMN_NAME"]

                        // Snowflake DATA_TYPE is already the normalized type name
                        let rawType = (
                            dbColumn["DATA_TYPE"] || ""
                        ).toLowerCase()

                        // Map Snowflake types to TypeORM types
                        if (rawType === "text") rawType = "varchar"
                        else if (rawType === "fixed") rawType = "number"

                        tableColumn.type = rawType

                        // Handle precision/scale for numeric types
                        if (
                            rawType === "number" ||
                            rawType === "numeric" ||
                            rawType === "decimal" ||
                            rawType === "float"
                        ) {
                            if (
                                dbColumn["NUMERIC_PRECISION"] !== null &&
                                !this.isDefaultColumnPrecision(
                                    table,
                                    tableColumn,
                                    dbColumn["NUMERIC_PRECISION"],
                                )
                            ) {
                                tableColumn.precision =
                                    dbColumn["NUMERIC_PRECISION"]
                            }
                            if (
                                dbColumn["NUMERIC_SCALE"] !== null &&
                                !this.isDefaultColumnScale(
                                    table,
                                    tableColumn,
                                    dbColumn["NUMERIC_SCALE"],
                                )
                            ) {
                                tableColumn.scale = dbColumn["NUMERIC_SCALE"]
                            }
                        }

                        // Handle datetime precision
                        if (
                            rawType === "timestamp_ntz" ||
                            rawType === "timestamp_tz" ||
                            rawType === "timestamp_ltz" ||
                            rawType === "time" ||
                            rawType === "date"
                        ) {
                            if (dbColumn["DATETIME_PRECISION"] !== null) {
                                tableColumn.precision =
                                    !this.isDefaultColumnPrecision(
                                        table,
                                        tableColumn,
                                        dbColumn["DATETIME_PRECISION"],
                                    )
                                        ? dbColumn["DATETIME_PRECISION"]
                                        : undefined
                            }
                        }

                        // check only columns that have length property
                        if (
                            this.driver.withLengthColumnTypes.includes(
                                tableColumn.type as ColumnType,
                            )
                        ) {
                            if (dbColumn["CHARACTER_MAXIMUM_LENGTH"]) {
                                const length =
                                    dbColumn[
                                        "CHARACTER_MAXIMUM_LENGTH"
                                    ].toString()
                                tableColumn.length =
                                    !this.isDefaultColumnLength(
                                        table,
                                        tableColumn,
                                        length,
                                    )
                                        ? length
                                        : ""
                            }
                        }

                        tableColumn.isNullable =
                            dbColumn["IS_NULLABLE"] === "YES"

                        // Check primary key
                        const pkForColumn = dbPkByTableCol.get(
                            colTableKey(
                                dbTable["TABLE_SCHEMA"],
                                dbTable["TABLE_NAME"],
                                dbColumn["COLUMN_NAME"],
                            ),
                        )
                        if (pkForColumn) {
                            tableColumn.isPrimary = true

                            // find all columns involved in primary key constraint
                            const allPkColumns = (
                                dbPkByTable.get(tk) ?? []
                            ).map((pk) => pk.column_name)

                            // build default primary key constraint name
                            const pkName =
                                this.connection.namingStrategy.primaryKeyName(
                                    table,
                                    allPkColumns,
                                )

                            // if primary key has user-defined constraint name, write it in table column
                            if (pkForColumn.constraint_name !== pkName) {
                                tableColumn.primaryKeyConstraintName =
                                    pkForColumn.constraint_name
                            }
                        }

                        // Check unique constraints
                        const uniqueForColumn =
                            dbUkByTableCol.get(
                                colTableKey(
                                    dbTable["TABLE_SCHEMA"],
                                    dbTable["TABLE_NAME"],
                                    dbColumn["COLUMN_NAME"],
                                ),
                            ) ?? []
                        // A column is unique only if it has a single-column unique constraint
                        const isConstraintComposite = uniqueForColumn.every(
                            (uk) => {
                                const members =
                                    dbUkByConstraint.get(uk.constraint_name) ??
                                    []
                                return members.some(
                                    (other) =>
                                        other.column_name !==
                                        dbColumn["COLUMN_NAME"],
                                )
                            },
                        )
                        tableColumn.isUnique =
                            uniqueForColumn.length > 0 && !isConstraintComposite

                        // Handle Snowflake AUTOINCREMENT / IDENTITY
                        if (dbColumn["IS_IDENTITY"] === "YES") {
                            tableColumn.isGenerated = true
                            tableColumn.generationStrategy = "identity"
                            // Snowflake INFORMATION_SCHEMA.COLUMNS does not have IDENTITY_GENERATION;
                            // Snowflake identity is always "ALWAYS" (no BY DEFAULT option).
                            tableColumn.generatedIdentity = "ALWAYS"
                        } else if (
                            dbColumn["COLUMN_DEFAULT"] !== null &&
                            dbColumn["COLUMN_DEFAULT"] !== undefined
                        ) {
                            const colDefault = dbColumn["COLUMN_DEFAULT"]

                            // Check for Snowflake auto-increment pattern
                            if (
                                typeof colDefault === "string" &&
                                colDefault
                                    .toUpperCase()
                                    .includes("AUTOINCREMENT")
                            ) {
                                tableColumn.isGenerated = true
                                tableColumn.generationStrategy = "increment"
                            } else {
                                tableColumn.default = colDefault
                            }
                        }

                        // Snowflake INFORMATION_SCHEMA.COLUMNS does not have
                        // IS_GENERATED / GENERATION_EXPRESSION columns.
                        // Load stored generated column expressions from typeorm_metadata.
                        const genExpr = dbGeneratedColumnsMap.get(
                            `${dbTable["TABLE_SCHEMA"]}\0${dbTable["TABLE_NAME"]}\0${dbColumn["COLUMN_NAME"]}`,
                        )
                        if (genExpr) {
                            tableColumn.isGenerated = true
                            tableColumn.generatedType = "STORED"
                            tableColumn.asExpression = genExpr
                        }

                        // Column comments fetched via SHOW COLUMNS (not in INFORMATION_SCHEMA)
                        const colComment = columnCommentMap.get(
                            dbColumn["COLUMN_NAME"],
                        )
                        tableColumn.comment = colComment || undefined

                        // Snowflake INFORMATION_SCHEMA.COLUMNS does not have
                        // CHARACTER_SET_NAME or COLLATION_NAME columns.

                        return tableColumn
                    },
                )

                // build unique constraints
                const tableUks = dbUkByTable.get(tk) ?? []
                const tableUniqueConstraintNames = OrmUtils.uniq(
                    tableUks.map((uk) => uk.constraint_name),
                )

                table.uniques = tableUniqueConstraintNames.map(
                    (constraintName) => {
                        const columns = (
                            dbUkByConstraint.get(constraintName) ?? []
                        ).map((uk) => uk.column_name)

                        return new TableUnique({
                            name: constraintName,
                            columnNames: columns,
                        })
                    },
                )

                // build check constraints
                table.checks = (dbChecksByTable.get(tk) ?? []).map((cc) => {
                    return new TableCheck({
                        name: cc["CONSTRAINT_NAME"],
                        expression: cc["CHECK_CLAUSE"],
                    })
                })

                // Snowflake does not support exclusion constraints
                table.exclusions = []

                // build foreign keys
                const tableFks = dbFkByTable.get(tk) ?? []
                const tableForeignKeyConstraintNames = OrmUtils.uniq(
                    tableFks.map((fk) => fk.constraint_name),
                )

                table.foreignKeys = tableForeignKeyConstraintNames.map(
                    (constraintName) => {
                        const foreignKeys =
                            dbFkByConstraint.get(constraintName) ?? []

                        const firstFk = foreignKeys[0]

                        // if referenced table located in currently used schema, we don't need to concat schema name to table name.
                        const refSchema = getSchemaFromKey(
                            firstFk,
                            "referenced_table_schema",
                        )
                        const referencedTableName = this.driver.buildTableName(
                            firstFk["referenced_table_name"],
                            refSchema,
                        )

                        return new TableForeignKey({
                            name: constraintName,
                            columnNames: foreignKeys.map(
                                (fk) => fk.column_name,
                            ),
                            referencedSchema:
                                firstFk["referenced_table_schema"],
                            referencedTableName: referencedTableName,
                            referencedColumnNames: foreignKeys.map(
                                (fk) => fk.referenced_column_name,
                            ),
                            onDelete: firstFk["on_delete"],
                        })
                    },
                )

                // Snowflake does not support user-created indexes
                table.indices = []

                return table
            }),
        )
    }

    /**
     * Builds create table sql.
     *
     * @param table - Table definition to generate the CREATE TABLE statement from.
     * @param createForeignKeys - Whether to include foreign key constraints in the statement.
     * @returns A {@link Query} containing the CREATE TABLE DDL.
     */
    protected createTableSql(table: Table, createForeignKeys?: boolean): Query {
        const columnDefinitions = table.columns
            .map((column) => this.buildCreateColumnSql(table, column))
            .join(", ")
        let sql = `CREATE TABLE ${this.escapePath(table)} (${columnDefinitions}`

        // Merge table.uniques with implicit single-column uniques from
        // columns marked isUnique, without mutating table.uniques.
        const allUniques = [...table.uniques]
        table.columns
            .filter((column) => column.isUnique)
            .forEach((column) => {
                const isUniqueExist = allUniques.some(
                    (unique) =>
                        unique.columnNames.length === 1 &&
                        unique.columnNames[0] === column.name,
                )
                if (!isUniqueExist)
                    allUniques.push(
                        new TableUnique({
                            name: this.connection.namingStrategy.uniqueConstraintName(
                                table,
                                [column.name],
                            ),
                            columnNames: [column.name],
                        }),
                    )
            })

        if (allUniques.length > 0) {
            const uniquesSql = allUniques
                .map((unique) => {
                    const uniqueName = unique.name
                        ? unique.name
                        : this.connection.namingStrategy.uniqueConstraintName(
                              table,
                              unique.columnNames,
                          )
                    const columnNamesSql = this.quoteColumns(unique.columnNames)
                    // Snowflake does not support DEFERRABLE constraints
                    return `CONSTRAINT ${this.escapeIdentifier(
                        uniqueName,
                    )} UNIQUE (${columnNamesSql})`
                })
                .join(", ")

            sql += `, ${uniquesSql}`
        }

        if (table.checks.length > 0) {
            const checksSql = table.checks
                .map((check) => {
                    const checkName = check.name
                        ? check.name
                        : this.connection.namingStrategy.checkConstraintName(
                              table,
                              check.expression!,
                          )
                    return `CONSTRAINT ${this.escapeIdentifier(
                        checkName,
                    )} CHECK (${check.expression})`
                })
                .join(", ")

            sql += `, ${checksSql}`
        }

        // Snowflake does not support EXCLUDE constraints — skip exclusions

        if (table.foreignKeys.length > 0 && createForeignKeys) {
            const foreignKeysSql = table.foreignKeys
                .map((fk) => {
                    const columnNamesSql = this.quoteColumns(fk.columnNames)
                    if (!fk.name)
                        fk.name = this.connection.namingStrategy.foreignKeyName(
                            table,
                            fk.columnNames,
                            this.getTablePath(fk),
                            fk.referencedColumnNames,
                        )

                    const referencedColumnNamesSql = this.quoteColumns(
                        fk.referencedColumnNames,
                    )

                    let constraint = `CONSTRAINT ${this.escapeIdentifier(
                        fk.name,
                    )} FOREIGN KEY (${columnNamesSql}) REFERENCES ${this.escapePath(
                        this.getTablePath(fk),
                    )} (${referencedColumnNamesSql})`
                    if (fk.onDelete) constraint += ` ON DELETE ${fk.onDelete}`
                    // Snowflake does not support ON UPDATE for foreign keys
                    // Snowflake does not support DEFERRABLE constraints

                    return constraint
                })
                .join(", ")

            sql += `, ${foreignKeysSql}`
        }

        const primaryColumns = table.columns.filter(
            (column) => column.isPrimary,
        )
        if (primaryColumns.length > 0) {
            const primaryKeyName = this.resolvePrimaryKeyName(
                table,
                primaryColumns,
            )

            const columnNamesSql = this.quoteColumns(
                primaryColumns.map((column) => column.name),
            )
            sql += `, CONSTRAINT ${this.escapeIdentifier(
                primaryKeyName,
            )} PRIMARY KEY (${columnNamesSql})`
        }

        sql += `)`

        // Column comments are handled separately in createTable() because
        // Snowflake SDK does not support multi-statement SQL.

        return new Query(sql)
    }

    /**
     * Loads Snowflake version.
     *
     * @returns The Snowflake server version string (e.g. `"8.0.2"`).
     */
    protected async getVersion(): Promise<string> {
        const result = await this.query(
            `SELECT CURRENT_VERSION() AS "CURRENT_VERSION"`,
        )
        return result[0]["CURRENT_VERSION"]
    }

    /**
     * Builds drop table sql.
     *
     * @param tableOrPath - Table instance or qualified table name to drop.
     * @returns A {@link Query} containing the DROP TABLE DDL.
     */
    protected dropTableSql(tableOrPath: Table | string): Query {
        return new Query(`DROP TABLE ${this.escapePath(tableOrPath)}`)
    }

    /**
     * Builds create view sql. Supports both regular and materialized views.
     *
     * @param view - View definition including the expression or query builder callback.
     * @returns A {@link Query} containing the CREATE VIEW (or CREATE MATERIALIZED VIEW) DDL.
     */
    protected createViewSql(view: View): Query {
        // Snowflake supports materialized views with limitations
        const materializedClause = view.materialized ? "MATERIALIZED " : ""
        const viewName = this.escapePath(view)

        if (typeof view.expression === "string") {
            return new Query(
                `CREATE ${materializedClause}VIEW ${viewName} AS ${view.expression}`,
            )
        } else {
            return new Query(
                `CREATE ${materializedClause}VIEW ${viewName} AS ${view
                    .expression(this.connection)
                    .getQuery()}`,
            )
        }
    }

    /**
     * Builds an INSERT query that stores the view definition in the typeorm metadata table.
     *
     * @param view - View whose definition should be persisted.
     * @returns A {@link Query} that inserts the view metadata row.
     */
    protected async insertViewDefinitionSql(view: View): Promise<Query> {
        const currentSchema = await this.getCurrentSchema()

        let { schema, tableName: name } = this.driver.parseTableName(view)

        if (!schema) {
            schema = currentSchema
        }

        const type = view.materialized
            ? MetadataTableType.MATERIALIZED_VIEW
            : MetadataTableType.VIEW
        const expression =
            typeof view.expression === "string"
                ? view.expression.trim()
                : view.expression(this.connection).getQuery()
        return this.insertTypeormMetadataSql({
            type,
            schema,
            name,
            value: expression,
        })
    }

    /**
     * Builds drop view sql.
     *
     * @param view - View to drop.
     * @returns A {@link Query} containing the DROP VIEW DDL.
     */
    protected dropViewSql(view: View): Query {
        const materializedClause = view.materialized ? "MATERIALIZED " : ""
        return new Query(
            `DROP ${materializedClause}VIEW ${this.escapePath(view)}`,
        )
    }

    /**
     * Builds a DELETE query that removes the view definition from the typeorm metadata table.
     *
     * @param view - View whose metadata row should be deleted.
     * @returns A {@link Query} that deletes the view metadata row.
     */
    protected async deleteViewDefinitionSql(view: View): Promise<Query> {
        const currentSchema = await this.getCurrentSchema()

        let { schema, tableName: name } = this.driver.parseTableName(view)

        if (!schema) {
            schema = currentSchema
        }

        const type = view.materialized
            ? MetadataTableType.MATERIALIZED_VIEW
            : MetadataTableType.VIEW
        return this.deleteTypeormMetadataSql({ type, schema, name })
    }

    /**
     * Builds create index sql.
     *
     * Snowflake does not support user-created indexes.
     * Returns a no-op query for interface compatibility.
     *
     * @param _table - Unused. Present for interface compatibility.
     * @param _index - Unused. Present for interface compatibility.
     * @returns A no-op {@link Query} (`SELECT 1`).
     */
    protected createIndexSql(_table: Table, _index: TableIndex): Query {
        return new Query(`SELECT 1 /* Snowflake does not support indexes */`)
    }

    /**
     * Builds create view index sql.
     *
     * Snowflake does not support user-created indexes.
     * Returns a no-op query for interface compatibility.
     *
     * @param _view - Unused. Present for interface compatibility.
     * @param _index - Unused. Present for interface compatibility.
     * @returns A no-op {@link Query} (`SELECT 1`).
     */
    protected createViewIndexSql(_view: View, _index: TableIndex): Query {
        return new Query(`SELECT 1 /* Snowflake does not support indexes */`)
    }

    /**
     * Builds drop index sql.
     *
     * Snowflake does not support user-created indexes.
     * Returns a no-op query for interface compatibility.
     *
     * @param _table - Unused. Present for interface compatibility.
     * @param _indexOrName - Unused. Present for interface compatibility.
     * @returns A no-op {@link Query} (`SELECT 1`).
     */
    protected dropIndexSql(
        _table: Table | View,
        _indexOrName: TableIndex | string,
    ): Query {
        return new Query(`SELECT 1 /* Snowflake does not support indexes */`)
    }

    /**
     * Builds create primary key sql.
     *
     * @param table - Table to add the primary key to.
     * @param columnNames - Column names that form the primary key.
     * @param constraintName - Optional explicit constraint name; auto-generated if omitted.
     * @returns A {@link Query} containing the ALTER TABLE ADD PRIMARY KEY DDL.
     */
    protected createPrimaryKeySql(
        table: Table,
        columnNames: string[],
        constraintName?: string,
    ): Query {
        const primaryKeyName = constraintName
            ? constraintName
            : this.connection.namingStrategy.primaryKeyName(table, columnNames)

        const columnNamesSql = this.quoteColumns(columnNames)

        return new Query(
            `ALTER TABLE ${this.escapePath(
                table,
            )} ADD CONSTRAINT ${this.escapeIdentifier(
                primaryKeyName,
            )} PRIMARY KEY (${columnNamesSql})`,
        )
    }

    /**
     * Builds drop primary key sql.
     *
     * @param table - Table whose primary key should be dropped.
     * @returns A {@link Query} containing the ALTER TABLE DROP CONSTRAINT DDL.
     * @throws {@link TypeORMError} if the table has no primary columns.
     */
    protected dropPrimaryKeySql(table: Table): Query {
        if (!table.primaryColumns.length)
            throw new TypeORMError(`Table ${table.name} has no primary keys.`)

        const primaryKeyName = this.resolvePrimaryKeyName(
            table,
            table.primaryColumns,
        )

        return new Query(
            `ALTER TABLE ${this.escapePath(
                table,
            )} DROP CONSTRAINT ${this.escapeIdentifier(primaryKeyName)}`,
        )
    }

    /**
     * Builds create unique constraint sql.
     *
     * @param table - Table to add the unique constraint to.
     * @param uniqueConstraint - Unique constraint definition.
     * @returns A {@link Query} containing the ALTER TABLE ADD UNIQUE DDL.
     */
    protected createUniqueConstraintSql(
        table: Table,
        uniqueConstraint: TableUnique,
    ): Query {
        const columnNamesSql = this.quoteColumns(uniqueConstraint.columnNames)
        // Snowflake does not support DEFERRABLE constraints
        return new Query(
            `ALTER TABLE ${this.escapePath(
                table,
            )} ADD CONSTRAINT ${this.escapeIdentifier(
                uniqueConstraint.name!,
            )} UNIQUE (${columnNamesSql})`,
        )
    }

    /**
     * Builds drop unique constraint sql.
     *
     * @param table - Table to drop the unique constraint from.
     * @param uniqueOrName - Unique constraint instance or constraint name.
     * @returns A {@link Query} containing the ALTER TABLE DROP CONSTRAINT DDL.
     */
    protected dropUniqueConstraintSql(
        table: Table,
        uniqueOrName: TableUnique | string,
    ): Query {
        const uniqueName = InstanceChecker.isTableUnique(uniqueOrName)
            ? uniqueOrName.name
            : uniqueOrName
        return new Query(
            `ALTER TABLE ${this.escapePath(
                table,
            )} DROP CONSTRAINT ${this.escapeIdentifier(uniqueName!)}`,
        )
    }

    /**
     * Builds create check constraint sql.
     *
     * @param table - Table to add the check constraint to.
     * @param checkConstraint - Check constraint definition.
     * @returns A {@link Query} containing the ALTER TABLE ADD CHECK DDL.
     */
    protected createCheckConstraintSql(
        table: Table,
        checkConstraint: TableCheck,
    ): Query {
        return new Query(
            `ALTER TABLE ${this.escapePath(
                table,
            )} ADD CONSTRAINT ${this.escapeIdentifier(
                checkConstraint.name!,
            )} CHECK (${checkConstraint.expression})`,
        )
    }

    /**
     * Builds drop check constraint sql.
     *
     * @param table - Table to drop the check constraint from.
     * @param checkOrName - Check constraint instance or constraint name.
     * @returns A {@link Query} containing the ALTER TABLE DROP CONSTRAINT DDL.
     */
    protected dropCheckConstraintSql(
        table: Table,
        checkOrName: TableCheck | string,
    ): Query {
        const checkName = InstanceChecker.isTableCheck(checkOrName)
            ? checkOrName.name
            : checkOrName
        return new Query(
            `ALTER TABLE ${this.escapePath(
                table,
            )} DROP CONSTRAINT ${this.escapeIdentifier(checkName!)}`,
        )
    }

    /**
     * Builds create exclusion constraint sql.
     * Note: Snowflake does not support exclusion constraints — always throws.
     *
     * @param table - Unused.
     * @param exclusionConstraint - Unused.
     * @returns Never returns.
     * @throws {@link TypeORMError} always.
     */
    protected createExclusionConstraintSql(
        _table: Table,
        _exclusionConstraint: TableExclusion,
    ): Query {
        throw new TypeORMError(
            `Snowflake does not support exclusion constraints.`,
        )
    }

    /**
     * Builds drop exclusion constraint sql.
     * Note: Snowflake does not support exclusion constraints — always throws.
     *
     * @param table - Unused.
     * @param exclusionOrName - Unused.
     * @returns Never returns.
     * @throws {@link TypeORMError} always.
     */
    protected dropExclusionConstraintSql(
        _table: Table,
        _exclusionOrName: TableExclusion | string,
    ): Query {
        throw new TypeORMError(
            `Snowflake does not support exclusion constraints.`,
        )
    }

    /**
     * Builds create foreign key sql.
     *
     * @param table - Table to add the foreign key to.
     * @param foreignKey - Foreign key definition.
     * @returns A {@link Query} containing the ALTER TABLE ADD FOREIGN KEY DDL.
     */
    protected createForeignKeySql(
        table: Table,
        foreignKey: TableForeignKey,
    ): Query {
        const columnNamesSql = this.quoteColumns(foreignKey.columnNames)
        const referencedColumnNamesSql = this.quoteColumns(
            foreignKey.referencedColumnNames,
        )
        let sql =
            `ALTER TABLE ${this.escapePath(
                table,
            )} ADD CONSTRAINT ${this.escapeIdentifier(
                foreignKey.name!,
            )} FOREIGN KEY (${columnNamesSql}) ` +
            `REFERENCES ${this.escapePath(
                this.getTablePath(foreignKey),
            )}(${referencedColumnNamesSql})`
        if (foreignKey.onDelete) sql += ` ON DELETE ${foreignKey.onDelete}`
        // Snowflake does not support ON UPDATE for foreign keys
        // Snowflake does not support DEFERRABLE constraints

        return new Query(sql)
    }

    /**
     * Builds drop foreign key sql.
     *
     * @param table - Table to drop the foreign key from.
     * @param foreignKeyOrName - Foreign key instance or constraint name.
     * @returns A {@link Query} containing the ALTER TABLE DROP CONSTRAINT DDL.
     */
    protected dropForeignKeySql(
        table: Table,
        foreignKeyOrName: TableForeignKey | string,
    ): Query {
        const foreignKeyName = InstanceChecker.isTableForeignKey(
            foreignKeyOrName,
        )
            ? foreignKeyOrName.name
            : foreignKeyOrName
        return new Query(
            `ALTER TABLE ${this.escapePath(
                table,
            )} DROP CONSTRAINT ${this.escapeIdentifier(foreignKeyName!)}`,
        )
    }

    /**
     * Escapes a given comment so it's safe to include in a query.
     *
     * @param comment - Raw comment string; may be `undefined` or empty.
     * @returns `'escaped_comment'` string literal, or `"NULL"` if the comment is empty.
     */
    protected escapeComment(comment?: string): string {
        if (!comment || comment.length === 0) {
            return "NULL"
        }

        comment = comment.replace(/'/g, "''").replace(/\u0000/g, "") // Null bytes aren't allowed in comments

        return `'${comment}'`
    }

    /**
     * Escapes given table or view path.
     * Each component (database, schema, table) is individually escaped via {@link escapeIdentifier}.
     *
     * @param target - A {@link Table}, {@link View}, or dot-separated path string.
     * @returns A fully escaped, dot-separated path string (e.g. `"db"."schema"."table"`).
     */
    protected escapePath(target: Table | View | string): string {
        const { database, schema, tableName } =
            this.driver.parseTableName(target)

        if (database && schema) {
            return `${this.escapeIdentifier(database)}.${this.escapeIdentifier(
                schema,
            )}.${this.escapeIdentifier(tableName)}`
        }

        if (schema) {
            return `${this.escapeIdentifier(schema)}.${this.escapeIdentifier(
                tableName,
            )}`
        }

        return this.escapeIdentifier(tableName)
    }

    /**
     * Parse a table target into its schema and table name components,
     * defaulting the schema to the current session schema if not specified.
     *
     * @param target - A {@link Table} instance or a qualified/unqualified table name string.
     * @returns An object with `schema` and `tableName` properties (unescaped).
     */
    protected async parseTableNameWithSchema(
        target: Table | string,
    ): Promise<{ schema: string; tableName: string }> {
        const parsed = this.driver.parseTableName(target)

        if (!parsed.schema) {
            parsed.schema = await this.getCurrentSchema()
        }

        return { schema: parsed.schema, tableName: parsed.tableName }
    }

    /**
     * Get the table name with table schema as a dot-separated string.
     * Note: Without ' or "
     *
     * @param target - A {@link Table} instance or a qualified/unqualified table name string.
     * @returns A dot-separated `"schema.tableName"` string (unescaped).
     */
    protected async getTableNameWithSchema(
        target: Table | string,
    ): Promise<string> {
        const { schema, tableName } = await this.parseTableNameWithSchema(
            target,
        )
        return `${schema}.${tableName}`
    }

    /**
     * Builds a query for create column.
     * Uses Snowflake AUTOINCREMENT for auto-increment columns.
     *
     * @param table - Table the column belongs to (used for naming conventions).
     * @param column - Column definition.
     * @returns A SQL fragment for the column definition (e.g. `"col" VARCHAR(255) NOT NULL DEFAULT 'x'`).
     */
    protected buildCreateColumnSql(table: Table, column: TableColumn): string {
        let c = this.escapeIdentifier(column.name)
        if (
            column.isGenerated === true &&
            column.generationStrategy !== "uuid"
        ) {
            if (column.generationStrategy === "identity") {
                // Snowflake IDENTITY column — always GENERATED ALWAYS
                c += ` ${this.connection.driver.createFullType(
                    column,
                )} GENERATED ALWAYS AS IDENTITY`
            } else if (column.generationStrategy === "increment") {
                // Snowflake AUTOINCREMENT
                c += ` ${this.connection.driver.createFullType(
                    column,
                )} AUTOINCREMENT`
            } else {
                // Unrecognized generationStrategy — still append the type to
                // avoid producing invalid SQL with a missing type clause.
                c += " " + this.connection.driver.createFullType(column)
            }
        } else {
            // Type is appended here for non-generated columns, UUID generated
            // columns, and simple-enum columns. The isGenerated non-uuid branch
            // above already appends the type as part of IDENTITY/AUTOINCREMENT.
            c += " " + this.connection.driver.createFullType(column)
        }

        // Snowflake does NOT support VIRTUAL generated columns — only STORED.
        if (column.generatedType === "VIRTUAL" && column.asExpression) {
            throw new TypeORMError(
                `Snowflake does not support VIRTUAL generated columns. ` +
                    `Column "${column.name}" must use generatedType: "STORED".`,
            )
        }

        // Snowflake supports stored generated columns
        const isStoredGenerated =
            column.generatedType === "STORED" && !!column.asExpression
        if (isStoredGenerated) {
            c += ` AS (${column.asExpression})`
        }

        // Snowflake COLLATE accepts string literals ('en-ci'), not identifiers ("en-ci").
        if (column.collation)
            c += ` COLLATE '${column.collation.replace(/'/g, "''")}'`

        // Snowflake stored generated columns do NOT allow NOT NULL or DEFAULT.
        if (!isStoredGenerated && column.isNullable !== true) c += " NOT NULL"

        // DEFAULT clause — UUID generation and explicit default are mutually exclusive.
        // Snowflake stored generated columns do NOT allow DEFAULT.
        if (isStoredGenerated) {
            // No DEFAULT for stored generated columns.
        } else if (
            column.isGenerated &&
            column.generationStrategy === "uuid" &&
            column.default === undefined
        ) {
            c += " DEFAULT UUID_STRING()"
        } else if (column.default !== undefined && column.default !== null) {
            c += " DEFAULT " + column.default
        }

        return c
    }

    /**
     * Snowflake does not support partitioned tables.
     *
     * @returns Always `false`.
     */
    protected async hasSupportForPartitionedTables(): Promise<boolean> {
        return false
    }

    /**
     * Change table comment.
     * Snowflake supports COMMENT ON TABLE.
     *
     * @param tableOrName - Target table or its name.
     * @param newComment - New comment text, or `undefined` to set to NULL.
     * @returns Resolves when the comment has been updated.
     */
    async changeTableComment(
        tableOrName: Table | string,
        newComment?: string,
    ): Promise<void> {
        const table = InstanceChecker.isTable(tableOrName)
            ? tableOrName
            : await this.getCachedTable(tableOrName)

        const escapedNewComment = this.escapeComment(newComment)
        const escapedOldComment = this.escapeComment(table.comment)

        if (escapedNewComment === escapedOldComment) {
            return
        }

        const upQueries: Query[] = []
        const downQueries: Query[] = []

        upQueries.push(
            new Query(
                `COMMENT ON TABLE ${this.escapePath(
                    table,
                )} IS ${escapedNewComment}`,
            ),
        )

        downQueries.push(
            new Query(
                `COMMENT ON TABLE ${this.escapePath(
                    table,
                )} IS ${escapedOldComment}`,
            ),
        )

        await this.executeQueries(upQueries, downQueries)

        const newTable = table.clone()
        newTable.comment = newComment
        this.replaceCachedTable(table, newTable)
    }
}
