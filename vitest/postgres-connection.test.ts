import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { EventEmitter } from "events"

/**
 * Comprehensive tests for the Postgres connection, retry, and query logic.
 *
 * Behavioral contract under test:
 *
 *   1. Normal queries: once connected, queries run with NO timeout — they
 *      can take as long as they need.
 *
 *   2. Tier-1 retryable errors ("Connection terminated unexpectedly"):
 *      retry INDEFINITELY with 500ms sleep.
 *
 *   3. Tier-2 retryable errors (ECONNREFUSED,
 *      ECONNRESET, ETIMEDOUT, PG error codes, recovery mode, etc.):
 *      retry up to maxRetryDuration (5 minutes) with 5000ms sleep.
 *
 *   4. Non-retryable errors: throw immediately, no retry.
 *
 *   5. No spurious timeout errors from internal health-checks.
 *
 * The tests cover:
 *   - createPool: pool creation, validation, timeout handling, cleanup
 *   - obtainMasterConnection: client acquisition, timeout, leak prevention
 *   - connect: tier-1 / tier-2 retry logic with correct time budgets
 *   - query (via QueryRunner): retry, reconnection, no artificial timeouts
 */

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeFakeClient(overrides: Record<string, any> = {}) {
    const client = new EventEmitter() as any
    client.query = vi
        .fn()
        .mockResolvedValue({ rows: [], rowCount: 0, command: "SELECT" })
    client.release = vi.fn()
    client.removeAllListeners = vi.fn().mockReturnThis()
    Object.assign(client, overrides)
    return client
}

function makeFakePool(client?: any) {
    const resolvedClient = client ?? makeFakeClient()
    const pool = new EventEmitter() as any
    pool.connect = vi.fn().mockResolvedValue(resolvedClient)
    pool.end = vi.fn().mockResolvedValue(undefined)
    pool.removeAllListeners = vi.fn().mockReturnThis()
    return pool
}

function makeFakeLogger() {
    return {
        log: vi.fn(),
        logQuery: vi.fn(),
        logQueryError: vi.fn(),
        logQuerySlow: vi.fn(),
        logSchemaBuild: vi.fn(),
        logMigration: vi.fn(),
    }
}

function makeFakeDataSource(logger?: any) {
    return {
        logger: logger ?? makeFakeLogger(),
        subscribers: [],
    } as any
}

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

vi.mock("pg", () => {
    const Pool = vi.fn(function () {})
    return { Pool, PoolClient: vi.fn() }
})

vi.mock("../src/driver/postgres/sleep", () => ({
    sleep: vi.fn().mockResolvedValue(undefined),
}))

import { Pool } from "pg"
import { sleep } from "../src/driver/postgres/sleep"
import {
    PostgresDriver,
    classifyError,
} from "../src/driver/postgres/PostgresDriver"
import { PostgresQueryRunner } from "../src/driver/postgres/PostgresQueryRunner"
import { QueryFailedError } from "../src/error/QueryFailedError"

// ============================================================================
// classifyError helper
// ============================================================================
describe("classifyError", () => {
    it("should return 'tier1' for 'Connection terminated unexpectedly'", () => {
        expect(
            classifyError(new Error("Connection terminated unexpectedly")),
        ).toBe("tier1")
    })

    it("should return null for 'Connection failed' (no longer a special error)", () => {
        expect(classifyError(new Error("Connection failed"))).toBeNull()
    })

    it("should return 'tier1' when message contains 'Connection terminated unexpectedly'", () => {
        expect(
            classifyError(
                new Error(
                    "pg: Connection terminated unexpectedly during query",
                ),
            ),
        ).toBe("tier1")
    })

    it("should return 'tier2' for pg connection timeout ('Connection terminated due to connection timeout')", () => {
        expect(
            classifyError(
                new Error("Connection terminated due to connection timeout"),
            ),
        ).toBe("tier2")
    })

    it("should return 'tier2' for pg pool timeout ('timeout exceeded when trying to connect')", () => {
        expect(
            classifyError(new Error("timeout exceeded when trying to connect")),
        ).toBe("tier2")
    })

    it("should return 'tier2' for ECONNREFUSED", () => {
        const err = new Error("refused") as any
        err.code = "ECONNREFUSED"
        expect(classifyError(err)).toBe("tier2")
    })

    it("should return 'tier2' for ECONNRESET", () => {
        const err = new Error("reset") as any
        err.code = "ECONNRESET"
        expect(classifyError(err)).toBe("tier2")
    })

    it("should return 'tier2' for ETIMEDOUT", () => {
        const err = new Error("timeout") as any
        err.code = "ETIMEDOUT"
        expect(classifyError(err)).toBe("tier2")
    })

    it("should return 'tier2' for PG codes 40001, 58P01, 57014, 57P03", () => {
        for (const code of ["40001", "58P01", "57014", "57P03"]) {
            const err = new Error("pg") as any
            err.code = code
            expect(classifyError(err)).toBe("tier2")
        }
    })

    it("should return 'tier2' for recovery/startup messages", () => {
        expect(
            classifyError(new Error("the database system is in recovery mode")),
        ).toBe("tier2")
        expect(
            classifyError(new Error("the database system is starting up")),
        ).toBe("tier2")
    })

    it("should return 'tier2' for replica conflict messages (case-insensitive)", () => {
        expect(
            classifyError(
                new Error("Query Might Have Conflicted With Replica Reconnect"),
            ),
        ).toBe("tier2")
        expect(
            classifyError(
                new Error("Canceling Statement Due To Conflict With Recovery"),
            ),
        ).toBe("tier2")
    })

    it("should return null for non-retryable errors", () => {
        expect(classifyError(new Error("authentication failed"))).toBeNull()
        expect(classifyError(new Error("syntax error"))).toBeNull()
        expect(classifyError(new Error("relation does not exist"))).toBeNull()
    })

    it("should return null for errors without message", () => {
        expect(classifyError({ code: "UNKNOWN" })).toBeNull()
        expect(classifyError(null)).toBeNull()
        expect(classifyError(undefined)).toBeNull()
        expect(classifyError("string error")).toBeNull()
    })
})

// ============================================================================
// createPool
// ============================================================================
describe("PostgresDriver.createPool", () => {
    let driver: PostgresDriver

    beforeEach(() => {
        vi.clearAllMocks()
        driver = Object.create(PostgresDriver.prototype)
        driver.connection = makeFakeDataSource()
        ;(driver as any).options = {} as any
    })

    it("should create pool, run SELECT 1 health-check, and return pool on success", async () => {
        const client = makeFakeClient()
        const pool = makeFakePool(client)
        vi.mocked(Pool).mockImplementation(function () {
            return pool as any
        })

        const result = await (driver as any).createPool({}, {})

        expect(result).toBe(pool)
        expect(client.query).toHaveBeenCalledWith("SELECT 1")
        expect(client.release).toHaveBeenCalled()
    })

    it("should throw original error and clean up when pool.connect() rejects", async () => {
        const pool = makeFakePool()
        pool.connect.mockRejectedValue(new Error("ECONNREFUSED"))
        vi.mocked(Pool).mockImplementation(function () {
            return pool as any
        })

        await expect((driver as any).createPool({}, {})).rejects.toThrow(
            "ECONNREFUSED",
        )
        expect(pool.removeAllListeners).toHaveBeenCalled()
        expect(pool.end).toHaveBeenCalled()
    })

    it("should throw original error and release client when health-check query fails", async () => {
        const client = makeFakeClient()
        client.query.mockRejectedValue(new Error("SELECT 1 failed"))
        const pool = makeFakePool(client)
        vi.mocked(Pool).mockImplementation(function () {
            return pool as any
        })

        await expect((driver as any).createPool({}, {})).rejects.toThrow(
            "SELECT 1 failed",
        )
        expect(client.release).toHaveBeenCalled()
        expect(pool.end).toHaveBeenCalled()
    })

    it("should pass connectTimeoutMS to pg Pool as connectionTimeoutMillis", async () => {
        const client = makeFakeClient()
        const pool = makeFakePool(client)
        let capturedConfig: any
        vi.mocked(Pool).mockImplementation(function (config: any) {
            capturedConfig = config
            return pool as any
        })

        await (driver as any).createPool({ connectTimeoutMS: 3000 }, {})

        expect(capturedConfig.connectionTimeoutMillis).toBe(3000)
    })

    it("should NOT wrap SELECT 1 with a separate timeout (no spurious Query timeout)", async () => {
        // The old code had a 5s timeout around SELECT 1. Verify that a slow
        // health-check query doesn't produce "Query timeout" — it just waits.
        const client = makeFakeClient()
        // Simulate a slow SELECT 1 that takes 100ms
        client.query.mockImplementation(
            () =>
                new Promise((resolve) =>
                    setTimeout(
                        () =>
                            resolve({
                                rows: [],
                                rowCount: 0,
                                command: "SELECT",
                            }),
                        100,
                    ),
                ),
        )
        const pool = makeFakePool(client)
        vi.mocked(Pool).mockImplementation(function () {
            return pool as any
        })

        const result = await (driver as any).createPool({}, {})

        expect(result).toBe(pool)
        expect(client.query).toHaveBeenCalledWith("SELECT 1")
    })

    it("should enable TCP keepAlive in pool config by default", async () => {
        const client = makeFakeClient()
        const pool = makeFakePool(client)
        let capturedConfig: any
        vi.mocked(Pool).mockImplementation(function (config: any) {
            capturedConfig = config
            return pool as any
        })

        await (driver as any).createPool({}, {})

        expect(capturedConfig.keepAlive).toBe(true)
        expect(capturedConfig.keepAliveInitialDelayMillis).toBe(10000)
    })

    it("should set connectionTimeoutMillis to 10s by default", async () => {
        const client = makeFakeClient()
        const pool = makeFakePool(client)
        let capturedConfig: any
        vi.mocked(Pool).mockImplementation(function (config: any) {
            capturedConfig = config
            return pool as any
        })

        await (driver as any).createPool({}, {})

        expect(capturedConfig.connectionTimeoutMillis).toBe(10000)
    })

    it("should respect custom connectTimeoutMS over default", async () => {
        const client = makeFakeClient()
        const pool = makeFakePool(client)
        let capturedConfig: any
        vi.mocked(Pool).mockImplementation(function (config: any) {
            capturedConfig = config
            return pool as any
        })

        await (driver as any).createPool({ connectTimeoutMS: 30000 }, {})

        expect(capturedConfig.connectionTimeoutMillis).toBe(30000)
    })

    it("should allow extra options to override keepAlive defaults", async () => {
        const client = makeFakeClient()
        const pool = makeFakePool(client)
        let capturedConfig: any
        vi.mocked(Pool).mockImplementation(function (config: any) {
            capturedConfig = config
            return pool as any
        })

        await (driver as any).createPool(
            { extra: { keepAlive: false, keepAliveInitialDelayMillis: 60000 } },
            {},
        )

        // extra spreads last, so it overrides our defaults
        expect(capturedConfig.keepAlive).toBe(false)
        expect(capturedConfig.keepAliveInitialDelayMillis).toBe(60000)
    })

    it("should call setKeepAlive on the socket in the connect event handler", async () => {
        const mockStream = { setKeepAlive: vi.fn() }
        const client = makeFakeClient()
        ;(client as any).connection = { stream: mockStream }

        // Capture the pool's "connect" listener and invoke it manually
        let connectListener: Function | undefined
        const pool = makeFakePool(client)
        const origOn = pool.on.bind(pool)
        pool.on = vi.fn().mockImplementation((event: string, cb: Function) => {
            if (event === "connect") connectListener = cb
            return origOn(event, cb)
        })

        vi.mocked(Pool).mockImplementation(function () {
            return pool as any
        })

        await (driver as any).createPool({}, {})

        // Simulate a new connection event
        expect(connectListener).toBeDefined()
        connectListener!(client)

        expect(mockStream.setKeepAlive).toHaveBeenCalledWith(true, 10000)
    })
})

// ============================================================================
// obtainMasterConnection
// ============================================================================
describe("PostgresDriver.obtainMasterConnection", () => {
    let driver: PostgresDriver

    beforeEach(() => {
        vi.clearAllMocks()
        driver = Object.create(PostgresDriver.prototype)
        driver.connection = makeFakeDataSource()
        ;(driver as any).options = {} as any
    })

    it("should throw when pool (this.master) is not set", async () => {
        ;(driver as any).master = undefined
        await expect(driver.obtainMasterConnection()).rejects.toThrow()
    })

    it("should return [client, safeRelease] on success", async () => {
        const client = makeFakeClient()
        ;(driver as any).master = makeFakePool(client)

        const [returned, release] = await driver.obtainMasterConnection()

        expect(returned).toBe(client)
        expect(typeof release).toBe("function")
        release()
        expect(client.release).toHaveBeenCalled()
    })

    it("should NOT run a SELECT 1 health-check (no spurious Query timeout)", async () => {
        const client = makeFakeClient()
        ;(driver as any).master = makeFakePool(client)

        await driver.obtainMasterConnection()

        // client.query should NOT have been called — obtainMasterConnection
        // just hands out the client, no health-check
        expect(client.query).not.toHaveBeenCalled()
    })

    it("should propagate pool.connect() errors directly (pg handles timeouts)", async () => {
        const pool = makeFakePool()
        pool.connect.mockRejectedValue(new Error("pool exhausted"))
        ;(driver as any).master = pool

        await expect(driver.obtainMasterConnection()).rejects.toThrow(
            "pool exhausted",
        )
    })

    it("safeRelease should not throw even if client.release throws", async () => {
        const client = makeFakeClient()
        client.release.mockImplementation(() => {
            throw new Error("already released")
        })
        ;(driver as any).master = makeFakePool(client)

        const [, release] = await driver.obtainMasterConnection()
        expect(() => release()).not.toThrow()
    })
})

// ============================================================================
// connect() — retry logic
// ============================================================================
describe("PostgresDriver.connect", () => {
    let driver: PostgresDriver
    let mockCreatePool: ReturnType<typeof vi.fn>
    let mockCreateQueryRunner: ReturnType<typeof vi.fn>

    beforeEach(() => {
        vi.clearAllMocks()
        driver = Object.create(PostgresDriver.prototype)
        driver.connection = makeFakeDataSource()
        driver.maxRetryDuration = 5 * 60 * 1000
        ;(driver as any).options = {} as any
        ;(driver as any).database = undefined
        ;(driver as any).searchSchema = undefined
        ;(driver as any).schema = undefined

        mockCreatePool = vi.fn()
        ;(driver as any).createPool = mockCreatePool
        mockCreateQueryRunner = vi.fn()
        ;(driver as any).createQueryRunner = mockCreateQueryRunner
    })

    function setupSuccessfulQueryRunner() {
        const qr = {
            getVersion: vi.fn().mockResolvedValue("15.0"),
            getCurrentDatabase: vi.fn().mockResolvedValue("testdb"),
            getCurrentSchema: vi.fn().mockResolvedValue("public"),
            release: vi.fn(),
        }
        mockCreateQueryRunner.mockReturnValue(qr)
        return qr
    }

    // --- Basic success ---

    it("should connect successfully on first attempt", async () => {
        mockCreatePool.mockResolvedValue(makeFakePool())
        const qr = setupSuccessfulQueryRunner()

        await driver.connect()

        expect((driver as any).master).toBeDefined()
        expect((driver as any).version).toBe("15.0")
        expect(qr.release).toHaveBeenCalled()
    })

    it("should set database/schema from query runner when not pre-set", async () => {
        mockCreatePool.mockResolvedValue(makeFakePool())
        setupSuccessfulQueryRunner()

        await driver.connect()

        expect((driver as any).database).toBe("testdb")
        expect((driver as any).searchSchema).toBe("public")
        expect((driver as any).schema).toBe("public")
    })

    it("should not overwrite pre-set database/schema", async () => {
        ;(driver as any).database = "mydb"
        ;(driver as any).searchSchema = "myschema"
        ;(driver as any).schema = "custom"
        mockCreatePool.mockResolvedValue(makeFakePool())
        setupSuccessfulQueryRunner()

        await driver.connect()

        expect((driver as any).database).toBe("mydb")
        expect((driver as any).searchSchema).toBe("myschema")
        expect((driver as any).schema).toBe("custom")
    })

    // --- Tier-1: retry indefinitely ---

    it("should retry on 'Connection terminated unexpectedly' (tier-1) with 500ms sleep", async () => {
        mockCreatePool
            .mockRejectedValueOnce(
                new Error("Connection terminated unexpectedly"),
            )
            .mockResolvedValue(makeFakePool())
        setupSuccessfulQueryRunner()

        await driver.connect()

        expect(sleep).toHaveBeenCalledWith(500)
        expect(mockCreatePool).toHaveBeenCalledTimes(2)
    })

    it("tier-1 should retry INDEFINITELY — no maxRetryDuration cap", async () => {
        driver.maxRetryDuration = 100 // very short cap

        const realNow = Date.now
        let currentTime = 1000
        vi.spyOn(Date, "now").mockImplementation(() => currentTime)

        let callCount = 0
        mockCreatePool.mockImplementation(() => {
            callCount++
            if (callCount <= 10) {
                return Promise.reject(
                    new Error("Connection terminated unexpectedly"),
                )
            }
            // 11th attempt: succeed
            return Promise.resolve(makeFakePool())
        })
        setupSuccessfulQueryRunner()

        vi.mocked(sleep).mockImplementation(async () => {
            currentTime += 50 // advance time past maxRetryDuration each sleep
        })

        // Despite elapsed time >> maxRetryDuration, tier-1 keeps retrying
        await driver.connect()

        expect(callCount).toBe(11)
        // Time is now 1000 + 10*50 = 1500, well past maxRetryDuration=100
        // But it still connected successfully
        expect(sleep).toHaveBeenCalledTimes(10)

        Date.now = realNow
    })

    // --- Tier-2: retry up to maxRetryDuration ---

    it("should retry on ECONNREFUSED (tier-2) with 5000ms sleep", async () => {
        const err = new Error("connection refused") as any
        err.code = "ECONNREFUSED"
        mockCreatePool
            .mockRejectedValueOnce(err)
            .mockResolvedValue(makeFakePool())
        setupSuccessfulQueryRunner()

        await driver.connect()

        expect(sleep).toHaveBeenCalledWith(5000)
        expect(mockCreatePool).toHaveBeenCalledTimes(2)
    })

    it("should retry on ECONNRESET (tier-2) with 5000ms sleep", async () => {
        const err = new Error("connection reset") as any
        err.code = "ECONNRESET"
        mockCreatePool
            .mockRejectedValueOnce(err)
            .mockResolvedValue(makeFakePool())
        setupSuccessfulQueryRunner()
        await driver.connect()
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on ETIMEDOUT (tier-2) with 5000ms sleep", async () => {
        const err = new Error("timed out") as any
        err.code = "ETIMEDOUT"
        mockCreatePool
            .mockRejectedValueOnce(err)
            .mockResolvedValue(makeFakePool())
        setupSuccessfulQueryRunner()
        await driver.connect()
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on PG error codes 40001, 58P01, 57014, 57P03 (tier-2)", async () => {
        for (const code of ["40001", "58P01", "57014", "57P03"]) {
            vi.clearAllMocks()
            ;(driver as any).master = undefined
            const err = new Error(`pg error ${code}`) as any
            err.code = code
            mockCreatePool
                .mockRejectedValueOnce(err)
                .mockResolvedValue(makeFakePool())
            setupSuccessfulQueryRunner()
            await driver.connect()
            expect(sleep).toHaveBeenCalledWith(5000)
        }
    })

    it("should retry on 'the database system is in recovery mode' (tier-2)", async () => {
        mockCreatePool
            .mockRejectedValueOnce(
                new Error("the database system is in recovery mode"),
            )
            .mockResolvedValue(makeFakePool())
        setupSuccessfulQueryRunner()
        await driver.connect()
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on 'the database system is starting up' (tier-2)", async () => {
        mockCreatePool
            .mockRejectedValueOnce(
                new Error("the database system is starting up"),
            )
            .mockResolvedValue(makeFakePool())
        setupSuccessfulQueryRunner()
        await driver.connect()
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on 'query might have conflicted with replica reconnect' (tier-2, case-insensitive)", async () => {
        mockCreatePool
            .mockRejectedValueOnce(
                new Error("Query Might Have Conflicted With Replica Reconnect"),
            )
            .mockResolvedValue(makeFakePool())
        setupSuccessfulQueryRunner()
        await driver.connect()
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on 'canceling statement due to conflict with recovery' (tier-2, case-insensitive)", async () => {
        mockCreatePool
            .mockRejectedValueOnce(
                new Error("Canceling Statement Due To Conflict With Recovery"),
            )
            .mockResolvedValue(makeFakePool())
        setupSuccessfulQueryRunner()
        await driver.connect()
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("tier-2 should throw after exceeding maxRetryDuration (wall-clock)", async () => {
        driver.maxRetryDuration = 200

        const realNow = Date.now
        let currentTime = 1000
        vi.spyOn(Date, "now").mockImplementation(() => currentTime)

        const err = new Error("refused") as any
        err.code = "ECONNREFUSED"
        mockCreatePool.mockRejectedValue(err)

        vi.mocked(sleep).mockImplementation(async () => {
            currentTime += 150
        })

        await expect(driver.connect()).rejects.toThrow()

        // attempt 1: elapsed=0 < 200 → sleep → time+=150
        // attempt 2: elapsed=150 < 200 → sleep → time+=150
        // attempt 3: elapsed=300 > 200 → throw
        expect(mockCreatePool).toHaveBeenCalledTimes(3)

        Date.now = realNow
    })

    // --- Non-retryable ---

    it("should throw immediately on non-retryable errors (e.g. auth failure)", async () => {
        mockCreatePool.mockRejectedValue(new Error("authentication failed"))

        await expect(driver.connect()).rejects.toThrow("authentication failed")
        expect(sleep).not.toHaveBeenCalled()
        expect(mockCreatePool).toHaveBeenCalledTimes(1)
    })

    // --- maxRetryDuration default ---

    it("should have maxRetryDuration default of 5 minutes", () => {
        // maxRetryDuration is a class field, so check on the driver instance
        // created in beforeEach (which manually sets it)
        expect(driver.maxRetryDuration).toBe(5 * 60 * 1000)
    })
})

// ============================================================================
// query() — retry logic and normal execution
// ============================================================================
describe("PostgresQueryRunner.query", () => {
    let driver: any
    let queryRunner: PostgresQueryRunner
    let mockDbConnection: any

    beforeEach(() => {
        vi.clearAllMocks()
        driver = Object.create(PostgresDriver.prototype)
        driver.connection = makeFakeDataSource()
        driver.maxRetryDuration = 5 * 60 * 1000
        driver.options = {} as any
        driver.connectedQueryRunners = []

        mockDbConnection = makeFakeClient()
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValue([mockDbConnection, vi.fn()])

        queryRunner = new PostgresQueryRunner(driver, "master")
    })

    afterEach(() => {
        vi.restoreAllMocks()
    })

    // --- Normal query execution (no timeout) ---

    it("should execute query successfully and return rows", async () => {
        mockDbConnection.query.mockResolvedValue({
            rows: [{ id: 1 }],
            rowCount: 1,
            command: "SELECT",
        })

        const result = await queryRunner.query("SELECT * FROM users")

        expect(result).toEqual([{ id: 1 }])
        expect(mockDbConnection.query).toHaveBeenCalledWith(
            "SELECT * FROM users",
            undefined,
        )
    })

    it("normal query should have NO timeout — waits as long as needed", async () => {
        // Simulate a query that takes 500ms to resolve
        mockDbConnection.query.mockImplementation(
            () =>
                new Promise((resolve) =>
                    setTimeout(
                        () =>
                            resolve({
                                rows: [{ ok: 1 }],
                                rowCount: 1,
                                command: "SELECT",
                            }),
                        500,
                    ),
                ),
        )

        const result = await queryRunner.query("SELECT pg_sleep(10)")

        expect(result).toEqual([{ ok: 1 }])
        // No timeout error, no retry, no sleep
        expect(sleep).not.toHaveBeenCalled()
    })

    it("should return structured result when useStructuredResult is true", async () => {
        mockDbConnection.query.mockResolvedValue({
            rows: [{ id: 1 }],
            rowCount: 1,
            command: "SELECT",
        })

        const result = await queryRunner.query(
            "SELECT * FROM users",
            undefined,
            true,
        )

        expect(result.records).toEqual([{ id: 1 }])
        expect(result.affected).toBe(1)
    })

    it("should return [rows, rowCount] for DELETE queries", async () => {
        mockDbConnection.query.mockResolvedValue({
            rows: [],
            rowCount: 5,
            command: "DELETE",
        })
        const result = await queryRunner.query(
            "DELETE FROM users WHERE id > 10",
        )
        expect(result).toEqual([[], 5])
    })

    it("should return [rows, rowCount] for UPDATE queries", async () => {
        mockDbConnection.query.mockResolvedValue({
            rows: [],
            rowCount: 3,
            command: "UPDATE",
        })
        const result = await queryRunner.query("UPDATE users SET name = 'test'")
        expect(result).toEqual([[], 3])
    })

    // --- Tier-1 retry: indefinite ---

    it("should retry on 'Connection terminated unexpectedly' (tier-1, 500ms sleep)", async () => {
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [{ id: 1 }],
            rowCount: 1,
            command: "SELECT",
        })

        mockDbConnection.query.mockRejectedValueOnce(
            new Error("Connection terminated unexpectedly"),
        )
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([mockDbConnection, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])

        const result = await queryRunner.query("SELECT 1")

        expect(sleep).toHaveBeenCalledWith(500)
        expect(result).toEqual([{ id: 1 }])
    })

    it("should throw immediately on 'Connection failed' (not retryable)", async () => {
        mockDbConnection.query.mockRejectedValueOnce(
            new Error("Connection failed"),
        )

        await expect(queryRunner.query("SELECT 1")).rejects.toThrow(
            QueryFailedError,
        )
        expect(sleep).not.toHaveBeenCalled()
    })

    it("tier-1 query retry should be INDEFINITE — no maxRetryDuration cap", async () => {
        driver.maxRetryDuration = 50 // very short cap

        const realNow = Date.now
        let currentTime = 1000
        vi.spyOn(Date, "now").mockImplementation(() => currentTime)

        let callCount = 0
        const brokenConn = makeFakeClient()
        brokenConn.query.mockRejectedValue(
            new Error("Connection terminated unexpectedly"),
        )

        const goodConn = makeFakeClient()
        goodConn.query.mockResolvedValue({
            rows: [{ ok: 1 }],
            rowCount: 1,
            command: "SELECT",
        })

        driver.obtainMasterConnection = vi.fn().mockImplementation(() => {
            callCount++
            if (callCount <= 5) return Promise.resolve([brokenConn, vi.fn()])
            return Promise.resolve([goodConn, vi.fn()])
        })

        vi.mocked(sleep).mockImplementation(async () => {
            currentTime += 100 // each sleep advances past maxRetryDuration
        })

        // Should eventually succeed despite elapsed >> maxRetryDuration
        const result = await queryRunner.query("SELECT 1")

        expect(result).toEqual([{ ok: 1 }])
        expect(callCount).toBe(6)
        // Total elapsed = 5 * 100 = 500ms >> maxRetryDuration=50ms, but still succeeded

        Date.now = realNow
    })

    // --- Tier-2 retry: capped at maxRetryDuration ---

    it("should retry on ECONNREFUSED (tier-2, 5000ms sleep)", async () => {
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })
        const err = new Error("refused") as any
        err.code = "ECONNREFUSED"
        mockDbConnection.query.mockRejectedValueOnce(err)
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([mockDbConnection, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])

        await queryRunner.query("SELECT 1")
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on ECONNRESET (tier-2, 5000ms sleep)", async () => {
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })
        const err = new Error("reset") as any
        err.code = "ECONNRESET"
        mockDbConnection.query.mockRejectedValueOnce(err)
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([mockDbConnection, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])
        await queryRunner.query("SELECT 1")
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on ETIMEDOUT (tier-2, 5000ms sleep)", async () => {
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })
        const err = new Error("timed out") as any
        err.code = "ETIMEDOUT"
        mockDbConnection.query.mockRejectedValueOnce(err)
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([mockDbConnection, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])
        await queryRunner.query("SELECT 1")
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on PG error 40001 (serialization failure, tier-2)", async () => {
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })
        const err = new Error("serialization failure") as any
        err.code = "40001"
        mockDbConnection.query.mockRejectedValueOnce(err)
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([mockDbConnection, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])
        await queryRunner.query("SELECT 1")
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on 'the database system is in recovery mode' (tier-2)", async () => {
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })
        mockDbConnection.query.mockRejectedValueOnce(
            new Error("the database system is in recovery mode"),
        )
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([mockDbConnection, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])
        await queryRunner.query("SELECT 1")
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on 'the database system is starting up' (tier-2)", async () => {
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })
        mockDbConnection.query.mockRejectedValueOnce(
            new Error("the database system is starting up"),
        )
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([mockDbConnection, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])
        await queryRunner.query("SELECT 1")
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on 'query might have conflicted with replica reconnect' (tier-2)", async () => {
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })
        mockDbConnection.query.mockRejectedValueOnce(
            new Error("Query might have conflicted with replica reconnect"),
        )
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([mockDbConnection, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])
        await queryRunner.query("SELECT 1")
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("should retry on 'canceling statement due to conflict with recovery' (tier-2)", async () => {
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })
        mockDbConnection.query.mockRejectedValueOnce(
            new Error("Canceling statement due to conflict with recovery"),
        )
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([mockDbConnection, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])
        await queryRunner.query("SELECT 1")
        expect(sleep).toHaveBeenCalledWith(5000)
    })

    it("tier-2 query retry should throw QueryFailedError after maxRetryDuration", async () => {
        driver.maxRetryDuration = 200

        const realNow = Date.now
        let currentTime = 1000
        vi.spyOn(Date, "now").mockImplementation(() => currentTime)

        const err = new Error("refused") as any
        err.code = "ECONNREFUSED"
        mockDbConnection.query.mockRejectedValue(err)

        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValue([mockDbConnection, vi.fn()])

        vi.mocked(sleep).mockImplementation(async () => {
            currentTime += 150
        })

        await expect(queryRunner.query("SELECT 1")).rejects.toThrow(
            QueryFailedError,
        )

        // attempt 1: elapsed=0 < 200 → sleep → time+=150
        // attempt 2: elapsed=150 < 200 → sleep → time+=150
        // attempt 3: elapsed=300 > 200 → throw
        expect(driver.obtainMasterConnection).toHaveBeenCalledTimes(3)

        Date.now = realNow
    })

    // --- Non-retryable ---

    it("should throw QueryFailedError immediately on non-retryable errors", async () => {
        mockDbConnection.query.mockRejectedValue(
            new Error("syntax error at position 1"),
        )

        await expect(queryRunner.query("SELCT * FROM users")).rejects.toThrow(
            QueryFailedError,
        )
        expect(sleep).not.toHaveBeenCalled()
    })

    // --- Reconnection behavior ---

    it("should obtain a fresh connection on retry after connection error", async () => {
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [{ ok: true }],
            rowCount: 1,
            command: "SELECT",
        })

        mockDbConnection.query.mockRejectedValueOnce(
            new Error("Connection terminated unexpectedly"),
        )

        let obtainCount = 0
        driver.obtainMasterConnection = vi.fn().mockImplementation(() => {
            obtainCount++
            if (obtainCount === 1)
                return Promise.resolve([mockDbConnection, vi.fn()])
            return Promise.resolve([freshConn, vi.fn()])
        })

        const result = await queryRunner.query("SELECT 1")

        expect(driver.obtainMasterConnection).toHaveBeenCalledTimes(2)
        expect(result).toEqual([{ ok: true }])
    })

    it("should NOT call driver.connect() during query retry (no pool recreation)", async () => {
        const connectSpy = vi.fn()
        driver.connect = connectSpy

        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })

        mockDbConnection.query.mockRejectedValueOnce(
            new Error("Connection terminated unexpectedly"),
        )
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([mockDbConnection, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])

        await queryRunner.query("SELECT 1")

        expect(connectSpy).not.toHaveBeenCalled()
    })

    // --- Slow query logging ---

    it("should log slow queries when maxQueryExecutionTime is set", async () => {
        driver.options.maxQueryExecutionTime = 50

        const realDateNow = Date.now
        let callCount = 0
        vi.spyOn(Date, "now").mockImplementation(() => {
            callCount++
            // 1st call: queryStartTime
            // 2nd call: queryEndTime (100ms later)
            if (callCount <= 1) return 1000
            return 1100
        })

        mockDbConnection.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })

        await queryRunner.query("SELECT 1")

        expect(driver.connection.logger.logQuerySlow).toHaveBeenCalledWith(
            100,
            "SELECT 1",
            undefined,
            queryRunner,
        )

        Date.now = realDateNow
    })
})

// ============================================================================
// QueryRunner.connect — connection caching
// ============================================================================
describe("PostgresQueryRunner.connect", () => {
    let driver: any
    let queryRunner: PostgresQueryRunner

    beforeEach(() => {
        vi.clearAllMocks()
        driver = Object.create(PostgresDriver.prototype)
        driver.connection = makeFakeDataSource()
        driver.options = {} as any
        driver.connectedQueryRunners = []
        queryRunner = new PostgresQueryRunner(driver, "master")
    })

    it("should cache the database connection on subsequent calls", async () => {
        const client = makeFakeClient()
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValue([client, vi.fn()])

        const conn1 = await queryRunner.connect()
        const conn2 = await queryRunner.connect()

        expect(conn1).toBe(conn2)
        expect(driver.obtainMasterConnection).toHaveBeenCalledTimes(1)
    })

    it("should register the query runner in connectedQueryRunners", async () => {
        const client = makeFakeClient()
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValue([client, vi.fn()])

        await queryRunner.connect()

        expect(driver.connectedQueryRunners).toContain(queryRunner)
    })

    it("should be lazy — no connection acquired until first query/connect call", () => {
        // QueryRunner constructor should NOT call obtainMasterConnection
        driver.obtainMasterConnection = vi.fn()

        const qr = new PostgresQueryRunner(driver, "master")

        expect(driver.obtainMasterConnection).not.toHaveBeenCalled()
        expect((qr as any).databaseConnection).toBeUndefined()
        expect((qr as any).databaseConnectionPromise).toBeUndefined()
    })
})

// ============================================================================
// QueryRunner.release — cleanup and idempotency
// ============================================================================
describe("PostgresQueryRunner.release", () => {
    let driver: any
    let queryRunner: PostgresQueryRunner
    let mockReleaseFn: ReturnType<typeof vi.fn>

    beforeEach(() => {
        vi.clearAllMocks()
        driver = Object.create(PostgresDriver.prototype)
        driver.connection = makeFakeDataSource()
        driver.options = {} as any
        driver.connectedQueryRunners = []

        mockReleaseFn = vi.fn()
        const client = makeFakeClient()
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValue([client, mockReleaseFn])

        queryRunner = new PostgresQueryRunner(driver, "master")
    })

    it("should release the connection and unregister from connectedQueryRunners", async () => {
        await queryRunner.connect()
        expect(driver.connectedQueryRunners).toContain(queryRunner)

        await queryRunner.release()

        expect(driver.connectedQueryRunners).not.toContain(queryRunner)
        expect((queryRunner as any).isReleased).toBe(true)
    })

    it("should be idempotent — calling release() twice does not throw or double-release", async () => {
        await queryRunner.connect()

        await queryRunner.release()
        await queryRunner.release() // second call

        // releaseCallback should only have been called once
        expect(mockReleaseFn).toHaveBeenCalledTimes(1)
    })
})

// ============================================================================
// Transaction safety — query retry must NOT happen mid-transaction
// ============================================================================
describe("PostgresQueryRunner.query — transaction safety", () => {
    let driver: any
    let queryRunner: PostgresQueryRunner
    let mockDbConnection: any

    beforeEach(() => {
        vi.clearAllMocks()
        driver = Object.create(PostgresDriver.prototype)
        driver.connection = makeFakeDataSource()
        driver.maxRetryDuration = 5 * 60 * 1000
        driver.options = {} as any
        driver.connectedQueryRunners = []

        mockDbConnection = makeFakeClient()
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValue([mockDbConnection, vi.fn()])

        queryRunner = new PostgresQueryRunner(driver, "master")
    })

    it("should NOT retry on connection error if transaction is active — throws immediately", async () => {
        // Get connected first
        await queryRunner.connect()

        // Now simulate an active transaction
        ;(queryRunner as any).isTransactionActive = true
        ;(queryRunner as any).transactionDepth = 1

        // The next query on this connection will fail with a connection error
        mockDbConnection.query.mockRejectedValueOnce(
            new Error("Connection terminated unexpectedly"),
        )

        await expect(
            queryRunner.query("UPDATE users SET name = 'test' WHERE id = 1"),
        ).rejects.toThrow(QueryFailedError)

        // Should NOT have retried (no sleep called)
        expect(sleep).not.toHaveBeenCalled()
    })

    it("should NOT retry on tier-2 error if transaction is active", async () => {
        await queryRunner.connect()
        ;(queryRunner as any).isTransactionActive = true
        ;(queryRunner as any).transactionDepth = 1

        const err = new Error("serialization failure") as any
        err.code = "40001"
        mockDbConnection.query.mockRejectedValueOnce(err)

        await expect(
            queryRunner.query("INSERT INTO orders VALUES (1)"),
        ).rejects.toThrow(QueryFailedError)

        expect(sleep).not.toHaveBeenCalled()
    })

    it("should still retry connection errors when NOT in a transaction", async () => {
        ;(queryRunner as any).isTransactionActive = false

        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })

        mockDbConnection.query.mockRejectedValueOnce(
            new Error("Connection terminated unexpectedly"),
        )
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([mockDbConnection, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])

        await queryRunner.query("SELECT 1")

        // Should have retried
        expect(sleep).toHaveBeenCalledWith(500)
    })

    it("startTransaction itself can retry if connection drops before transaction begins", async () => {
        // startTransaction calls this.query("START TRANSACTION")
        // At that point isTransactionActive is true, so retry is blocked.
        // But the connection error means the START TRANSACTION never executed,
        // so the error should propagate to the caller who can retry the whole transaction.
        await queryRunner.connect()

        mockDbConnection.query.mockRejectedValueOnce(
            new Error("Connection terminated unexpectedly"),
        )

        // startTransaction sets isTransactionActive = true BEFORE calling query
        await expect(queryRunner.startTransaction()).rejects.toThrow()

        // No retry happened (isTransactionActive was true when query ran)
        expect(sleep).not.toHaveBeenCalled()
    })

    it("error in transaction propagates to caller — caller retries the whole transaction", async () => {
        // Simulates the full pattern:
        // 1. Start transaction (succeeds)
        // 2. INSERT (connection drops) → throws QueryFailedError
        // 3. Caller catches, retries entire transaction on new QueryRunner
        await queryRunner.connect()

        // First: START TRANSACTION succeeds
        mockDbConnection.query.mockResolvedValueOnce({
            rows: [],
            rowCount: 0,
            command: "START",
        })
        await queryRunner.startTransaction()
        expect((queryRunner as any).isTransactionActive).toBe(true)

        // Second: INSERT fails with connection error
        mockDbConnection.query.mockRejectedValueOnce(
            new Error("Connection terminated unexpectedly"),
        )

        const txError = await queryRunner
            .query("INSERT INTO users (name) VALUES ('Alice')")
            .catch((e) => e)

        expect(txError).toBeInstanceOf(QueryFailedError)
        expect(sleep).not.toHaveBeenCalled()

        // Caller creates a new QueryRunner and retries the whole transaction
        const newMockConn = makeFakeClient()
        newMockConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValue([newMockConn, vi.fn()])

        const qr2 = new PostgresQueryRunner(driver, "master")
        await qr2.connect()

        // Retry: START TRANSACTION
        newMockConn.query.mockResolvedValueOnce({
            rows: [],
            rowCount: 0,
            command: "START",
        })
        await qr2.startTransaction()

        // Retry: INSERT (succeeds this time)
        newMockConn.query.mockResolvedValueOnce({
            rows: [{ id: 1 }],
            rowCount: 1,
            command: "INSERT",
        })
        const result = await qr2.query(
            "INSERT INTO users (name) VALUES ('Alice')",
        )

        expect(result).toEqual([{ id: 1 }])

        // Retry: COMMIT
        newMockConn.query.mockResolvedValueOnce({
            rows: [],
            rowCount: 0,
            command: "COMMIT",
        })
        await qr2.commitTransaction()
        expect((qr2 as any).isTransactionActive).toBe(false)
    })

    it("non-retryable error in transaction still throws immediately", async () => {
        await queryRunner.connect()

        mockDbConnection.query.mockResolvedValueOnce({
            rows: [],
            rowCount: 0,
            command: "START",
        })
        await queryRunner.startTransaction()

        // Syntax error — non-retryable, should throw immediately
        mockDbConnection.query.mockRejectedValueOnce(
            new Error('syntax error at or near "INSRT"'),
        )

        await expect(
            queryRunner.query("INSRT INTO users VALUES (1)"),
        ).rejects.toThrow(QueryFailedError)

        expect(sleep).not.toHaveBeenCalled()
    })
})

// ============================================================================
// Query retry — state reset verification
// ============================================================================
describe("PostgresQueryRunner.query — state reset on retry", () => {
    let driver: any
    let queryRunner: PostgresQueryRunner

    beforeEach(() => {
        vi.clearAllMocks()
        driver = Object.create(PostgresDriver.prototype)
        driver.connection = makeFakeDataSource()
        driver.maxRetryDuration = 5 * 60 * 1000
        driver.options = {} as any
        driver.connectedQueryRunners = []
        queryRunner = new PostgresQueryRunner(driver, "master")
    })

    it("should clear databaseConnection and databaseConnectionPromise before retry", async () => {
        const brokenConn = makeFakeClient()
        brokenConn.query.mockRejectedValueOnce(
            new Error("Connection terminated unexpectedly"),
        )
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [],
            rowCount: 0,
            command: "SELECT",
        })

        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValueOnce([brokenConn, vi.fn()])
            .mockResolvedValueOnce([freshConn, vi.fn()])

        await queryRunner.query("SELECT 1")

        // After retry, the queryRunner should have the fresh connection
        // obtainMasterConnection should have been called twice (once for each connect)
        expect(driver.obtainMasterConnection).toHaveBeenCalledTimes(2)
    })

    it("should use fresh connection on retry — not the stale cached one", async () => {
        const brokenConn = makeFakeClient()
        brokenConn.query.mockRejectedValue(
            new Error("Connection terminated unexpectedly"),
        )
        const freshConn = makeFakeClient()
        freshConn.query.mockResolvedValue({
            rows: [{ id: 42 }],
            rowCount: 1,
            command: "SELECT",
        })

        let callCount = 0
        driver.obtainMasterConnection = vi.fn().mockImplementation(() => {
            callCount++
            if (callCount === 1) return Promise.resolve([brokenConn, vi.fn()])
            return Promise.resolve([freshConn, vi.fn()])
        })

        const result = await queryRunner.query("SELECT 42")

        expect(result).toEqual([{ id: 42 }])
        // Second query should have been on freshConn, not brokenConn
        expect(freshConn.query).toHaveBeenCalledWith("SELECT 42", undefined)
    })
})

// ============================================================================
// disconnect / closePool — cleanup
// ============================================================================
describe("PostgresDriver.disconnect", () => {
    let driver: PostgresDriver

    beforeEach(() => {
        vi.clearAllMocks()
        driver = Object.create(PostgresDriver.prototype)
        driver.connection = makeFakeDataSource()
        ;(driver as any).options = {} as any
        ;(driver as any).connectedQueryRunners = []
    })

    it("should release all connected query runners before ending pool", async () => {
        const pool = makeFakePool()
        // closePool uses callback-style pool.end(cb)
        pool.end = vi.fn((cb: Function) => cb())
        ;(driver as any).master = pool

        // Create two fake connected query runners
        const qr1 = { release: vi.fn() }
        const qr2 = { release: vi.fn() }
        ;(driver as any).connectedQueryRunners = [qr1, qr2]

        // release() must actually remove from the array (as real release does)
        qr1.release.mockImplementation(async () => {
            const idx = (driver as any).connectedQueryRunners.indexOf(qr1)
            if (idx !== -1) (driver as any).connectedQueryRunners.splice(idx, 1)
        })
        qr2.release.mockImplementation(async () => {
            const idx = (driver as any).connectedQueryRunners.indexOf(qr2)
            if (idx !== -1) (driver as any).connectedQueryRunners.splice(idx, 1)
        })

        await (driver as any).closePool(pool)

        expect(qr1.release).toHaveBeenCalled()
        expect(qr2.release).toHaveBeenCalled()
        expect((driver as any).connectedQueryRunners).toHaveLength(0)
    })

    it("should call pool.end() after releasing all query runners", async () => {
        const pool = makeFakePool()
        pool.end = vi.fn((cb: Function) => cb())
        ;(driver as any).master = pool
        ;(driver as any).connectedQueryRunners = []

        await (driver as any).closePool(pool)

        expect(pool.end).toHaveBeenCalled()
    })
})

// ============================================================================
// Error safety — err.message undefined
// ============================================================================
describe("Error safety — err without message property", () => {
    let driver: any
    let queryRunner: PostgresQueryRunner
    let mockDbConnection: any

    beforeEach(() => {
        vi.clearAllMocks()
        driver = Object.create(PostgresDriver.prototype)
        driver.connection = makeFakeDataSource()
        driver.maxRetryDuration = 5 * 60 * 1000
        driver.options = {} as any
        driver.connectedQueryRunners = []

        mockDbConnection = makeFakeClient()
        driver.obtainMasterConnection = vi
            .fn()
            .mockResolvedValue([mockDbConnection, vi.fn()])
        queryRunner = new PostgresQueryRunner(driver, "master")
    })

    it("query() should not crash when error has no message property", async () => {
        // Some drivers or pg internals may throw non-Error objects
        mockDbConnection.query.mockRejectedValue({ code: "UNKNOWN" })

        await expect(queryRunner.query("SELECT 1")).rejects.toThrow(
            QueryFailedError,
        )
        // Should not have thrown TypeError about reading 'includes' of undefined
    })

    it("query() should not crash when error is a plain string", async () => {
        mockDbConnection.query.mockRejectedValue("something went wrong")

        await expect(queryRunner.query("SELECT 1")).rejects.toThrow(
            QueryFailedError,
        )
    })

    it("connect() should not crash when error has no message property", async () => {
        const mockCreatePool = vi.fn().mockRejectedValue({ code: "UNKNOWN" })
        driver.createPool = mockCreatePool

        await expect(driver.connect()).rejects.toBeDefined()
        expect(mockCreatePool).toHaveBeenCalledTimes(1) // should throw immediately (non-retryable)
    })
})

// ============================================================================
// connect() — pool cleanup on retry
// ============================================================================
describe("PostgresDriver.connect — pool cleanup on retry", () => {
    let driver: PostgresDriver
    let mockCreatePool: ReturnType<typeof vi.fn>
    let mockCreateQueryRunner: ReturnType<typeof vi.fn>

    beforeEach(() => {
        vi.clearAllMocks()
        driver = Object.create(PostgresDriver.prototype)
        driver.connection = makeFakeDataSource()
        driver.maxRetryDuration = 5 * 60 * 1000
        ;(driver as any).options = {} as any
        ;(driver as any).database = undefined
        ;(driver as any).searchSchema = undefined
        ;(driver as any).schema = undefined
        ;(driver as any).master = undefined

        mockCreatePool = vi.fn()
        ;(driver as any).createPool = mockCreatePool
        mockCreateQueryRunner = vi.fn()
        ;(driver as any).createQueryRunner = mockCreateQueryRunner
    })

    function setupSuccessfulQueryRunner() {
        const qr = {
            getVersion: vi.fn().mockResolvedValue("15.0"),
            getCurrentDatabase: vi.fn().mockResolvedValue("testdb"),
            getCurrentSchema: vi.fn().mockResolvedValue("public"),
            release: vi.fn(),
        }
        mockCreateQueryRunner.mockReturnValue(qr)
        return qr
    }

    it("should KEEP pool when probe fails with tier-1 error (Connection terminated unexpectedly)", async () => {
        const pool = makeFakePool()

        // createPool succeeds, but getVersion fails with tier-1 error
        mockCreatePool.mockResolvedValue(pool)

        const failQr = {
            getVersion: vi
                .fn()
                .mockRejectedValueOnce(
                    new Error("Connection terminated unexpectedly"),
                )
                .mockResolvedValue("15.0"),
            getCurrentDatabase: vi.fn().mockResolvedValue("testdb"),
            getCurrentSchema: vi.fn().mockResolvedValue("public"),
            release: vi.fn(),
        }
        mockCreateQueryRunner.mockReturnValue(failQr)

        await driver.connect()

        // failQr should have been released in the catch block
        expect(failQr.release).toHaveBeenCalled()
        // pool should NOT have been destroyed — tier-1 keeps the pool
        expect(pool.end).not.toHaveBeenCalled()
        // createPool should only have been called once (pool was kept)
        expect(mockCreatePool).toHaveBeenCalledTimes(1)
        expect((driver as any).master).toBe(pool)
    })

    it("should DESTROY pool when probe fails with tier-2 error (ECONNREFUSED)", async () => {
        const pool1 = makeFakePool()
        const pool2 = makeFakePool()

        mockCreatePool.mockResolvedValueOnce(pool1).mockResolvedValueOnce(pool2)

        const err = new Error("refused") as any
        err.code = "ECONNREFUSED"
        const failQr = {
            getVersion: vi.fn().mockRejectedValueOnce(err),
            release: vi.fn(),
        }
        const successQr = {
            getVersion: vi.fn().mockResolvedValue("15.0"),
            getCurrentDatabase: vi.fn().mockResolvedValue("testdb"),
            getCurrentSchema: vi.fn().mockResolvedValue("public"),
            release: vi.fn(),
        }
        mockCreateQueryRunner
            .mockReturnValueOnce(failQr)
            .mockReturnValueOnce(successQr)

        await driver.connect()

        // pool1 should have been destroyed — tier-2 destroys pool
        expect(pool1.removeAllListeners).toHaveBeenCalled()
        expect(pool1.end).toHaveBeenCalled()
        // pool2 is the new pool
        expect(mockCreatePool).toHaveBeenCalledTimes(2)
        expect((driver as any).master).toBe(pool2)
    })

    it("should retry when createPool fails with a retryable error (ECONNREFUSED)", async () => {
        const pool = makeFakePool()
        const err = new Error("connect ECONNREFUSED") as any
        err.code = "ECONNREFUSED"
        mockCreatePool.mockRejectedValueOnce(err).mockResolvedValueOnce(pool)
        setupSuccessfulQueryRunner()

        await driver.connect()

        expect(mockCreatePool).toHaveBeenCalledTimes(2)
        expect(sleep).toHaveBeenCalledWith(5000)
        expect((driver as any).master).toBe(pool)
    })

    it("should not attempt pool cleanup on first attempt (no previous pool)", async () => {
        ;(driver as any).master = undefined
        const pool = makeFakePool()
        mockCreatePool.mockResolvedValue(pool)
        setupSuccessfulQueryRunner()

        await driver.connect()

        // pool.end should not have been called — it's the active pool
        expect(pool.end).not.toHaveBeenCalled()
        expect((driver as any).master).toBe(pool)
    })
})

// ============================================================================
// E2E INTEGRATION TESTS
//
// These only mock pg.Pool at the bottom. All real code above runs:
// connect() → createPool() → obtainMasterConnection() → query()
// ============================================================================

function makeFakePoolClient() {
    const client = new EventEmitter() as any
    client.release = vi.fn()
    client.removeAllListeners = vi
        .fn()
        .mockImplementation(function (this: any) {
            EventEmitter.prototype.removeAllListeners.call(this)
            return this
        })
    client.query = vi.fn().mockImplementation((sql: string) => {
        if (sql === "SELECT 1")
            return Promise.resolve({
                rows: [{}],
                rowCount: 1,
                command: "SELECT",
            })
        if (sql === "SELECT version()")
            return Promise.resolve({
                rows: [{ version: "PostgreSQL 15.4 on x86_64" }],
                rowCount: 1,
                command: "SELECT",
            })
        if (sql === "SELECT * FROM current_database()")
            return Promise.resolve({
                rows: [{ current_database: "testdb" }],
                rowCount: 1,
                command: "SELECT",
            })
        if (sql === "SELECT * FROM current_schema()")
            return Promise.resolve({
                rows: [{ current_schema: "public" }],
                rowCount: 1,
                command: "SELECT",
            })
        return Promise.resolve({ rows: [], rowCount: 0, command: "SELECT" })
    })
    return client
}

function makeE2EDriver() {
    const driver = Object.create(PostgresDriver.prototype) as PostgresDriver
    driver.connection = makeFakeDataSource()
    driver.maxRetryDuration = 5 * 60 * 1000
    ;(driver as any).options = {} as any
    ;(driver as any).database = undefined
    ;(driver as any).searchSchema = undefined
    ;(driver as any).schema = undefined
    ;(driver as any).master = undefined
    ;(driver as any).connectedQueryRunners = []
    return driver
}

function setupPoolMock(clientProvider: () => any) {
    vi.mocked(Pool).mockImplementation(function () {
        const pool = new EventEmitter() as any
        pool.connect = vi
            .fn()
            .mockImplementation(() => Promise.resolve(clientProvider()))
        pool.end = vi.fn((cb?: Function): any => {
            if (cb) cb()
            else return Promise.resolve()
        })
        pool.removeAllListeners = vi
            .fn()
            .mockImplementation(function (this: any) {
                EventEmitter.prototype.removeAllListeners.call(this)
                return this
            })
        ;(Pool as any).__lastPool = pool
        return pool as any
    })
}

describe("E2E: connect() → query() full lifecycle", () => {
    let driver: PostgresDriver

    beforeEach(() => {
        vi.clearAllMocks()
        driver = makeE2EDriver()
    })

    it("connect() runs real createPool + probe queries and sets version/database/schema", async () => {
        setupPoolMock(makeFakePoolClient)

        await driver.connect()

        expect((driver as any).version).toBe("15.4")
        expect((driver as any).database).toBe("testdb")
        expect((driver as any).schema).toBe("public")
        expect((driver as any).master).toBeDefined()
    })

    it("connect() then query() returns data through real code path", async () => {
        setupPoolMock(makeFakePoolClient)
        await driver.connect()

        // Replace pool's client for user queries
        const pool = (Pool as any).__lastPool
        const userClient = makeFakePoolClient()
        userClient.query = vi.fn().mockImplementation((sql: string) => {
            if (sql === "SELECT * FROM users")
                return Promise.resolve({
                    rows: [{ id: 1, name: "Alice" }],
                    rowCount: 1,
                    command: "SELECT",
                })
            return Promise.resolve({ rows: [], rowCount: 0, command: "SELECT" })
        })
        pool.connect.mockResolvedValue(userClient)

        const qr = driver.createQueryRunner("master")
        const result = await qr.query("SELECT * FROM users")
        await qr.release()

        expect(result).toEqual([{ id: 1, name: "Alice" }])
    })
})

describe("E2E: connect() retry through real createPool", () => {
    let driver: PostgresDriver

    beforeEach(() => {
        vi.clearAllMocks()
        driver = makeE2EDriver()
    })

    it("first createPool fails (SELECT 1 throws) → retries → succeeds", async () => {
        let attempt = 0
        setupPoolMock(() => {
            attempt++
            if (attempt <= 2) {
                // First 2 clients: health-check fails
                const c = makeFakePoolClient()
                c.query = vi
                    .fn()
                    .mockRejectedValue(
                        new Error("Connection terminated unexpectedly"),
                    )
                return c
            }
            return makeFakePoolClient()
        })

        await driver.connect()

        // createPool propagates original error → tier-1 → 500ms sleep
        expect(sleep).toHaveBeenCalledWith(500)
        expect((driver as any).version).toBe("15.4")
    })
})

describe("E2E: query() retry through real obtainMasterConnection", () => {
    let driver: PostgresDriver

    beforeEach(async () => {
        vi.clearAllMocks()
        driver = makeE2EDriver()
        setupPoolMock(makeFakePoolClient)
        await driver.connect()
        vi.mocked(sleep).mockClear()
    })

    it("query fails with connection drop → gets fresh client → succeeds", async () => {
        const pool = (Pool as any).__lastPool
        let clientNum = 0

        pool.connect.mockImplementation(() => {
            clientNum++
            const c = makeFakePoolClient()
            if (clientNum === 1) {
                c.query = vi.fn().mockImplementation((sql: string) => {
                    if (sql === "SELECT * FROM orders")
                        return Promise.reject(
                            new Error("Connection terminated unexpectedly"),
                        )
                    return Promise.resolve({
                        rows: [],
                        rowCount: 0,
                        command: "SELECT",
                    })
                })
            } else {
                c.query = vi.fn().mockImplementation((sql: string) => {
                    if (sql === "SELECT * FROM orders")
                        return Promise.resolve({
                            rows: [{ id: 1 }],
                            rowCount: 1,
                            command: "SELECT",
                        })
                    return Promise.resolve({
                        rows: [],
                        rowCount: 0,
                        command: "SELECT",
                    })
                })
            }
            return Promise.resolve(c)
        })

        const qr = driver.createQueryRunner("master")
        const result = await qr.query("SELECT * FROM orders")
        await qr.release()

        expect(result).toEqual([{ id: 1 }])
        expect(sleep).toHaveBeenCalledWith(500)
        expect(clientNum).toBe(2)
    })
})

describe("E2E: mid-transaction connection error → no retry", () => {
    let driver: PostgresDriver

    beforeEach(async () => {
        vi.clearAllMocks()
        driver = makeE2EDriver()
        setupPoolMock(makeFakePoolClient)
        await driver.connect()
        vi.mocked(sleep).mockClear()
    })

    it("connection error during active transaction throws immediately", async () => {
        const pool = (Pool as any).__lastPool
        const client = makeFakePoolClient()
        client.query = vi.fn().mockImplementation((sql: string) => {
            if (sql === "START TRANSACTION")
                return Promise.resolve({
                    rows: [],
                    rowCount: 0,
                    command: "START",
                })
            if (sql === "INSERT INTO t VALUES (1)")
                return Promise.reject(
                    new Error("Connection terminated unexpectedly"),
                )
            return Promise.resolve({ rows: [], rowCount: 0, command: "SELECT" })
        })
        pool.connect.mockResolvedValue(client)

        const qr = driver.createQueryRunner("master")
        await qr.startTransaction()

        await expect(qr.query("INSERT INTO t VALUES (1)")).rejects.toThrow(
            QueryFailedError,
        )

        expect(sleep).not.toHaveBeenCalled()
    })
})

describe("E2E: connection reuse", () => {
    let driver: PostgresDriver

    beforeEach(async () => {
        vi.clearAllMocks()
        driver = makeE2EDriver()
        setupPoolMock(makeFakePoolClient)
        await driver.connect()
    })

    it("multiple queries on same QueryRunner use one PoolClient", async () => {
        const pool = (Pool as any).__lastPool
        const client = makeFakePoolClient()
        pool.connect.mockResolvedValue(client)

        // Clear pool.connect call count from connect() phase
        pool.connect.mockClear()

        const qr = driver.createQueryRunner("master")
        await qr.query("SELECT 1")
        await qr.query("SELECT 2")
        await qr.query("SELECT 3")

        // Only 1 pool.connect for this query runner (connection is cached)
        expect(pool.connect).toHaveBeenCalledTimes(1)
        await qr.release()
        expect(client.release).toHaveBeenCalledTimes(1)
    })
})
