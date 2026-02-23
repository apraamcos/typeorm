import { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions"
import type { ConnectionOptions as SfConnectionOptions } from "snowflake-sdk"

/**
 * Properties picked directly from `snowflake-sdk`'s `ConnectionOptions`.
 * JSDoc is inherited from the SDK — no need to duplicate.
 *
 * Required by TypeORM: `account`, `username`.
 * Optional pass-through: everything else the SDK supports that makes sense
 * for a pooled ORM connection.
 */
type SfPassthrough = Pick<
    SfConnectionOptions,
    | "password"
    | "region"
    | "accessUrl"
    | "host"
    | "database"
    | "schema"
    | "warehouse"
    | "role"
    | "timeout"
    | "clientSessionKeepAlive"
    | "clientSessionKeepAliveHeartbeatFrequency"
    | "jsTreatIntegerAsBigInt"
    | "application"
    | "authenticator"
    | "token"
    | "privateKey"
    | "privateKeyPath"
    | "privateKeyPass"
    | "proxyHost"
    | "proxyPort"
    | "proxyProtocol"
    | "proxyUser"
    | "proxyPassword"
    | "noProxy"
    | "queryTag"
    | "fetchAsString"
    | "arrayBindingThreshold"
    | "resultPrefetch"
    | "retryTimeout"
    | "clientRequestMFAToken"
    | "clientStoreTemporaryCredential"
    | "credentialCacheDir"
    | "passcode"
    | "passcodeInPassword"
    | "browserActionTimeout"
    | "disableConsoleLogin"
    | "validateDefaultParameters"
    // OAuth options
    | "oauthClientId"
    | "oauthClientSecret"
    | "oauthAuthorizationUrl"
    | "oauthTokenRequestUrl"
    | "oauthScope"
    | "oauthRedirectUri"
    | "oauthChallengeMethod"
    | "oauthEnableSingleUseRefreshTokens"
    // Workload Identity options
    | "workloadIdentityProvider"
    | "workloadIdentityImpersonationPath"
    | "workloadIdentityAzureEntraIdResource"
    | "workloadIdentityAzureClientId"
    // CRL validation options
    | "certRevocationCheckMode"
    | "crlAllowCertificatesWithoutCrlURL"
    | "crlInMemoryCache"
    | "crlOnDiskCache"
>

export interface SnowflakeConnectionOptions
    extends BaseDataSourceOptions,
        SfPassthrough {
    /**
     * Database type.
     */
    readonly type: "snowflake"

    /**
     * Your account identifier (required).
     */
    readonly account: string

    /**
     * Snowflake user login name.
     * Required for password/key-pair auth, optional for OAuth/token/workload-identity auth.
     */
    readonly username?: string

    /**
     * Connection pool options. Uses generic-pool under the hood (via snowflake-sdk's createPool).
     *
     * generic-pool defaults are already Lambda-friendly (max:1, min:0,
     * testOnBorrow:false, evictionRunIntervalMillis:0). We only override
     * `acquireTimeoutMillis` to 30s (generic-pool default is `null` = wait
     * forever, which would hang Lambda).
     *
     * All options are passed through to generic-pool. For long-running servers,
     * consider overriding:
     *  - max: 10 (or higher)
     *  - testOnBorrow: true (validate connection health before use)
     *  - acquireTimeoutMillis: 120000 (allow warehouse resume time)
     *  - evictionRunIntervalMillis: 60000 (enable idle connection cleanup)
     *  - clientSessionKeepAlive: true (on the top-level options, not pool)
     *
     * @see https://docs.snowflake.com/en/developer-guide/node-js/nodejs-driver-connect#creating-a-connection-pool
     */
    pool?: {
        /** Maximum number of connections in the pool. Default: 1 */
        max?: number
        /** Minimum number of connections in the pool. Default: 0 */
        min?: number
        /** Maximum queued acquire requests. Default: unlimited */
        maxWaitingClients?: number
        /** Validate connection before handing it out. Default: false */
        testOnBorrow?: boolean
        /** Validate connection when returning to pool. Default: false */
        testOnReturn?: boolean
        /** Max time (ms) to wait for a connection from the pool. Default: 30000 (our override; generic-pool default is null = no timeout) */
        acquireTimeoutMillis?: number
        /** Whether to use FIFO (true) or LIFO (false) for idle connections. Default: true */
        fifo?: boolean
        /** How often (ms) to run the idle connection evictor. 0 = disabled. Default: 0 */
        evictionRunIntervalMillis?: number
        /** Number of connections to check per eviction run. Default: 3 */
        numTestsPerEvictionRun?: number
        /** Idle time (ms) before eviction when pool size > min. Default: -1 (disabled) */
        softIdleTimeoutMillis?: number
        /** Idle time (ms) before unconditional eviction. Default: 30000 */
        idleTimeoutMillis?: number
        /** Priority range for acquire requests (0 = highest). Default: 1 */
        priorityRange?: number
        /** Whether to start the pool immediately. Default: true */
        autostart?: boolean
    }

    /**
     * Snowflake SDK global log level.
     *
     * Passed to `snowflake-sdk`'s `configure({ logLevel })` on first connect.
     * Default: `"ERROR"`.
     *
     * @see https://docs.snowflake.com/en/developer-guide/node-js/nodejs-driver-configure
     */
    sdkLogLevel?: "ERROR" | "WARN" | "INFO" | "DEBUG" | "TRACE" | "OFF"

    /**
     * TypeORM's generic `poolSize` option is not applicable to Snowflake.
     * Snowflake pool sizing is configured via the `pool.max` / `pool.min`
     * options which map directly to generic-pool's API.
     *
     * Declaring this as `never` causes a compile-time error if someone
     * accidentally sets `poolSize`, preventing silent misconfiguration.
     */
    poolSize?: never
}
