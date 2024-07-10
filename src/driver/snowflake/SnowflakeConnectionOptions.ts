import { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions"

export interface SnowflakeConnectionOptions extends BaseDataSourceOptions {
    /**
     * Database type.
     */
    readonly type: "snowflake"

    /**
     * The full name of your account (provided by Snowflake). Note that your full account name might include additional segments
     * that identify the region and cloud platform where your account is hosted.
     */
    account: string

    /**
     * Snowflake user login name to connect with.
     */
    username: string

    /**
     * Password for the user. Set this option if you set the authenticator option to SNOWFLAKE or the Okta URL endpoint for your
     * Okta account (e.g. https://<okta_account_name>.okta.com) or if you left the authenticator option unset.
     */
    password?: string

    /**
     * @deprecated
     * The ID for the region where your account is located.
     *
     * This parameter is no longer used because the region information, if required, is included as part of the full account name.
     * It is documented here only for backward compatibility
     */
    region?: string | undefined

    /**
     * The default database to use for the session after connecting.
     */
    database?: string | undefined

    /**
     * The default schema to use for the session after connecting.
     */
    schema?: string | undefined

    /**
     * The default virtual warehouse to use for the session after connecting. Used for performing queries, loading data, etc.
     */
    warehouse?: string | undefined

    /**
     * The default security role to use for the session after connecting.
     */
    role?: string | undefined

    /**
     * Number of milliseconds to keep the connection alive with no response. Default: 60000 (1 minute).
     */
    timeout?: number | undefined

    /**
     * By default, client connections typically time out approximately 3-4 hours after the most recent query was executed.
     *
     * If the parameter clientSessionKeepAlive is set to true, the client’s connection to the server will be kept alive
     * indefinitely, even if no queries are executed.
     *
     * The default setting of this parameter is false.
     *
     * If you set this parameter to true, make sure that your program explicitly disconnects from the server when your program
     * has finished. Do not exit without disconnecting.
     */
    clientSessionKeepAlive?: boolean | undefined

    /**
     * (Applies only when `clientSessionKeepAlive` is true)
     *
     * This parameter sets the frequency (interval in seconds) between heartbeat messages.
     *
     * You can loosely think of a connection heartbeat message as substituting for a query and restarting the timeout countdown
     * for the connection. In other words, if the connection would time out after at least 4 hours of inactivity, the heartbeat
     * resets the timer so that the timeout will not occur until at least 4 hours after the most recent heartbeat (or query).
     *
     * The default value is 3600 seconds (one hour). The valid range of values is 900 - 3600. Because timeouts usually occur after
     * at least 4 hours, a heartbeat every 1 hour is normally sufficient to keep the connection alive. Heartbeat intervals of less
     * than 3600 seconds are rarely necessary or useful.
     */
    clientSessionKeepAliveHeartbeatFrequency?: number | undefined

    jsTreatIntegerAsBigInt?: boolean | undefined

    /**
     * Specifies the name of the client application connecting to Snowflake.
     */
    application?: string

    /**
     * Specifies the authenticator to use for verifying user login credentials. You can set this to one of the following values:
     *  - SNOWFLAKE: Use the internal Snowflake authenticator. You must also set the password option.
     *  - EXTERNALBROWSER: Use your web browser to authenticate with Okta, ADFS, or any other SAML 2.0-compliant identity provider
     *    (IdP) that has been defined for your account.
     *  - https://<okta_account_name>.okta.com: Use Native SSO through Okta.
     *  - OAUTH: Use OAuth for authentication. You must also set the token option to the OAuth token (see below).
     *  - SNOWFLAKE_JWT: Use key pair authentication. See Using Key Pair Authentication & Key Pair Rotation.
     * The default value is SNOWFLAKE.
     * For more information on authentication, see {@link https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html Managing/Using Federated Authentication}
     *  and {@link https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use.html OAuth with Clients, Drivers, and Connectors}.
     */
    authenticator?: string

    /**
     * Specifies the OAuth token to use for authentication. Set this option if you set the authenticator option to OAUTH.
     */
    token?: string

    /**
     * Specifies the private key (in PEM format) for key pair authentication.
     * For details, see {@link https://docs.snowflake.com/en/user-guide/nodejs-driver-use.html#label-nodejs-key-pair-authentication Using Key Pair Authentication & Key Pair Rotation}.
     */
    privateKey?: string

    /**
     * Specifies the local path to the private key file (e.g. rsa_key.p8).
     * For details, see {@link https://docs.snowflake.com/en/user-guide/nodejs-driver-use.html#label-nodejs-key-pair-authentication Using Key Pair Authentication & Key Pair Rotation}.
     */
    privateKeyPath?: string

    /**
     * Specifies the passcode to decrypt the private key file, if the file is encrypted.
     * For details, see {@link https://docs.snowflake.com/en/user-guide/nodejs-driver-use.html#label-nodejs-key-pair-authentication Using Key Pair Authentication & Key Pair Rotation}.
     */
    privateKeyPass?: string
}
