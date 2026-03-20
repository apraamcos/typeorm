import swc from "unplugin-swc"
import { defineConfig } from "vitest/config"

// Optional peer dependencies that may not be installed.
// These are externalized so Vite/Vitest doesn't try to resolve them.
const optionalDeps = [
    "pg",
    "pg-native",
    "pg-query-stream",
    "better-sqlite3",
    "sqlite3",
    "sql.js",
    "mssql",
    "mysql2",
    "oracledb",
    "mongodb",
    "redis",
    "ioredis",
    "@sap/hana-client",
    "@google-cloud/spanner",
    "typeorm-aurora-data-api-driver",
    "snowflake-sdk",
]

export default defineConfig({
    plugins: [
        swc.vite({
            jsc: {
                parser: {
                    syntax: "typescript",
                    decorators: true,
                },
                transform: {
                    legacyDecorator: true,
                    decoratorMetadata: true,
                },
                target: "es2021",
            },
        }),
    ],
    // Disable Oxc transform since SWC handles TypeScript transformation
    // (suppresses the "esbuild option is set to false" warning from unplugin-swc)
    oxc: false as any,
    resolve: {
        alias: Object.fromEntries(
            optionalDeps.map((dep) => [
                dep,
                // Redirect to a virtual empty module so Vite doesn't
                // fail when resolving optional peer deps from source
                `data:text/javascript,export default {}`,
            ]),
        ),
    },
    test: {
        include: ["vitest/**/*.test.ts"],
        environment: "node",
        testTimeout: 10_000,
        restoreMocks: true,
    },
})
