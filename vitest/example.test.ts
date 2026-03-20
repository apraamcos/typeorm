import { describe, expect, it } from "vitest"
import { Entity, PrimaryGeneratedColumn, Column } from "../src"
import { getMetadataArgsStorage } from "../src"

/**
 * Sample test to verify Vitest + SWC setup works correctly,
 * including TypeScript decorators and emitDecoratorMetadata.
 */

@Entity()
class SampleUser {
    @PrimaryGeneratedColumn()
    id: number

    @Column()
    name: string

    @Column({ nullable: true })
    email: string
}

describe("Vitest setup verification", () => {
    it("should run a basic assertion", () => {
        expect(1 + 1).toBe(2)
    })

    it("should handle TypeORM decorators and metadata", () => {
        const storage = getMetadataArgsStorage()

        // Verify that the @Entity decorator registered metadata
        const tableMetadata = storage.tables.find(
            (t) => t.target === SampleUser,
        )
        expect(tableMetadata).toBeDefined()
        expect(tableMetadata!.type).toBe("regular")

        // Verify that @Column decorators registered metadata
        const columnMetadata = storage.columns.filter(
            (c) => c.target === SampleUser,
        )
        expect(columnMetadata.length).toBeGreaterThanOrEqual(2)

        const nameColumn = columnMetadata.find((c) => c.propertyName === "name")
        expect(nameColumn).toBeDefined()
    })
})
