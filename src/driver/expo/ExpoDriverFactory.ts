import { DataSource } from "../../data-source"
import { ExpoDriver } from "./ExpoDriver"
import { ExpoLegacyDriver } from "./legacy/ExpoLegacyDriver"

export class ExpoDriverFactory {
    connection: DataSource

    constructor(connection: DataSource) {
        this.connection = connection
    }

    create(): ExpoDriver | ExpoLegacyDriver {
        return new ExpoDriver(this.connection)
    }
}
