const fs = require("fs");
const { R } = require("redbean-node");
const { setSetting, setting } = require("./util-server");
const { log, sleep } = require("../src/util");
const dayjs = require("dayjs");
const knex = require("knex");
const { PluginsManager } = require("./plugins-manager");

/**
 * Database & App Data Folder
 */
class Database {
    /**
     * Data Dir (Default: ./data)
     */
    static dataDir;

    /**
     * User Upload Dir (Default: ./data/upload)
     */
    static uploadDir;

    static path;

    /**
     * @type {boolean}
     */
    static patched = false;

    /**
     * For Backup only
     */
    static backupPath = null;

    static noReject = true;

    /**
     * Initialize the database
     * @param {Object} args Arguments to initialize DB with
     */
    static init(args) {
        // Data Directory (must be end with "/")
        Database.dataDir =
            process.env.DATA_DIR || args["data-dir"] || "./data/";

        // Plugin feature is working only if the dataDir = "./data";
        if (Database.dataDir !== "./data/") {
            log.warn(
                "PLUGIN",
                "Warning: In order to enable plugin feature, you need to use the default data directory: ./data/"
            );
            PluginsManager.disable = true;
        }

        Database.path = Database.dataDir + "kuma.db";
        if (!fs.existsSync(Database.dataDir)) {
            fs.mkdirSync(Database.dataDir, { recursive: true });
        }

        Database.uploadDir = Database.dataDir + "upload/";

        if (!fs.existsSync(Database.uploadDir)) {
            fs.mkdirSync(Database.uploadDir, { recursive: true });
        }

        log.info("db", `Data Dir: ${Database.dataDir}`);
    }

    static async connect(testMode = false) {
        const knexConfig = require("../knexfile.js");
        knexConfig.setPath(Database.path);

        Database.dialect = knexConfig.getDialect();

        const knexInstance = knex(knexConfig["development"]);

        await knexInstance.migrate.latest();

        R.setup(knexInstance);

        if (process.env.SQL_LOG === "1") {
            R.debug(true);
        }

        // Auto map the model to a bean object
        R.freeze(true);

        if (autoloadModels) {
            await R.autoloadModels("./server/model");
        }

        if (Database.dialect == "sqlite3") {
            await R.exec("PRAGMA foreign_keys = ON");
            if (testMode) {
                // Change to MEMORY
                await R.exec("PRAGMA journal_mode = MEMORY");
            } else {
                // Change to WAL
                await R.exec("PRAGMA journal_mode = WAL");
            }
            await R.exec("PRAGMA cache_size = -12000");
            await R.exec("PRAGMA auto_vacuum = FULL");

            console.log("SQLite config:");
            console.log(await R.getAll("PRAGMA journal_mode"));
            console.log(await R.getAll("PRAGMA cache_size"));
            console.log(
                "SQLite Version: " +
                    (await R.getCell("SELECT sqlite_version()"))
            );
        }
    }

    /**
     * Aquire a direct connection to database
     * @returns {any}
     */
    static getBetterSQLite3Database() {
        return R.knex.client.acquireConnection();
    }

    /**
     * Special handle, because tarn.js throw a promise reject that cannot be caught
     * @returns {Promise<void>}
     */
    static async close() {
        const listener = (reason, p) => {
            Database.noReject = false;
        };
        process.addListener("unhandledRejection", listener);

        log.info("db", "Closing the database");

        while (true) {
            Database.noReject = true;
            await R.close();
            await sleep(2000);

            if (Database.noReject) {
                break;
            } else {
                log.info("db", "Waiting to close the database");
            }
        }
        console.log("Database closed");

        process.removeListener("unhandledRejection", listener);
    }

    /**
     * One backup one time in this process.
     * Reset this.backupPath if you want to backup again
     * @param {string} version Version code of backup
     */
    static backup(version) {
        if (Database.dialect !== "sqlite3") return;

        if (!this.backupPath) {
            log.info("db", "Backing up the database");
            this.backupPath = this.dataDir + "kuma.db.bak" + version;
            fs.copyFileSync(Database.path, this.backupPath);

            const shmPath = Database.path + "-shm";
            if (fs.existsSync(shmPath)) {
                this.backupShmPath = shmPath + ".bak" + version;
                fs.copyFileSync(shmPath, this.backupShmPath);
            }

            const walPath = Database.path + "-wal";
            if (fs.existsSync(walPath)) {
                this.backupWalPath = walPath + ".bak" + version;
                fs.copyFileSync(walPath, this.backupWalPath);
            }

            // Double confirm if all files actually backup
            if (!fs.existsSync(this.backupPath)) {
                throw new Error("Backup failed! " + this.backupPath);
            }

            if (fs.existsSync(shmPath)) {
                if (!fs.existsSync(this.backupShmPath)) {
                    throw new Error("Backup failed! " + this.backupShmPath);
                }
            }

            if (fs.existsSync(walPath)) {
                if (!fs.existsSync(this.backupWalPath)) {
                    throw new Error("Backup failed! " + this.backupWalPath);
                }
            }
        }
    }

    /** Restore from most recent backup */
    static restore() {
        if (Database.dialect !== "sqlite3") return;

        if (this.backupPath) {
            log.error(
                "db",
                "Patching the database failed!!! Restoring the backup"
            );

            const shmPath = Database.path + "-shm";
            const walPath = Database.path + "-wal";

            // Make sure we have a backup to restore before deleting old db
            if (
                !fs.existsSync(this.backupPath) &&
                !fs.existsSync(shmPath) &&
                !fs.existsSync(walPath)
            ) {
                log.error(
                    "db",
                    "Backup file not found! Leaving database in failed state."
                );
                process.exit(1);
            }

            // Delete patch failed db
            try {
                if (fs.existsSync(Database.path)) {
                    fs.unlinkSync(Database.path);
                }

                if (fs.existsSync(shmPath)) {
                    fs.unlinkSync(shmPath);
                }

                if (fs.existsSync(walPath)) {
                    fs.unlinkSync(walPath);
                }
            } catch (e) {
                log.error(
                    "db",
                    "Restore failed; you may need to restore the backup manually"
                );
                process.exit(1);
            }

            // Restore backup
            fs.copyFileSync(this.backupPath, Database.path);

            if (this.backupShmPath) {
                fs.copyFileSync(this.backupShmPath, shmPath);
            }

            if (this.backupWalPath) {
                fs.copyFileSync(this.backupWalPath, walPath);
            }
        } else {
            log.info("db", "Nothing to restore");
        }
    }

    /** Get the size of the database */
    static getSize() {
        if (Database.dialect !== "sqlite3")
            throw { message: "DB size is only supported on SQLite" };

        debug("Database.getSize()");
        let stats = fs.statSync(Database.path);
        log.debug("db", stats);
        return stats.size;
    }

    /**
     * Shrink the database
     * @returns {Promise<void>}
     */
    static async shrink() {
        if (Database.dialect !== "sqlite3")
            throw { message: "VACUUM is only supported on SQLite" };

        return R.exec("VACUUM");
    }
}

module.exports = Database;
