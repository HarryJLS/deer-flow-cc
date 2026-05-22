-- DDL for AsyncOceanBaseSaver (langgraph checkpoint persistence).
--
-- Schema mirrors langgraph's reference SQLite saver (2 tables) but uses the
-- MySQL/OceanBase column types and storage options:
--   * BLOB  -> LONGBLOB         (room for >16MB blobs; verify
--                                 max_allowed_packet on the server)
--   * TEXT  -> VARCHAR(128)     (kept short so the 3-/5-column composite PK
--                                 stays well under the 3072-byte InnoDB key
--                                 limit at utf8mb4)
--   * INTEGER -> INT
--   * INSERT OR REPLACE -> INSERT ... ON DUPLICATE KEY UPDATE (in code)
--   * INSERT OR IGNORE  -> INSERT IGNORE                       (in code)
--
-- Char set utf8mb4 + collation utf8mb4_unicode_ci match the rest of the
-- DeerFlow database so cross-table joins and CONVERT() filters Just Work.

CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id            VARCHAR(128) NOT NULL,
    checkpoint_ns        VARCHAR(128) NOT NULL DEFAULT '',
    checkpoint_id        VARCHAR(128) NOT NULL,
    parent_checkpoint_id VARCHAR(128),
    type                 VARCHAR(64),
    checkpoint           LONGBLOB,
    metadata             LONGBLOB,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
) DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS writes (
    thread_id     VARCHAR(128) NOT NULL,
    checkpoint_ns VARCHAR(128) NOT NULL DEFAULT '',
    checkpoint_id VARCHAR(128) NOT NULL,
    task_id       VARCHAR(128) NOT NULL,
    idx           INT NOT NULL,
    channel       VARCHAR(255) NOT NULL,
    type          VARCHAR(64),
    value         LONGBLOB,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
) DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
