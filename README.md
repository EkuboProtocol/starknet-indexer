# `@ekubo/indexer`

Service for indexing Ekubo events into a Postgres database.

## Overview

The indexer focuses on producing an always-consistent realtime view of Ekubo events, using the Apibara service to get a stream of relevant data.

Events are not transformed by the indexer, simply cataloged for later use such as in materialized views or complex analytical queries.

## Syncing a new node

It can take weeks to sync a new mainnet node, so it's recommended to start by using
the [`pg_restore`](https://www.postgresql.org/docs/current/app-pgrestore.html) utility with
the [latest backup](https://github.com/EkuboProtocol/indexer/actions/workflows/backup.yml) of our production database.

The backup is in the directory format, and can be imported more quickly in parallel using the `--jobs` parameter
of `pg_restore`. To utilize the backup:

- Download the `Backup.zip` artifact from the latest workflow run
- Extract the directory
- `pg_restore --dbname=mainnet --format=d --jobs=16 --no-owner --no-privileges --disable-triggers ~/Downloads/Production\ Backups`

After restoring from the backup, you can start the indexer and it should begin at the last block that was synced before the backup started.
