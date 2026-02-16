# Backup Schedules

Backup Schedules can be configured the Admin GUI in the "Backup" tab.

Schedule configuration:
- Destination: "File" is supported. The backups will e written to the `backup` dir.
- DB
- Collection: `*` for all collections or collection name
- Schedule: String with `"minute hour dayOfMonth month dayOfWeek"`, e.g. `"0 0 * * *"` (= each day at midnight)
- Retention

Each database pod will backup his data. 
So if your replication is 3, each documents is also stored in 3 backups.

# Ad Hoc Backups

Define in the form:
- DB
- Collection: `*` for all collections or collection name

and click "Execute backup"

# Restore Backups

In the backup table choose one backup and click the restore link to open the restore form.

*Hint: If the restore option is not shown in the backup table, one backup might not be in status "OK". You can fix this by open a console in the pods and set all backups to "OK" manually in the "admin" database, see [[admin-problems.md|fixing problems]].*

If multiple collections are included in the backup, choose one.

To restore the index check the respective field. Expire indexes can be excluded to restore already expired data.

The restore will create a new collection with a new name: The backup date will be extended to the original collection name. By this you can
1. copy selected documents to the original table
2. delete the original table and rename the restored table to the original name