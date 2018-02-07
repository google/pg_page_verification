# pg_page_verification

Keeping data safe is the top responsibility of anyone running a database.  To 
prevent against data loss, the Cloud SQL for PostgreSQL team has developed the
PostgreSQL Page Verification tool (pg_page_verification) to verify checksums
on PostgreSQL data pages without having to load each page into shared buffer
cache.  The tool currently skips the Free Space Map (FSM), Visibility Map (VM)
and pg_internal.init files since they can be regenerated.  It can be run when
the database process is online or offline and supports subsequent segments
for tables larger than 1GB.  Cloud SQL for PostgreSQL uses the tool at scale
to validate backups.

## Usage

./pg_page_verification -D /path/to/data/dir

## Disclaimer

This is not an official Google product.
