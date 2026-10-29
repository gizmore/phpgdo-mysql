# phpgdo-mysql

GDOv7 MySQL/MariaDB DBMS.

Since phgdo 7.0.2 module DBMS is a required multi-provider-module.

`dbmsEscapeSearch()` prepares literal user text for interpolation inside a
quoted MySQL/MariaDB `LIKE` pattern: quotes and backslashes remain data, and
`%` and `_` are treated literally rather than as caller-supplied wildcards.
This follows the module's existing backslash-escaping SQL-mode assumption.

### Supported DBMS

The following DBMS are supported by phpgdo.

- [MySQL/MariaDB](./README.md)
- [SQLite](https://github.com/gizmore/phpgdo-sqlite) (NEW)
- [Postgres](https://github.com/gizmore/phpgdo-postgres) (TODO)
