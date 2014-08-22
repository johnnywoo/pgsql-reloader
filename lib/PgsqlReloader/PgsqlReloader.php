<?php

namespace Kola;

/**
 * To make reloading even faster:
 * 1. make a '@reloader-dump-skip' comment on tables you are sure don't need reloading
 * 2. UNSAFE: set fsync=off in your postgres.conf (your data will not survive but it speeds things up for tests)
 */
class PostgresReloader
{
    const SKIP_COMMENT = '@reloader-dump-skip'; // will not be escaped!

    /** @var resource */
    protected $connection;

    protected $uri;
    protected $dbName;
    protected $backupDbName;

    /**
     * @param string $uri pgsql://user:pass@host:port/dbname
     */
    public function __construct($uri)
    {
        $this->uri = $uri;

        $parts = parse_url($uri);

        $this->dbName       = trim($parts['path'], '/');
        $this->backupDbName = $this->dbName . '_production_backup';

        $this->reconnect($this->dbName);
    }

    /** @var string[] */
    protected $dumps = array();

    public function save($name)
    {
        $dump = '';

        $dump .= "SET statement_timeout = 0;\n";
        $dump .= "SET client_encoding = 'UTF8';\n";

        // disable triggers
        $dump .= "SET session_replication_role = replica;\n";

        $tables = $this->getTables();

        $dump .= "\n\n";

        // deleting everything from all tables
        foreach ($tables as $table) {
            $dump .= "DELETE FROM " . $this->escapeKey($table) . ";\n";
        }

        $dump .= "\n\n";

        // table data
        foreach ($tables as $table) {
            $resource = $this->query("SELECT * FROM " . $this->escapeKey($table));
            $data = pg_fetch_all($resource);
            if (empty($data)) {
                continue;
            }

            $fieldNames = array_keys($data[0]);
            $fieldTypes = array();

            foreach ($fieldNames as $i => $fieldName) {
                $fieldTypes[$fieldName] = pg_field_type($resource, $i);
            }

            $dump .= "INSERT INTO " . $this->escapeKey($table) . "\n";
            $dump .= "    (" . join(', ', array_map(array($this, 'escapeKey'), $fieldNames)) . ")\n";
            $dump .= "VALUES\n";
            foreach ($data as $i => $row) {
                $valuesSql = array();
                foreach ($row as $fieldName => $value) {
                    $valuesSql[] = $this->escape($value, $fieldTypes[$fieldName]);
                }

                $dump .= "    (" . join(', ', $valuesSql) . ")";
                if ($i + 1 != count($data)) {
                    $dump .= ",";
                }
                $dump .= "\n";
            }
            $dump .= ";\n";
        }

        $dump .= "\n\n";

        // sequences
        $sequenceExprs = array();
        foreach ($this->getSequenceValues() as $row) {
            $sequenceExprs[] = 'pg_catalog.setval('
                . $this->escape($this->escapeKey($row['sequence_name']))
                . ', ' . $this->escape($row['last_value'])
                . ', ' . ($row['is_called'] == 't' ? 'TRUE' : 'FALSE')
                . ')'
            ;
        }
        if (!empty($sequenceExprs)) {
            $dump .= "SELECT\n    " . join(",\n    ", $sequenceExprs) . ";\n";
        }

        $this->dumps[$name] = $dump;
    }

    public function restore($name)
    {
        if (!isset($this->dumps[$name])) {
            return false;
        }

        $this->query($this->dumps[$name]);

        return true;
    }

    public function saveProduction()
    {
        if ($this->databaseExists($this->backupDbName)) {
            return;
            // We don't fail here if there was a production dump already.
            //
            // That means some test died and restoreProduction() was not called.
            // Therefore the DB is likely in a (broken) test state now.
            // We don't want to overwrite a production dump with a broken test one.
            //
            // Next correct test run will properly call restoreProduction(),
            // destroying the dump, and the database state will become normal again.
        }

        $this->terminateConnectionsToDatabase($this->dbName);
        $this->reconnect('postgres');
        $this->query('ALTER DATABASE ' . $this->escapeKey($this->dbName) . ' RENAME TO ' . $this->escapeKey($this->backupDbName));
        $this->query('CREATE DATABASE ' . $this->escapeKey($this->dbName));
        $this->reconnect($this->dbName);
    }

    public function restoreProduction()
    {
        if (!$this->databaseExists($this->backupDbName)) {
            throw new PostgresReloaderException('Cannot restore production database: no backup found');
        }

        $this->terminateConnectionsToDatabase($this->dbName);
        $this->reconnect('postgres');
        $this->query('DROP DATABASE ' . $this->escapeKey($this->dbName));
        $this->query('ALTER DATABASE ' . $this->escapeKey($this->backupDbName) . ' RENAME TO ' . $this->escapeKey($this->dbName));
        $this->reconnect($this->dbName);
    }


    //
    // DUMPER
    //

    /**
     * @return string[]
     */
    protected function getTables()
    {
        // select all tables that are not marked with a skip-comment
        return $this->column("
            SELECT c.relname
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
                AND c.relkind = 'r'
                AND NOT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_description d
                    WHERE d.objoid = c.oid
                        AND d.description LIKE '%" . static::SKIP_COMMENT . "%'
                )
            ORDER BY 1
        ");
    }

    /**
     * @return array
     */
    protected function getSequenceValues()
    {
        $names = $this->column("
            SELECT c.relname
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
                AND c.relkind = 'S'
                AND NOT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_description d
                    WHERE d.objoid = c.oid
                        AND d.description LIKE '%" . static::SKIP_COMMENT . "%'
                )
            ORDER BY 1
        ");

        if (empty($names)) {
            return null;
        }

        $valueStatements = array();
        $i = 0;
        foreach ($names as $name) {
            $i++;
            $valueStatements[] = 'SELECT sequence_name, last_value, is_called FROM ' . $this->escapeKey($name);
        }
        return $this->select(join(' UNION ALL ', $valueStatements));
    }


    //
    // DATABASE RENAMER
    //

    protected function databaseExists($dbName)
    {
        return (bool) $this->cell('SELECT datname FROM pg_database WHERE NOT datistemplate AND datname = ' . $this->escape($dbName));
    }

    protected function terminateConnectionsToDatabase($dbName)
    {
        // Нельзя удалить базу пока к ней есть коннекты. Запрос на удаление зависит от версии постгреса.
        $version = $this->cell('SELECT version()');
        if (preg_match('/PostgreSQL (\d\.\d)/', $version, $match) && version_compare($match[1], '9.2', '>=')) {
            $this->query('
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = ' . $this->escape($dbName) . '
                    AND pid <> pg_backend_pid()
            ');
        } else {
            $this->query('
                SELECT pg_terminate_backend(pg_stat_activity.procpid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = ' . $this->escape($dbName) . '
                    AND procpid <> pg_backend_pid()
            ');
        }
    }


    //
    // DATABASE
    //

    protected function reconnect($dbName)
    {
        if ($this->connection) {
            pg_close($this->connection);
        }

        $parts = parse_url($this->uri);

        $dsn = 'dbname=' . $dbName;
        if ($parts['user']) {
            $dsn .= ' user=' . $parts['user'];
        }
        if ($parts['pass']) {
            $dsn .= ' password=' . $parts['pass'];
        }
        if ($parts['host']) {
            $dsn .= ' host=' . $parts['host'];
        }
        if ($parts['port']) {
            $dsn .= ' port=' . $parts['port'];
        }

        $this->connection = pg_connect($dsn);

        if (!$this->connection) {
            throw new PostgresReloaderException("Error: cannot connect to Postgres at {$dsn}");
        }
    }

    /**
     * @param mixed $value
     * @param string $fieldType
     * @return string
     */
    protected function escape($value, $fieldType = 'varchar')
    {
        if ($value === null) {
            return 'NULL';
        }
        switch ($fieldType) {
            case 'timestamp':
            case 'varchar':
            case 'text':
            case 'bpchar':
            case 'geography':
            case 'inet':
            case 'uuid':
                return pg_escape_literal($this->connection, $value);

            case 'numeric':
            case 'int4':
                return $value;

            case 'bool':
                return $value == 't' ? 'TRUE' : 'FALSE';

            default:
                throw new PostgresReloaderException("Error: unknown field type {$fieldType} value " . var_export($value, true));
        }
    }

    protected function escapeKey($key)
    {
        return pg_escape_identifier($this->connection, $key);
    }

    protected function query($sql)
    {
        $res = pg_query($this->connection, $sql);
        if ($res === false) {
            throw new PostgresReloaderException('Error in SQL: ' . pg_last_error($this->connection));
        }
        return $res;
    }

    protected function select($sql)
    {
        return pg_fetch_all($this->query($sql));
    }

    protected function column($sql)
    {
        $list = array();
        foreach (pg_fetch_all($this->query($sql)) as $row) {
            $list[] = reset($row);
        }
        return $list;
    }

    protected function cell($sql)
    {
        return pg_fetch_result($this->query($sql), 0);
    }
}
