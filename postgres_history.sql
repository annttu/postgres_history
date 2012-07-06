/* Created 6.7.2012 by Antti 'Annttu' Jaakkola

I was to lazy to create history tables for all my tables so I created 
postgresql function to do them easily.

Related:
http://bytes.com/topic/postgresql/answers/732169-syntax-view-structure-table-using-sql
http://bytes.com/topic/postgresql/answers/172978-sql-command-list-tables
*/
/*
-- List tables on schema

SELECT n.nspname as "Schema",
c.relname as "Name",
CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'i' THEN
'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' END as "Type",
u.usename as "Owner"
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_user u ON u.usesysid = c.relowner
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','')
AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;

-- Get table schema

SELECT
  a.attnum,
  a.attname AS field,
  t.typname AS type,
  a.attlen AS length,
  a.atttypmod AS lengthvar,
  a.attnotnull AS notnull
FROM
  pg_class c,
  pg_attribute a,
  pg_type t
WHERE
  c.relname = 'your_table_name'
  AND a.attnum > 0
  AND a.attrelid = c.oid
  AND a.atttypid = t.oid
  ORDER BY a.attnum

*/
-- combine these two into function to create history table --

CREATE TYPE event_type AS ENUM ('INSERT','UPDATE', 'DELETE');

CREATE OR REPLACE FUNCTION create_history_table(tablename text)
RETURNS VOID 
AS $$
DECLARE
    historytable text;
    oldcols text;
    cols text;
    col RECORD;
BEGIN
IF tablename IS NULL OR tablename = '' THEN
    RAISE EXCEPTION 'No table name given';
ELSE
    oldcols := '';
    cols := '';
    historytable := tablename || '_history';
    EXECUTE 'DROP RULE IF EXISTS ' || tablename || '_delete_historize ON ' || tablename;
    EXECUTE 'DROP RULE IF EXISTS ' || tablename || '_update_historize ON ' || tablename;
    EXECUTE 'DROP TABLE IF EXISTS ' || historytable;
    EXECUTE 'CREATE TABLE ' || historytable || ' (' || historytable || '_id serial PRIMARY KEY, 
            historized timestamptz DEFAULT NOW(), operation event_type, 
            old_xmin integer default 0, old_xmax integer default 0)';
    FOR col IN 
            SELECT
              a.attnum,
              a.attname AS field,
              t.typname AS type
            FROM
              pg_class c,
              pg_attribute a,
              pg_type t
            WHERE
              c.relname = tablename
              AND a.attnum > 0
              AND a.attrelid = c.oid
              AND a.atttypid = t.oid
              ORDER BY a.attnum 
        LOOP
            EXECUTE 'ALTER TABLE ' || historytable || ' ADD COLUMN ' || col.field || ' ' || col.type;
            IF oldcols = '' THEN
                oldcols := 'OLD.' || col.field;
                cols := col.field;
            ELSE
                oldcols := oldcols || ', OLD.' || col.field;
                cols := cols || ', ' || col.field;  
            END IF;
        END LOOP;
        EXECUTE 'CREATE RULE ' || tablename || '_delete_historize AS ON DELETE TO ' || tablename || ' DO ALSO 
            INSERT INTO ' || historytable || ' ( operation, old_xmax, old_xmin, ' || cols || ' ) SELECT 
            ' || quote_literal('DELETE') || ', cast(txid_current() as text)::integer, 
            cast(OLD.xmin as text)::integer, ' || oldcols;
        EXECUTE 'CREATE RULE ' || tablename || '_update_historize AS ON UPDATE TO ' || tablename || ' DO ALSO 
            INSERT INTO ' || historytable || ' ( operation, old_xmax, old_xmin, ' || cols || ' ) SELECT 
            ' || quote_literal('UPDATE') || ', cast(txid_current() as text)::integer, 
            cast(OLD.xmin as text)::integer, ' || oldcols;
    END IF;
END;       
$$ LANGUAGE plpgsql;