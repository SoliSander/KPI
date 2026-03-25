CREATE OR REPLACE FUNCTION public.get_database_table_sizes(
	dbname text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    rec record;
    counter int := 1;
    result_text text := '';
    total_bytes bigint := 0;
    db_size_pretty text;
BEGIN
    SELECT pg_size_pretty(pg_database_size(dbname))
    INTO db_size_pretty;

    result_text := result_text || 'Database: ' || dbname || ' (' || db_size_pretty || ')' || E'\n\n';

    FOR rec IN
        SELECT 
            table_schema,
            table_name,
            pg_total_relation_size(format('%I.%I', table_schema, table_name)) AS total_size,
            pg_size_pretty(pg_total_relation_size(format('%I.%I', table_schema, table_name))) AS size_pretty
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY pg_total_relation_size(format('%I.%I', table_schema, table_name)) DESC
    LOOP
        result_text := result_text ||
                       counter || '. ' || rec.table_schema || '.' || rec.table_name ||
                       ': ' || rec.size_pretty || E'\n';

        total_bytes := total_bytes + rec.total_size;
        counter := counter + 1;
    END LOOP;

    result_text := result_text || E'\nTotal size of all tables: ' ||
                    pg_size_pretty(total_bytes) || E'\n';

    RETURN result_text;
END;
$BODY$;

ALTER FUNCTION public.get_database_table_sizes(text)
    OWNER TO kpi;