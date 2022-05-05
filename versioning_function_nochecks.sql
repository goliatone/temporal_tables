CREATE OR REPLACE FUNCTION versioning()
RETURNS TRIGGER AS $$
DECLARE
  sys_period text;
  history_table text;
  manipulate jsonb;
  ignore_unchanged_values bool;
  common_columns text[];
  time_stamp_to_use timestamptz := current_timestamp;
  range_lower timestamptz;
  transaction_info txid_snapshot;
  existing_range tstzrange;
  copy_new jsonb;
  copy_old jsonb;
  json_new jsonb;
  json_old jsonb;
  name_col text;
BEGIN
  -- version 0.4.0

  sys_period := TG_ARGV[0];
  history_table := TG_ARGV[1];
  ignore_unchanged_values := TG_ARGV[3];

  IF ignore_unchanged_values AND TG_OP = 'UPDATE' AND NEW IS NOT DISTINCT FROM OLD THEN
    RETURN OLD;
  END IF;

  IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
    -- Ignore rows already modified in this transaction
    transaction_info := txid_current_snapshot();
    IF OLD.xmin::text >= (txid_snapshot_xmin(transaction_info) % (2^32)::bigint)::text
    AND OLD.xmin::text <= (txid_snapshot_xmax(transaction_info) % (2^32)::bigint)::text THEN
      IF TG_OP = 'DELETE' THEN
        RETURN OLD;
      END IF;

      RETURN NEW;
    END IF;

    EXECUTE format('SELECT $1.%I', sys_period) USING OLD INTO existing_range;

    IF TG_ARGV[2] = 'true' THEN
      -- mitigate update conflicts
      range_lower := lower(existing_range);
      IF range_lower >= time_stamp_to_use THEN
        time_stamp_to_use := range_lower + interval '1 microseconds';
      END IF;
    END IF;

    WITH history AS
      (SELECT attname
      FROM   pg_attribute
      WHERE  attrelid = history_table::regclass
      AND    attnum > 0
      AND    NOT attisdropped),
      main AS
      (SELECT attname
      FROM   pg_attribute
      WHERE  attrelid = TG_RELID
      AND    attnum > 0
      AND    NOT attisdropped)
    SELECT array_agg(quote_ident(history.attname)) INTO common_columns
      FROM history
      INNER JOIN main
      ON history.attname = main.attname
      AND history.attname != sys_period;

     --- We want to compare the actual tracked columns one by one since
    --- NEW IS NOT DISTINCT FROM OLD is not exhaustive.
    IF TG_OP = 'UPDATE' THEN
      json_new := '{}'::jsonb;
      json_old := '{}'::jsonb;

      copy_new := to_jsonb(NEW);
      copy_old := to_jsonb(OLD);

      FOREACH name_col in array common_columns LOOP
        json_new := json_new || jsonb_build_object(name_col, jsonb_extract_path(copy_new, name_col));
        json_old := json_old || jsonb_build_object(name_col, jsonb_extract_path(copy_old, name_col));
      END LOOP;

      IF json_new @> json_old AND json_old @> json_new THEN
        RETURN OLD;
      END IF;
    END IF;

    EXECUTE ('INSERT INTO ' ||
      history_table ||
      '(' ||
      array_to_string(common_columns , ',') ||
      ',' ||
      quote_ident(sys_period) ||
      ') VALUES ($1.' ||
      array_to_string(common_columns, ',$1.') ||
      ',tstzrange($2, $3, ''[)''))')
       USING OLD, range_lower, time_stamp_to_use;
  END IF;

  IF TG_OP = 'UPDATE' OR TG_OP = 'INSERT' THEN
    manipulate := jsonb_set('{}'::jsonb, ('{' || sys_period || '}')::text[], to_jsonb(tstzrange(time_stamp_to_use, null, '[)')));

    RETURN jsonb_populate_record(NEW, manipulate);
  END IF;

  RETURN OLD;
END;
$$ LANGUAGE plpgsql;