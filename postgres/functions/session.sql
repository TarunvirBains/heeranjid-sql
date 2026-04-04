CREATE OR REPLACE FUNCTION set_heer_node_id(node_id INTEGER)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    validated_node_id INTEGER;
BEGIN
    IF node_id IS NULL THEN
        RAISE EXCEPTION 'node_id cannot be null';
    END IF;

    IF node_id < 0 OR node_id > 511 THEN
        RAISE EXCEPTION 'node_id % is out of range for HeerId', node_id;
    END IF;

    SELECT n.node_id
    INTO validated_node_id
    FROM heer_nodes AS n
    WHERE n.node_id = set_heer_node_id.node_id
      AND n.is_active = true;

    IF validated_node_id IS NULL THEN
        RAISE EXCEPTION 'node_id % is not registered as an active Heer node', node_id;
    END IF;

    PERFORM set_config('heer.node_id', node_id::text, false);
END;
$$;

CREATE OR REPLACE FUNCTION current_heer_node_id()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    configured_node_id TEXT;
    parsed_node_id INTEGER;
BEGIN
    configured_node_id := current_setting('heer.node_id', true);

    IF configured_node_id IS NULL OR configured_node_id = '' THEN
        RAISE EXCEPTION 'heer.node_id is not set for this session';
    END IF;

    parsed_node_id := configured_node_id::INTEGER;
    PERFORM set_heer_node_id(parsed_node_id);
    RETURN parsed_node_id;
END;
$$;
