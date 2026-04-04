SELECT node_id, name, description, is_active
FROM heer_nodes
WHERE node_id = $1
