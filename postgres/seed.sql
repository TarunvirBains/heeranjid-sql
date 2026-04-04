-- Default seed data for single-node deployments.
-- Safe to run multiple times (uses ON CONFLICT).

INSERT INTO heer_nodes (node_id, name, description, is_active)
VALUES (1, 'default', 'Default single-node instance', true)
ON CONFLICT (node_id) DO NOTHING;

INSERT INTO heer_node_state (node_id)
VALUES (1)
ON CONFLICT (node_id) DO NOTHING;

INSERT INTO heer_ranj_node_state (node_id)
VALUES (1)
ON CONFLICT (node_id) DO NOTHING;
