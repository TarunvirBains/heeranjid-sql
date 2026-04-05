-- Default seed data for single-node deployments.
-- Safe to run multiple times (uses ON CONFLICT).

INSERT INTO heer_config (id, epoch, precision)
VALUES (1, '2026-01-01T00:00:00', 'ns')
ON CONFLICT (id) DO UPDATE SET epoch = EXCLUDED.epoch, precision = EXCLUDED.precision;

INSERT INTO heer_nodes (node_id, name, description, is_active)
VALUES (1, 'default', 'Default single-node instance', true)
ON CONFLICT (node_id) DO NOTHING;

INSERT INTO heer_node_state (node_id)
VALUES (1)
ON CONFLICT (node_id) DO NOTHING;

INSERT INTO heer_ranj_node_state (node_id)
VALUES (1)
ON CONFLICT (node_id) DO NOTHING;
