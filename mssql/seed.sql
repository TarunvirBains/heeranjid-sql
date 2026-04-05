-- Default seed data for single-node deployments.
-- Safe to run multiple times (uses IF NOT EXISTS).

IF NOT EXISTS (SELECT 1 FROM heer_config WHERE id = 1)
    INSERT INTO heer_config (id, epoch)
    VALUES (1, '2026-01-01T00:00:00');
ELSE
    UPDATE heer_config SET epoch = '2026-01-01T00:00:00' WHERE id = 1;
GO

IF NOT EXISTS (SELECT 1 FROM heer_nodes WHERE node_id = 1)
    INSERT INTO heer_nodes (node_id, name, description, is_active)
    VALUES (1, N'default', N'Default single-node instance', 1);
GO

IF NOT EXISTS (SELECT 1 FROM heer_node_state WHERE node_id = 1)
    INSERT INTO heer_node_state (node_id)
    VALUES (1);
GO

IF NOT EXISTS (SELECT 1 FROM heer_ranj_node_state WHERE node_id = 1)
    INSERT INTO heer_ranj_node_state (node_id)
    VALUES (1);
GO
