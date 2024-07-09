
CREATE table IF NOT EXISTS  workspaces (
      id serial primary key,
      parent integer,
      tenant_id integer,
      name varchar(255) not null,
      foreign key (parent) references workspaces(id)
);

CREATE TEMP TABLE temp_table (nt INTEGER);

INSERT INTO temp_table (nt) VALUES (:num_tenants);

DO $$
DECLARE
    max_levels INTEGER := 5; -- Number of levels in the tree
    nodes_per_level INTEGER[] := ARRAY[1, 2, 3, 4, 5]; -- Number of nodes per level
    tenant_id INTEGER; -- Variable to hold tenant_id during iteration
    num_tenants INTEGER = 1000;
    level INTEGER;
    parent_id INTEGER;
    node_count INTEGER;
    i INTEGER;
BEGIN
    TRUNCATE TABLE workspaces RESTART IDENTITY;
    SELECT nt INTO num_tenants FROM temp_table;
    FOR tenant_id IN 1..num_tenants LOOP
        INSERT INTO workspaces (parent, tenant_id, name) VALUES (NULL, tenant_id, 'Root Tenant ' || tenant_id) RETURNING id INTO parent_id;

        FOR level IN 1..max_levels LOOP
            FOR node_count IN 1..nodes_per_level[level] LOOP
                INSERT INTO workspaces (parent, tenant_id, name) VALUES (parent_id, tenant_id, 'Level ' || level || ' Node ' || node_count || ' Tenant ' || tenant_id) RETURNING id INTO i;
                IF level < max_levels THEN
                    parent_id := i;
                END IF;
            END LOOP;
        END LOOP;
    END LOOP;
END $$;

DROP TABLE temp_table;

