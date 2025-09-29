-- Databricks notebook source
SHOW TABLES IN main.threat_graph_analytics;


-- COMMAND ----------

USE main.threat_graph_analytics;


-- COMMAND ----------

SELECT * FROM main.threat_graph_analytics.full_synthetic_stix_graph

-- COMMAND ----------


CREATE OR REPLACE TABLE main.threat_graph_analytics.stix_graph_edges
CLUSTER BY (src, dst) AS
SELECT
  raw:source_ref::string AS src,
  raw:target_ref::string AS dst,
  raw:relationship_type::string AS relationship_type,
  raw:created::timestamp AS edge_timestamp,
  raw
FROM main.threat_graph_analytics.full_synthetic_stix_graph
WHERE raw:type::string = 'relationship'
;

OPTIMIZE main.threat_graph_analytics.stix_graph_edges;

CREATE OR REPLACE TABLE main.threat_graph_analytics.stix_graph_nodes
CLUSTER BY (id) AS
SELECT
  raw:id::string AS id,
  raw:type::string AS type,
  raw
FROM main.threat_graph_analytics.full_synthetic_stix_graph
WHERE raw:type::string != 'relationship';

OPTIMIZE main.threat_graph_analytics.stix_graph_nodes;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION main.threat_graph_analytics.get_node_types()
RETURNS ARRAY<STRING>
COMMENT 'This gets all the available node types in the graph'
RETURN
  (SELECT collect_set(type)
  FROM main.threat_graph_analytics.stix_graph_nodes
  LIMIT 1)
;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION main.threat_graph_analytics.get_relationship_types()
RETURNS ARRAY<STRING>
COMMENT 'This gets all the available edge connection types in the graph'
RETURN
  SELECT collect_set(relationship_type)
  FROM main.threat_graph_analytics.stix_graph_edges
  LIMIT 1;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION main.threat_graph_analytics.get_node_metadata(node_id STRING)
RETURNS STRUCT<id: STRING, type: STRING, raw: STRING>
COMMENT 'Get all metadata / information about a particular node id'
RETURN
  (SELECT
    STRUCT(id, type, CAST(raw AS STRING) AS raw)
  FROM main.threat_graph_analytics.stix_graph_nodes
  WHERE id = node_id
  LIMIT 1);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION main.threat_graph_analytics.get_top_connected_nodes(limit_num INT DEFAULT 2, connected_node_types ARRAY<STRING> DEFAULT NULL)
RETURNS ARRAY<STRUCT<node_id: STRING, type: STRING, degree: INT>>
COMMENT 'Get the top connected nodes to a give node id to begin deeper analysis on impactful nodes. You can optionally supply a filter array<string> list of node types you want connections for.'
RETURN (
  SELECT collect_list(
    STRUCT(
      node_id,
      type,
      degree
    )
  )
  FROM (
    SELECT
      n.id AS node_id,
      n.type,
      (COALESCE(e_in.degree_in, 0) + COALESCE(e_out.degree_out, 0)) AS degree,
      ROW_NUMBER() OVER (ORDER BY (COALESCE(e_in.degree_in, 0) + COALESCE(e_out.degree_out, 0)) DESC) AS rn
    FROM main.threat_graph_analytics.stix_graph_nodes n
    LEFT JOIN (
      SELECT dst AS id, COUNT(*) AS degree_in
      FROM main.threat_graph_analytics.stix_graph_edges
      GROUP BY dst
    ) e_in ON n.id = e_in.id
    LEFT JOIN (
      SELECT src AS id, COUNT(*) AS degree_out
      FROM main.threat_graph_analytics.stix_graph_edges
      GROUP BY src
    ) e_out ON n.id = e_out.id
  )
  WHERE rn <= limit_num
  AND connected_node_types IS NULL OR array_contains(connected_node_types, type)
);

-- COMMAND ----------

-- DBTITLE 1,Breadth First Search
-- Use Stored procedures for more complex compound statements with multiple steps and params
CREATE OR REPLACE PROCEDURE main.threat_graph_analytics.bfs_search(
  input_node_id STRING, 
  max_level INT, 
  connected_node_types ARRAY<STRING> DEFAULT NULL
)
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Breadth first search from a given input node id with max_level being the number of degrees out from the node you want to search. You can also supply an array<list> of node types you want to track connections for'
AS
BEGIN
    WITH RECURSIVE neighborhood(level, node_id, prev_node_id, relationship_type, path, visited) AS (
      -- Base case: start from the given node
      SELECT
        0 AS level,
        id AS node_id,
        CAST(NULL AS STRING) AS prev_node_id,
        CAST(NULL AS STRING) AS relationship_type,
        ARRAY(id) AS path,
        ARRAY(id) AS visited
      FROM main.threat_graph_analytics.stix_graph_nodes AS n
      WHERE n.id = input_node_id

      UNION ALL

      -- Breadth-first search: traverse level by level
      SELECT
        n.level + 1,
        CASE WHEN e.src = n.node_id THEN e.dst ELSE e.src END AS node_id,
        n.node_id AS prev_node_id,
        e.relationship_type,
        array_union(n.path, ARRAY(CASE WHEN e.src = n.node_id THEN e.dst ELSE e.src END)) AS path,
        array_union(n.visited, ARRAY(CASE WHEN e.src = n.node_id THEN e.dst ELSE e.src END)) AS visited
      FROM neighborhood n
      JOIN main.threat_graph_analytics.stix_graph_edges e
        ON n.node_id = e.src OR n.node_id = e.dst
      WHERE n.level < max_level
        AND NOT array_contains(n.visited, CASE WHEN e.src = n.node_id THEN e.dst ELSE e.src END)
    )

    SELECT
      STRUCT(
        n.level AS level,
        n.node_id AS node_id,
        nodes.type AS type,
        n.prev_node_id AS prev_node_id,
        n.relationship_type AS relationship_type,
        array_join(n.path, ', ') AS path
      )
    FROM neighborhood n
    JOIN main.threat_graph_analytics.stix_graph_nodes nodes
      ON n.node_id = nodes.id
    WHERE n.level > 0
      AND (connected_node_types IS NULL OR array_contains(connected_node_types, nodes.type))
;
END;

-- COMMAND ----------

-- DBTITLE 1,Page Rank
CREATE OR REPLACE PROCEDURE main.threat_graph_analytics.pagerank_top_n(
  top_n INT,
  filter_node_types ARRAY<STRING> DEFAULT NULL
)
LANGUAGE SQL
SQL SECURITY INVOKER
COMMENT 'Run page rank on the STIX graph to get a stack rank of the most influential nodes, optionally filtered by node types'
AS
BEGIN
  WITH RECURSIVE
  params AS (
    SELECT CAST(0.85 AS DOUBLE) AS d, CAST(30 AS INT) AS max_iter
  ),
  nodes AS (
    SELECT id, type
    FROM main.threat_graph_analytics.stix_graph_nodes
    WHERE filter_node_types IS NULL OR array_contains(filter_node_types, type)
  ),
  total_nodes AS (
    SELECT COUNT(*) AS n FROM nodes
  ),
  edges AS (
    SELECT src, dst FROM main.threat_graph_analytics.stix_graph_edges
  ),
  outdeg AS (
    SELECT src, COUNT(*) AS outdeg
    FROM edges
    GROUP BY src
  ),

  /* Paths recursion: carry per-path weight; NO aggregation inside recursion */
  paths (node_id, weight, iteration) AS (
    -- iteration 0: start with 1/N at each node
    SELECT
      n.id         AS node_id,
      CAST(1.0 / t.n AS DOUBLE) AS weight,
      0            AS iteration
    FROM nodes n
    CROSS JOIN total_nodes t

    UNION ALL

    -- push weight along outgoing edges, scaling by d/outdeg
    SELECT
      e.dst        AS node_id,
      CAST(p.weight * par.d / NULLIF(od.outdeg, 0) AS DOUBLE) AS weight,
      p.iteration + 1 AS iteration
    FROM paths p
    JOIN params par         ON TRUE
    JOIN edges e            ON e.src = p.node_id
    LEFT JOIN outdeg od     ON od.src = e.src
    WHERE p.iteration < par.max_iter
  ),

  last_iter AS (
    SELECT MAX(iteration) AS k FROM paths
  ),

  /* Sum contributions at the final iteration and add teleport mass */
  final_rank AS (
    SELECT
      n.id AS node_id,
      n.type AS node_type,
      -- teleport series: ((1-d)/N) * (1 - d^k) / (1 - d)
      CAST( ((1 - par.d) / t.n) * (1 - POWER(par.d, li.k)) / (1 - par.d) AS DOUBLE )
        + COALESCE(pr_k.inbound_sum, 0.0) AS pagerank
    FROM nodes n
    CROSS JOIN params par
    CROSS JOIN total_nodes t
    CROSS JOIN last_iter li
    LEFT JOIN (
      SELECT node_id, SUM(weight) AS inbound_sum
      FROM paths
      WHERE iteration = (SELECT k FROM last_iter)
      GROUP BY node_id
    ) pr_k
    ON pr_k.node_id = n.id
  )

  SELECT node_id, node_type, pagerank
  FROM final_rank
  ORDER BY pagerank DESC
  LIMIT top_n;
END;

-- COMMAND ----------

SELECT * FROM main.threat_graph_analytics.stix_graph_edges

-- COMMAND ----------

SELECT main.threat_graph_analytics.get_node_types()

-- COMMAND ----------

SELECT main.threat_graph_analytics.get_top_connected_nodes(5)

-- COMMAND ----------

SELECT main.threat_graph_analytics.get_node_metadata('threat-actor--84ba3be4-3c30-4725-98a2-2d7b97f8269b')

-- COMMAND ----------

-- DBTITLE 1,Call BFS Stored Procedure
CALL main.threat_graph_analytics.bfs_search('threat-actor--c2a4aee6-e118-466d-93ef-72d42718455c',2)

-- COMMAND ----------

-- DBTITLE 1,Calling Page Rank Stored Procedure
-- Call your stored procedures to do advanced analytics
CALL main.threat_graph_analytics.pagerank_top_n(10, split('threat-actor,malware', ','))

-- COMMAND ----------

CALL main.threat_graph_analytics.pagerank_top_n(20, split('threat-actor,malware,tool', ','))

-- COMMAND ----------

-- DBTITLE 1,Querying the graph with your functions AND generic Models and Agents
-- You can query your custom function to do analysis / get node / campaign info, and then pass to AI models or even custom agents you host in model serving!
WITH results AS (
  SELECT main.threat_graph_analytics.get_node_metadata('threat-actor--84ba3be4-3c30-4725-98a2-2d7b97f8269b') AS node
)
SELECT ai_query(
  'databricks-meta-llama-3-3-70b-instruct',
  CONCAT('Summarize the following metadata: ', CAST(node AS STRING))
) AS summary
FROM results;
