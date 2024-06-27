DROP INDEX idx_resolution_exdatecur_join;
DROP INDEX idx_f_dp_tasksresolution_ec;
ANALYZE f_dp_resolution_exdatecur;
ANALYZE f_dp_tasksresolution_ec;

CREATE INDEX idx_person_stamp_person ON person_stamp(person);
CREATE INDEX idx_group_member_person_id ON group_member(person_id);
CREATE INDEX idx_group_group_child_group_id ON group_group(child_group_id);

CREATE INDEX idx_f_dp_resolution_id ON f_dp_resolution(id);
CREATE INDEX idx_f_dp_resltnbase_id ON f_dp_resltnbase(id);
CREATE INDEX idx_f_dp_resltnbase_execresp ON f_dp_resltnbase_execresp(executorresp, executorresp_type, owner, owner_type);
CREATE INDEX idx_f_dp_resolution_exdatecur ON f_dp_resolution_exdatecur(idx, owner, owner_type);

CREATE INDEX idx_f_dp_tasksresolution_id ON f_dp_tasksresolution(id);
CREATE INDEX idx_f_dp_tasksresolution_er ON f_dp_tasksresolution_er(executorresp, executorresp_type, owner, owner_type);
CREATE INDEX idx_f_dp_tasksresolution_edc ON f_dp_tasksresolution_edc(idx, owner, owner_type);


ANALYZE person_stamp;
ANALYZE group_member;
ANALYZE group_group;
ANALYZE f_dp_resolution;
ANALYZE f_dp_resltnbase;
ANALYZE f_dp_resltnbase_execresp;
ANALYZE f_dp_resolution_exdatecur;
ANALYZE f_dp_tasksresolution;
ANALYZE f_dp_tasksresolution_er;
ANALYZE f_dp_tasksresolution_edc;


WITH person_stamp_values AS (
  SELECT "stamp" 
  FROM person_stamp 
  WHERE person = 2976
), 
cur_user_groups AS (
  SELECT DISTINCT gg.parent_group_id 
  FROM group_member gm 
  INNER JOIN group_group gg ON gg.child_group_id = gm.usergroup 
  WHERE gm.person_id = 2976
) 
SELECT * 
FROM (
  SELECT resolution.id, 
    resolution.id_type, 
    author.id AS authorid, 
    author.id_type AS authorid_type, 
    author.orig_shortname AS authorname, 
    responsible_executor.beard AS executorid, 
    responsible_executor.beard_type AS executorid_type, 
    responsible_executor.beard_type AS executoridtype, 
    responsible_executor.unid AS executorunid, 
    responsible_executor.shortname AS executorname, 
    COALESCE(responsible_executor.deadline, resolution.ctrldeadline::date) AS deadline, 
    CASE WHEN responsible_executor.deadline IS NULL THEN 0 ELSE 1 END AS personaldeadline, 
    resolution.ctrldateexecution::date AS executiondate, 
    rkk_base.id AS rkkid, 
    rkk_base.id_type AS rkkid_type 
  FROM (
    SELECT f_dp_resolution.* 
    FROM f_dp_resolution 
    WHERE EXISTS (
      SELECT 1 
      FROM f_dp_rkkbase ptf 
      INNER JOIN f_dp_resltnbase rt ON ptf.id = rt.access_object_id 
      WHERE rt.id = f_dp_resolution.id 
      AND (ptf.security_stamp IS NULL OR ptf.security_stamp IN (SELECT stamp FROM person_stamp_values))
    ) 
    AND EXISTS (
      SELECT 1 
      FROM f_dp_rkkbase_read r 
      INNER JOIN f_dp_resltnbase rt ON r.object_id = rt.access_object_id 
      WHERE r.group_id IN (SELECT parent_group_id FROM cur_user_groups) 
      AND rt.id = f_dp_resolution.id 
      LIMIT 1
    )
  ) resolution 
  JOIN f_dp_resltnbase resolution_base ON resolution.id = resolution_base.id 
  AND resolution.id_type = resolution_base.id_type 
  AND resolution_base.ctrliscontrolled = 1 
  AND resolution.isproject <> 1 
  AND resolution.executioncanceldate IS NULL 
  AND resolution.ctrldateexecution IS NOT NULL 
  AND resolution.ctrldateexecution::date BETWEEN '2024-04-01' AND '2024-06-30' 
  JOIN f_dp_resltnbase_cntrller resolution_controller ON resolution_base.id = resolution_controller.owner 
  AND resolution_base.id_type = resolution_controller.owner_type 
  AND resolution_controller.controller IN (101272) 
  AND resolution_controller.controller_type = 1142 
  JOIN ss_module module ON resolution_base.module = module.id 
  AND resolution_base.module_type = module.id_type 
  JOIN ss_moduletype module_type ON module.type = module_type.id 
  AND module.type_type = module_type.id_type 
  AND module_type.alias NOT IN ('TempStorage', 'checkencoding') 
  JOIN so_beard author ON resolution_base.author = author.id 
  AND resolution_base.author_type = author.id_type 
  JOIN f_dp_rkkbase rkk_base ON resolution.hierroot = rkk_base.id 
  AND resolution.hierroot_type = rkk_base.id_type 
  JOIN LATERAL (
    SELECT executor_beard.id AS beard, 
      executor_beard.id_type AS beard_type, 
      executor_beard.orig_shortname AS shortname, 
      SUBSTRING(executor_beard.cmjunid, 0, 33) AS unid, 
      CASE WHEN executor_deadline.execdatecurr IS NULL OR executor_deadline.execdatecurr::date = '0001-1-1'::date THEN NULL ELSE executor_deadline.execdatecurr::date END AS deadline 
    FROM f_dp_resltnbase_execresp responsible_executor 
    JOIN f_dp_resltnbase_execcurr executor ON responsible_executor.executorresp = executor.executorcurr 
    AND responsible_executor.executorresp_type = executor.executorcurr_type 
    AND responsible_executor.owner = executor.owner 
    AND responsible_executor.owner_type = executor.owner_type 
    AND executor.owner = resolution_base.id 
    AND executor.owner_type = resolution_base.id_type 
    JOIN f_dp_resolution_exdatecur executor_deadline ON executor.idx = executor_deadline.idx 
    AND responsible_executor.owner = executor_deadline.owner 
    AND responsible_executor.owner_type = executor_deadline.owner_type 
    JOIN so_beard executor_beard ON responsible_executor.executorresp = executor_beard.id 
    AND responsible_executor.executorresp_type = executor_beard.id_type 
    WHERE EXISTS (
      SELECT 1 
      FROM f_dp_rkkbase ptf 
      WHERE ptf.id = f_dp_resltnbase_execresp.access_object_id 
      AND (ptf.security_stamp IS NULL OR ptf.security_stamp IN (SELECT stamp FROM person_stamp_values))
    ) 
    AND EXISTS (
      SELECT 1 
      FROM f_dp_rkkbase_read r 
      WHERE r.group_id IN (SELECT parent_group_id FROM cur_user_groups) 
      AND r.object_id = f_dp_resltnbase_execresp.access_object_id 
      LIMIT 1
    )
  ) responsible_executor ON true 
  WHERE resolution.ctrldateexecution::date <= COALESCE(resolution.ctrldeadline::date, responsible_executor.deadline, resolution.ctrldateexecution::date - 1)
  
  UNION ALL 
  
  SELECT resolution.id, 
    resolution.id_type, 
    author.id AS authorid, 
    author.id_type AS authorid_type, 
    author.orig_shortname AS authorname, 
    responsible_executor.beard AS executorid, 
    responsible_executor.beard_type AS executorid_type, 
    responsible_executor.beard_type AS executoridtype, 
    responsible_executor.unid AS executorunid, 
    responsible_executor.shortname AS executorname, 
    COALESCE(responsible_executor.deadline, resolution.ctrldeadline::date) AS deadline, 
    CASE WHEN responsible_executor.deadline IS NULL THEN 0 ELSE 1 END AS personaldeadline, 
    resolution.ctrldateexecution::date AS executiondate, 
    resolution.id AS rkkid, 
    resolution.id_type AS rkkid_type 
  FROM (
    SELECT f_dp_tasksresolution.* 
    FROM f_dp_tasksresolution 
    WHERE EXISTS (
      SELECT 1 
      FROM f_dp_rkkbase root_type 
      WHERE root_type.id = f_dp_tasksresolution.id 
      AND (root_type.security_stamp IS NULL OR root_type.security_stamp IN (SELECT stamp FROM person_stamp_values))
    ) 
    AND EXISTS (
      SELECT 1 
      FROM f_dp_rkkbase_read r 
      INNER JOIN f_dp_rkkbase rt ON r.object_id = rt.access_object_id 
      WHERE r.group_id IN (SELECT parent_group_id FROM cur_user_groups) 
      AND rt.id = f_dp_tasksresolution.id 
      AND (r.module IS NULL OR rt.module = r.module) 
      LIMIT 1
    )
  ) resolution 
  JOIN f_dp_rkkbase rkk_base ON resolution.id = rkk_base.id 
  AND resolution.id_type = rkk_base.id_type 
  AND resolution.ctrliscontrolled = 1 
  AND resolution.isproject <> 1 
  AND resolution.executioncanceldate IS NULL 
  AND resolution.ctrldateexecution IS NOT NULL 
  AND resolution.ctrldateexecution::date BETWEEN '2024-04-01' AND '2024-06-30' 
  JOIN f_dp_rkkbase_controller resolution_controller ON resolution.id = resolution_controller.owner 
  AND resolution.id_type = resolution_controller.owner_type 
  AND resolution_controller.controller IN (101272) 
  AND resolution_controller.controller_type = 1142 
  JOIN ss_module module ON rkk_base.module = module.id 
  AND rkk_base.module_type = module.id_type 
  JOIN ss_moduletype module_type ON module.type = module_type.id 
  AND module.type_type = module_type.id_type 
  AND module_type.alias NOT IN ('TempStorage', 'checkencoding') 
  JOIN so_beard author ON resolution.author = author.id 
  AND resolution.author_type = author.id_type 
  JOIN LATERAL (
    SELECT executor_beard.id AS beard, 
      executor_beard.id_type AS beard_type, 
      executor_beard.orig_shortname AS shortname, 
      SUBSTRING(executor_beard.cmjunid, 0, 33) AS unid, 
      CASE WHEN executor_deadline.execdatecurr IS NULL OR executor_deadline.execdatecurr::date = '0001-1-1'::date THEN NULL ELSE executor_deadline.execdatecurr::date END AS deadline 
    FROM f_dp_tasksresolution_er responsible_executor 
    JOIN f_dp_tasksresolution_ec executor ON responsible_executor.executorresp = executor.executorcurr 
    AND responsible_executor.executorresp_type = executor.executorcurr_type 
    AND responsible_executor.owner = executor.owner 
    AND responsible_executor.owner_type = executor.owner_type 
    AND executor.owner = resolution.id 
    AND executor.owner_type = resolution.id_type 
    JOIN f_dp_tasksresolution_edc executor_deadline ON executor.idx = executor_deadline.idx 
    AND responsible_executor.owner = executor_deadline.owner 
    AND responsible_executor.owner_type = executor_deadline.owner_type 
    JOIN so_beard executor_beard ON responsible_executor.executorresp = executor_beard.id 
    AND responsible_executor.executorresp_type = executor_beard.id_type 
    WHERE EXISTS (
      SELECT 1 
      FROM f_dp_rkkbase ptf 
      WHERE ptf.id = f_dp_tasksresolution_er.access_object_id 
      AND (ptf.security_stamp IS NULL OR ptf.security_stamp IN (SELECT stamp FROM person_stamp_values))
    ) 
    AND EXISTS (
      SELECT 1 
      FROM f_dp_rkkbase_read r 
      WHERE r.group_id IN (SELECT parent_group_id FROM cur_user_groups) 
      AND r.object_id = f_dp_tasksresolution_er.access_object_id 
      LIMIT 1
    )
  ) responsible_executor ON true 
  WHERE resolution.ctrldateexecution::date <= COALESCE(resolution.ctrldeadline::date, responsible_executor.deadline, resolution.ctrldateexecution::date - 1)
) AS resolution 
ORDER BY resolution.authorid, resolution.executoridtype, resolution.executorid, resolution.executorid_type, resolution.deadline;
