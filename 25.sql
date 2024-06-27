-- Удаление индексов
DROP INDEX IF EXISTS idx_f_dp_resolution_id_id_type;
DROP INDEX IF EXISTS idx_f_dp_resltnbase_id_id_type;
DROP INDEX IF EXISTS idx_f_dp_resltnbase_cntrller_owner_owner_type;
DROP INDEX IF EXISTS idx_ss_module_id_id_type;
DROP INDEX IF EXISTS idx_ss_moduletype_id_id_type;
DROP INDEX IF EXISTS idx_so_beard_id_id_type;
DROP INDEX IF EXISTS idx_f_dp_rkkbase_id_id_type;
DROP INDEX IF EXISTS idx_f_dp_resltnbase_cntrller_person_id;
DROP INDEX IF EXISTS idx_person_stamp_person;
DROP INDEX IF EXISTS idx_f_dp_rkkbase_read_group_id_object_id;
DROP INDEX IF EXISTS idx_f_dp_rkkbase_read_object_group_hash;
-- Обновление статистики
ANALYZE f_dp_resolution;
ANALYZE f_dp_resltnbase;
ANALYZE f_dp_resltnbase_cntrller;
ANALYZE ss_module;
ANALYZE ss_moduletype;
ANALYZE so_beard;
ANALYZE f_dp_rkkbase;
ANALYZE person_stamp;
ANALYZE f_dp_rkkbase_read;

-- Создание составных индексов для Nested Loop Joins
CREATE INDEX idx_executor_beard ON so_beard(id, id_type);
CREATE INDEX idx_f_dp_resolution_base ON f_dp_resolution(owner, owner_type, idx);

-- Создание индексов для частых ключей сортировки
CREATE INDEX idx_sort_author ON so_beard(id);
CREATE INDEX idx_sort_executor_beard ON so_beard(id_type, id);

-- Создание индексов для условий WHERE
CREATE INDEX idx_person_stamp_values ON person_stamp(person);
CREATE INDEX idx_group_member ON group_member(person_id);
CREATE INDEX idx_group_group ON group_group(child_group_id);
CREATE INDEX idx_f_dp_resltnbase_execcurr ON f_dp_resltnbase_execcurr(executorcurr, executorcurr_type);
CREATE INDEX idx_f_dp_resltnbase_execresp ON f_dp_resltnbase_execresp(executorresp, executorresp_type);

-- Создание индекса для агрегатных функций
CREATE INDEX idx_cur_user_groups ON group_group(parent_group_id);
CREATE INDEX "~f_dp_rkkbase_read-df179f20"
  ON f_dp_rkkbase_read(
    object_id
  , group_id
  );
-- Обновление статистики
ANALYZE so_beard;
ANALYZE f_dp_resolution;
ANALYZE person_stamp;
ANALYZE group_member;
ANALYZE group_group;
ANALYZE f_dp_resltnbase_execcurr;
ANALYZE f_dp_resltnbase_execresp;
ANALYZE f_dp_rkkbase_read;

WITH RECURSIVE person_stamp_values AS (
  SELECT "stamp" 
  FROM "person_stamp" 
  WHERE "person" = 2976
),
cur_user_groups AS (
  SELECT DISTINCT gg."parent_group_id"
  FROM "group_member" gm
  JOIN "group_group" gg ON gg."child_group_id" = gm."usergroup"
  WHERE gm."person_id" = 2976
),
resolution_data AS (
  SELECT 
    r.id, r.id_type, r.hierroot, r.hierroot_type, r.ctrldateexecution, r.ctrldeadline,
    a.id AS authorid, a.id_type AS authorid_type, a.orig_shortname AS authorname,
    rkk.id AS rkkid, rkk.id_type AS rkkid_type,
    re.executorresp AS executorid, re.executorresp_type AS executorid_type,
    eb.orig_shortname AS executorname, SUBSTRING(eb.cmjunid, 0, 33) AS executorunid,
    COALESCE(red.execdatecurr::date, r.ctrldeadline::date) AS deadline,
    CASE WHEN red.execdatecurr IS NOT NULL THEN 1 ELSE 0 END AS personaldeadline
  FROM (
    SELECT * FROM f_dp_resolution
    UNION ALL
    SELECT * FROM f_dp_tasksresolution
  ) r
  JOIN f_dp_rkkbase rkk ON r.hierroot = rkk.id AND r.hierroot_type = rkk.id_type
  JOIN so_beard a ON r.author = a.id AND r.author_type = a.id_type
  JOIN f_dp_resltnbase_execresp re ON r.id = re.owner AND r.id_type = re.owner_type
  JOIN so_beard eb ON re.executorresp = eb.id AND re.executorresp_type = eb.id_type
  LEFT JOIN f_dp_resolution_exdatecur red ON r.id = red.owner AND r.id_type = red.owner_type AND re.idx = red.idx
  WHERE r.ctrliscontrolled = 1
    AND r.isproject <> 1
    AND r.executioncanceldate IS NULL
    AND r.ctrldateexecution IS NOT NULL
    AND r.ctrldateexecution::date BETWEEN '2024-04-01' AND '2024-06-30'
),
filtered_resolution AS (
  SELECT rd.*
  FROM resolution_data rd
  JOIN f_dp_resltnbase_cntrller rc ON rd.id = rc.owner AND rd.id_type = rc.owner_type
  WHERE rc.controller = 101272 AND rc.controller_type = 1142
    AND rd.ctrldateexecution::date <= COALESCE(rd.ctrldeadline::date, rd.deadline, rd.ctrldateexecution::date - 1)
    AND EXISTS (
      SELECT 1 
      FROM f_dp_rkkbase ptf
      WHERE ptf.id = rd.rkkid
        AND (ptf.security_stamp IS NULL OR ptf.security_stamp IN (SELECT "stamp" FROM person_stamp_values))
    )
    AND EXISTS (
      SELECT 1 
      FROM f_dp_rkkbase_read r
      WHERE r.object_id = rd.rkkid
        AND r.group_id IN (SELECT parent_group_id FROM cur_user_groups)
    )
)
SELECT *
FROM filtered_resolution fr
JOIN ss_module m ON fr.hierroot = m.id AND fr.hierroot_type = m.id_type
JOIN ss_moduletype mt ON m.type = mt.id AND m.type_type = mt.id_type
WHERE mt.alias NOT IN ('TempStorage', 'checkencoding')
ORDER BY fr.authorid, fr.executorid_type, fr.executorid, fr.executorid_type, fr.deadline;
