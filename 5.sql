WITH person_stamp_values AS (
    SELECT "stamp"
    FROM "person_stamp"
    WHERE "person" = 2976
),
cur_user_groups AS (
    SELECT DISTINCT gg."parent_group_id"
    FROM "group_member" gm
    INNER JOIN "group_group" gg ON gg."child_group_id" = gm."usergroup"
    WHERE gm."person_id" = 2976
),
responsible_executors AS (
    SELECT 
        executor_beard."id" AS beard, 
        executor_beard."id_type" AS beard_type, 
        executor_beard."orig_shortname" AS shortname, 
        SUBSTRING(executor_beard."cmjunid", 0, 33) AS unid, 
        CASE WHEN executor_deadline."execdatecurr" IS NULL OR executor_deadline."execdatecurr"::date = '0001-1-1'::date THEN NULL ELSE executor_deadline."execdatecurr"::date END AS deadline,
        responsible_executor."access_object_id"
    FROM "f_dp_resltnbase_execresp" responsible_executor
    JOIN "f_dp_resltnbase_execcurr" executor ON responsible_executor."executorresp" = executor."executorcurr" AND responsible_executor."executorresp_type" = executor."executorcurr_type"
    JOIN "f_dp_resolution_exdatecur" executor_deadline ON executor."idx" = executor_deadline."idx"
    JOIN "so_beard" executor_beard ON responsible_executor."executorresp" = executor_beard."id" AND responsible_executor."executorresp_type" = executor_beard."id_type"
    WHERE EXISTS (
        SELECT 1
        FROM "f_dp_rkkbase" ptf
        WHERE ptf."id" = responsible_executor."access_object_id"
          AND (ptf."security_stamp" IS NULL OR ptf."security_stamp" IN (SELECT "stamp" FROM person_stamp_values))
    )
    AND EXISTS (
        SELECT 1
        FROM "f_dp_rkkbase_read" r
        WHERE r."group_id" IN (SELECT "parent_group_id" FROM cur_user_groups)
        AND r."object_id" = responsible_executor."access_object_id"
    )
)
SELECT 
    resolution."id",
    resolution."id_type",
    author."id" AS authorid,
    author."id_type" AS authorid_type,
    author."orig_shortname" AS authorname,
    responsible_executor."beard" AS executorid,
    responsible_executor."beard_type" AS executorid_type,
    responsible_executor."unid" AS executorunid,
    responsible_executor."shortname" AS executorname,
    COALESCE(responsible_executor."deadline", resolution."ctrldeadline"::date) AS deadline,
    CASE WHEN responsible_executor."deadline" IS NULL THEN 0 ELSE 1 END AS personaldeadline,
    resolution."ctrldateexecution"::date AS executiondate,
    rkk_base."id" AS rkkid,
    rkk_base."id_type" AS rkkid_type
FROM "f_dp_resolution" resolution
JOIN "f_dp_resltnbase" resolution_base ON resolution."id" = resolution_base."id" AND resolution."id_type" = resolution_base."id_type"
JOIN "f_dp_resltnbase_cntrller" resolution_controller ON resolution_base."id" = resolution_controller."owner" AND resolution_base."id_type" = resolution_controller."owner_type"
JOIN "ss_module" module ON resolution_base."module" = module."id" AND resolution_base."module_type" = module."id_type"
JOIN "ss_moduletype" module_type ON module."type" = module_type."id" AND module_type."type_type" = module_type."id_type"
JOIN "so_beard" author ON resolution_base."author" = author."id" AND resolution_base."author_type" = author."id_type"
JOIN "f_dp_rkkbase" rkk_base ON resolution."hierroot" = rkk_base."id" AND resolution."hierroot_type" = rkk_base."id_type"
JOIN responsible_executors responsible_executor ON responsible_executor.access_object_id = resolution_base.id -- или другое подходящее условие соединения
WHERE resolution."ctrldateexecution"::date <= COALESCE(resolution."ctrldeadline"::date, responsible_executor."deadline", resolution."ctrldateexecution"::date - 1);
