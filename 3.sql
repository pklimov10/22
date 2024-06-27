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
JOIN "ss_moduletype" module_type ON module."type" = module_type."id" AND module."type_type" = module_type."id_type"
JOIN "so_beard" author ON resolution_base."author" = author."id" AND resolution_base."author_type" = author."id_type"
JOIN "f_dp_rkkbase" rkk_base ON resolution."hierroot" = rkk_base."id" AND resolution."hierroot_type" = rkk_base."id_type"
JOIN responsible_executors responsible_executor ON true
WHERE resolution."ctrldateexecution"::date <= COALESCE(resolution."ctrldeadline"::date, responsible_executor."deadline", resolution."ctrldateexecution"::date - 1);
