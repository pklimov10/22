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
resolution_base AS (
    SELECT f_dp_resolution.*
    FROM "f_dp_resolution" f_dp_resolution
    WHERE EXISTS (
        SELECT 1
        FROM "f_dp_rkkbase" ptf
        INNER JOIN "f_dp_resltnbase" rt ON ptf."id" = rt."access_object_id"
        WHERE rt."id" = f_dp_resolution."id"
          AND (ptf."security_stamp" IS NULL OR ptf."security_stamp" IN (SELECT "stamp" FROM person_stamp_values))
    )
    AND EXISTS (
        SELECT 1
        FROM "f_dp_rkkbase_read" r
        WHERE r."group_id" IN (SELECT "parent_group_id" FROM cur_user_groups)
          AND r."object_id" = f_dp_resolution."id"
    )
),
executor_data AS (
    SELECT
        executor_beard."id" AS beard,
        executor_beard."id_type" AS beard_type,
        executor_beard."orig_shortname" AS shortname,
        SUBSTRING(executor_beard."cmjunid", 0, 33) AS unid,
        CASE WHEN executor_deadline."execdatecurr" IS NULL OR executor_deadline."execdatecurr"::date = '0001-1-1'::date THEN NULL ELSE executor_deadline."execdatecurr"::date END AS deadline
    FROM "f_dp_resltnbase_execresp" responsible_executor
    JOIN "f_dp_resltnbase_execcurr" executor ON responsible_executor."executorresp" = executor."executorcurr" AND responsible_executor."executorresp_type" = executor."executorcurr_type"
    JOIN "f_dp_resolution_exdatecur" executor_deadline ON executor."idx" = executor_deadline."idx"
    JOIN "so_beard" executor_beard ON responsible_executor."executorresp" = executor_beard."id" AND responsible_executor."executorresp_type" = executor_beard."id_type
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
    responsible_executor."beard_type" AS executoridtype, 
    responsible_executor."unid" AS executorunid, 
    responsible_executor."shortname" AS executorname, 
    COALESCE(responsible_executor."deadline", resolution."ctrldeadline"::date) AS deadline, 
    CASE WHEN responsible_executor."deadline" IS NULL THEN 0 ELSE 1 END AS personaldeadline, 
    resolution."ctrldateexecution"::date AS executiondate, 
    rkk_base."id" AS rkkid, 
    rkk_base."id_type" AS rkkid_type
FROM 
    resolution_base resolution
JOIN "f_dp_resltnbase" resolution_base ON resolution."id" = resolution_base."id" AND resolution."id_type" = resolution_base."id_type"
JOIN "f_dp_resltnbase_cntrller" resolution_controller ON resolution_base."id" = resolution_controller."owner" AND resolution_base."id_type" = resolution_controller."owner_type"
JOIN "ss_module" module ON resolution_base."module" = module."id" AND resolution_base."module_type" = module."id_type"
JOIN "ss_moduletype" module_type ON module."type" = module_type."id" AND module."type_type" = module_type."id_type"
JOIN "so_beard" author ON resolution_base."author" = author."id" AND resolution_base."author_type" = author."id_type"
JOIN "f_dp_rkkbase" rkk_base ON resolution."hierroot" = rkk_base."id" AND resolution."hierroot_type" = rkk_base."id_type"
JOIN executor_data responsible_executor ON true
WHERE resolution."ctrldateexecution"::date <= COALESCE(resolution."ctrldeadline"::date, responsible_executor."deadline", resolution."ctrldateexecution"::date - 1)
UNION ALL
SELECT 
    resolution."id", 
    resolution."id_type", 
    author."id" AS authorid, 
    author."id_type" AS authorid_type, 
    author."orig_shortname" AS authorname, 
    responsible_executor."beard" AS executorid, 
    responsible_executor."beard_type" AS executorid_type, 
    responsible_executor."beard_type" AS executoridtype, 
    responsible_executor."unid" AS executorunid, 
    responsible_executor."shortname" AS executorname, 
    COALESCE(responsible_executor."deadline", resolution."ctrldeadline"::date) AS deadline, 
    CASE WHEN responsible_executor."deadline" IS NULL THEN 0 ELSE 1 END AS personaldeadline, 
    resolution."ctrldateexecution"::date AS executiondate, 
    resolution."id" AS rkkid, 
    resolution."id_type" AS rkkid_type
FROM "f_dp_tasksresolution" resolution
JOIN "f_dp_rkkbase" rkk_base ON resolution."id" = rkk_base."id" AND resolution."id_type" = rkk_base."id_type"
JOIN "f_dp_rkkbase_controller" resolution_controller ON resolution."id" = resolution_controller."owner" AND resolution."id_type" = resolution_controller."owner_type"
JOIN "ss_module" module ON rkk_base."module" = module."id" AND rkk_base."module_type" = module."id_type"
JOIN "ss_moduletype" module_type ON module."type" = module_type."id" AND module."type_type" = module_type."id_type"
JOIN "so_beard" author ON resolution."author" = author."id" AND resolution."author_type" = author."id_type"
JOIN LATERAL (
    SELECT 
        executor_beard."id" AS beard, 
        executor_beard."id_type" AS beard_type, 
        executor_beard."orig_shortname" AS shortname, 
        SUBSTRING(executor_beard."cmjunid", 0, 33) AS unid, 
        CASE WHEN executor_deadline."execdatecurr" IS NULL OR executor_deadline."execdatecurr"::date = '0001-1-1'::date THEN NULL ELSE executor_deadline."execdatecurr"::date END AS deadline
    FROM "f_dp_tasksresolution_er" responsible_executor
    JOIN "f_dp_tasksresolution_ec" executor ON responsible_executor."executorresp" = executor."executorcurr" AND responsible_executor."executorresp_type" = executor."executorcurr_type"
    JOIN "f_dp_tasksresolution_edc" executor_deadline ON executor."idx" = executor_deadline."idx"
    JOIN "so_beard" executor_beard ON responsible_executor."executorresp" = executor_beard."id" AND responsible_executor."executorresp_type" = executor_beard."id_type
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
) responsible_executor ON true
WHERE resolution."ctrldateexecution"::date <= COALESCE(resolution."ctrldeadline"::date, responsible_executor."deadline", resolution."ctrldateexecution"::date - 1);



---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
---------------------------------------------------------
Да, вы можете разбить ваш сложный запрос на несколько частей и затем объединить результаты на стороне JasperReports. Это поможет уменьшить нагрузку на базу данных и улучшить производительность. Вот пример подхода к разбивке вашего запроса:

1. **Разделение запроса на подзапросы**:
   - Создайте несколько подзапросов для извлечения данных из различных таблиц и сохраните их результаты как отдельные отчеты или таблицы в JasperReports.

2. **Объединение результатов в JasperReports**:
   - Используйте объединение данных на уровне отчета, чтобы собрать результаты подзапросов в один окончательный отчет.

### Пример разбивки запроса

#### Шаг 1: Извлечение данных из таблиц `person_stamp` и `group_member`

```sql
-- Подзапрос 1: Извлечение значений штампов для заданного пользователя
SELECT "stamp"
FROM "person_stamp"
WHERE "person" = 2976;
```

```sql
-- Подзапрос 2: Извлечение групп пользователя
SELECT DISTINCT gg."parent_group_id"
FROM "group_member" gm
INNER JOIN "group_group" gg ON gg."child_group_id" = gm."usergroup"
WHERE gm."person_id" = 2976;
```

#### Шаг 2: Основной запрос для данных резолюций с фильтрацией на основе результатов первых двух подзапросов

```sql
-- Подзапрос 3: Основные данные резолюций
SELECT 
    f_dp_resolution.*, 
    author."id" AS authorid, 
    author."id_type" AS authorid_type, 
    author."orig_shortname" AS authorname, 
    responsible_executor."beard" AS executorid, 
    responsible_executor."beard_type" AS executorid_type, 
    responsible_executor."unid" AS executorunid, 
    responsible_executor."shortname" AS executorname, 
    COALESCE(responsible_executor."deadline", f_dp_resolution."ctrldeadline"::date) AS deadline, 
    CASE WHEN responsible_executor."deadline" IS NULL THEN 0 ELSE 1 END AS personaldeadline, 
    f_dp_resolution."ctrldateexecution"::date AS executiondate, 
    rkk_base."id" AS rkkid, 
    rkk_base."id_type" AS rkkid_type
FROM 
    "f_dp_resolution"
JOIN "f_dp_resltnbase" resolution_base ON f_dp_resolution."id" = resolution_base."id" AND f_dp_resolution."id_type" = resolution_base."id_type"
JOIN "f_dp_resltnbase_cntrller" resolution_controller ON resolution_base."id" = resolution_controller."owner" AND resolution_base."id_type" = resolution_controller."owner_type"
JOIN "ss_module" module ON resolution_base."module" = module."id" AND resolution_base."module_type" = module."id_type"
JOIN "ss_moduletype" module_type ON module."type" = module_type."id" AND module."type_type" = module_type."id_type"
JOIN "so_beard" author ON resolution_base."author" = author."id" AND resolution_base."author_type" = author."id_type"
JOIN "f_dp_rkkbase" rkk_base ON f_dp_resolution."hierroot" = rkk_base."id" AND f_dp_resolution."hierroot_type" = rkk_base."id_type"
JOIN (
    SELECT 
        executor_beard."id" AS beard, 
        executor_beard."id_type" AS beard_type, 
        executor_beard."orig_shortname" AS shortname, 
        SUBSTRING(executor_beard."cmjunid", 0, 33) AS unid, 
        CASE WHEN executor_deadline."execdatecurr" IS NULL OR executor_deadline."execdatecurr"::date = '0001-1-1'::date THEN NULL ELSE executor_deadline."execdatecurr"::date END AS deadline
    FROM "f_dp_resltnbase_execresp" responsible_executor
    JOIN "f_dp_resltnbase_execcurr" executor ON responsible_executor."executorresp" = executor."executorcurr" AND responsible_executor."executorresp_type" = executor."executorcurr_type"
    JOIN "f_dp_resolution_exdatecur" executor_deadline ON executor."idx" = executor_deadline."idx"
    JOIN "so_beard" executor_beard ON responsible_executor."executorresp" = executor_beard."id" AND responsible_executor."executorresp_type" = executor_beard."id_type
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
) responsible_executor ON true
WHERE 
    f_dp_resolution."ctrldateexecution"::date <= COALESCE(f_dp_resolution."ctrldeadline"::date, responsible_executor."deadline", f_dp_resolution."ctrldateexecution"::date - 1);
```

#### Шаг 3: Запрос для данных задач

```sql
-- Подзапрос 4: Данные задач
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
    resolution."id" AS rkkid, 
    resolution."id_type" AS rkkid_type
FROM "f_dp_tasksresolution" resolution
JOIN "f_dp_rkkbase" rkk_base ON resolution."id" = rkk_base."id" AND resolution."id_type" = rkk_base."id_type"
JOIN "f_dp_rkkbase_controller" resolution_controller ON resolution."id" = resolution_controller."owner" AND resolution."id_type" = resolution_controller."owner_type"
JOIN "ss_module" module ON rkk_base."module" = module."id" AND rkk_base."module_type" = module."id_type"
JOIN "ss_moduletype" module_type ON module."type" = module_type."id" AND module."type_type" = module_type."id_type"
JOIN "so_beard" author ON resolution."author" = author."id" AND resolution."author_type" = author."id_type"
JOIN (
    SELECT 
        executor_beard."id" AS beard, 
        executor_beard."id_type" AS beard_type, 
        executor_beard."orig_shortname" AS shortname, 
        SUBSTRING(executor_beard."cmjunid", 0, 33) AS unid, 
        CASE WHEN executor_deadline."execdatecurr" IS NULL OR executor_deadline."execdatecurr"::date = '0001-1-1'::date THEN NULL ELSE executor_deadline."execdatecurr"::date END AS deadline
    FROM "f_dp_tasksresolution_er" responsible_executor
    JOIN "f_dp_tasksresolution_ec" executor ON responsible_executor."executorresp" = executor."executorcurr" AND responsible_executor."executorresp_type" = executor."executorcurr_type"
    JOIN "f_dp_tasksresolution_edc" executor_deadline ON executor."idx" = executor_deadline."idx"
    JOIN "so_beard" executor_beard ON responsible_executor."executorresp" = executor_beard."id" AND responsible_executor."executorresp_type" = executor_beard."id_type
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
) responsible_executor ON true
WHERE 
    resolution."ctrldateexecution"::date <= COALESCE(resolution."ctrldeadline"::date, responsible_executor."deadline", resolution."ctrldateexecution"::date - 1);
```

#### Шаг 4: Объединение результатов в JasperReports

На стороне JasperReports, создайте два отдельных запроса для подзапросов 3 и 4 и объедините их результаты с использованием subdataset или другого подходящего механизма для объединения данных.

Таким образом, вы можете уменьшить нагрузку на базу данных, улучшить производительность и получить желаемый результат.

