WITH person_stamp_values AS (
  SELECT 
    "stamp" 
  FROM 
    "person_stamp" 
  WHERE 
    "person" = '35056'
), 
cur_user_groups AS (
  SELECT 
    DISTINCT gg."parent_group_id" 
  FROM 
    "group_member" gm 
    INNER JOIN "group_group" gg ON gg."child_group_id" = gm."usergroup" 
  WHERE 
    gm."person_id" = '35056'
) 
SELECT 
  resolution."id", 
  resolution."id_type", 
  resolution."rkkid", 
  resolution."rkkid_type", 
  resolution."authorid", 
  resolution."authorid_type", 
  resolution."executorid", 
  resolution."executorid_type", 
  resolution."executorunid", 
  resolution."authorname", 
  resolution."executorname", 
  resolution."deadline", 
  resolution."personaldeadline" 
FROM 
  (
    WITH approved_report AS (
      SELECT 
        report."id", 
        report."id_type", 
        report."hierparent", 
        report."hierparent_type", 
        report."author", 
        report."author_type", 
        report."execdate" 
      FROM 
        (
          SELECT 
            f_dp_report.* 
          FROM 
            "f_dp_report" f_dp_report 
          WHERE 
            1 = 1 
            AND EXISTS (
              SELECT 
                1 
              FROM 
                "f_dp_rkkbase" ptf 
              WHERE 
                ptf."id" = f_dp_report."access_object_id" 
                AND (
                  ptf."security_stamp" IS NULL 
                  OR ptf."security_stamp" IN (
                    SELECT 
                      "stamp" 
                    FROM 
                      "person_stamp_values"
                  )
                )
            ) 
            AND EXISTS (
              SELECT 
                1 
              FROM 
                "f_dp_rkkbase_read" r 
              WHERE 
                r."group_id" IN (
                  SELECT 
                    "parent_group_id" 
                  FROM 
                    "cur_user_groups"
                ) 
                AND r."object_id" = f_dp_report."access_object_id" 
              LIMIT 
                1
            )
        ) report 
        JOIN "ss_module" module ON report."module" = module."id" 
        AND report."module_type" = module."id_type" 
        JOIN "ss_moduletype" module_type ON module."type" = module_type."id" 
        AND module."type_type" = module_type."id_type" 
        AND module_type."alias" NOT IN ('TempStorage', 'checkencoding') 
      WHERE 
        report."isdeleted" <> 1 
        AND report."execdate" IS NOT NULL 
        AND report."approvestatus" = 1 
      ORDER BY 
        report."execdate" DESC
    ) 
    SELECT 
      resolution."id" "id", 
      resolution."id_type" "id_type", 
      author."id" "authorid", 
      author."id_type" "authorid_type", 
      author."orig_shortname" "authorname", 
      executor_beard."id" "executorid", 
      executor_beard."id_type" "executorid_type", 
      SUBSTRING(executor_beard."cmjunid", 0, 33) "executorunid", 
      executor_beard."orig_shortname" "executorname", 
      COALESCE(
        CASE WHEN executor_deadline."execdatecurr" IS NULL 
        OR executor_deadline."execdatecurr" :: date = '0001-1-1' :: date THEN NULL ELSE executor_deadline."execdatecurr" :: date END, 
        resolution."ctrldeadline" :: date
      ) "deadline", 
      CASE WHEN (
        CASE WHEN executor_deadline."execdatecurr" IS NULL 
        OR executor_deadline."execdatecurr" :: date = '0001-1-1' :: date THEN NULL ELSE executor_deadline."execdatecurr" :: date END
      ) IS NULL THEN 0 ELSE 1 END "personaldeadline", 
      rkk_base."id" "rkkid", 
      rkk_base."id_type" "rkkid_type" 
    FROM 
      (
        SELECT 
          f_dp_resolution.* 
        FROM 
          "f_dp_resolution" f_dp_resolution 
        WHERE 
          1 = 1 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase" ptf 
              INNER JOIN "f_dp_resltnbase" rt ON ptf."id" = rt."access_object_id" 
            WHERE 
              rt."id" = f_dp_resolution."id" 
              AND ptf."id" = rt."access_object_id" 
              AND (
                ptf."security_stamp" IS NULL 
                OR ptf."security_stamp" IN (
                  SELECT 
                    "stamp" 
                  FROM 
                    "person_stamp_values"
                )
              )
          ) 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase_read" r 
              INNER JOIN "f_dp_resltnbase" rt ON r."object_id" = rt."access_object_id" 
            WHERE 
              r."group_id" IN (
                SELECT 
                  "parent_group_id" 
                FROM 
                  "cur_user_groups"
              ) 
              AND rt."id" = f_dp_resolution."id" 
            LIMIT 
              1
          )
      ) resolution 
      JOIN "f_dp_resltnbase" resolution_base ON resolution."id" = resolution_base."id" 
      AND resolution."id_type" = resolution_base."id_type" 
      JOIN "ss_module" module ON resolution_base."module" = module."id" 
      AND resolution_base."module_type" = module."id_type" 
      JOIN "ss_moduletype" module_type ON module."type" = module_type."id" 
      AND module."type_type" = module_type."id_type" 
      AND module_type."alias" NOT IN ('TempStorage', 'checkencoding') 
      JOIN "so_beard" author ON resolution_base."author" = author."id" 
      AND resolution_base."author_type" = author."id_type" 
      JOIN "f_dp_resltnbase_execresp" responsible_executor ON resolution_base."id" = responsible_executor."owner" 
      AND resolution_base."id_type" = responsible_executor."owner_type" 
      JOIN (
        SELECT 
          f_dp_resltnbase_execcurr.* 
        FROM 
          "f_dp_resltnbase_execcurr" f_dp_resltnbase_execcurr 
        WHERE 
          1 = 1 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase" ptf 
            WHERE 
              ptf."id" = f_dp_resltnbase_execcurr."access_object_id" 
              AND (
                ptf."security_stamp" IS NULL 
                OR ptf."security_stamp" IN (
                  SELECT 
                    "stamp" 
                  FROM 
                    "person_stamp_values"
                )
              )
          ) 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase_read" r 
            WHERE 
              r."group_id" IN (
                SELECT 
                  "parent_group_id" 
                FROM 
                  "cur_user_groups"
              ) 
              AND r."object_id" = f_dp_resltnbase_execcurr."access_object_id" 
            LIMIT 
              1
          )
      ) executor ON responsible_executor."executorresp" = executor."executorcurr" 
      AND responsible_executor."executorresp_type" = executor."executorcurr_type" 
      AND responsible_executor."owner" = executor."owner" 
      AND responsible_executor."owner_type" = executor."owner_type" 
      JOIN (
        SELECT 
          f_dp_resolution_exdatecur.* 
        FROM 
          "f_dp_resolution_exdatecur" f_dp_resolution_exdatecur 
        WHERE 
          1 = 1 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase" ptf 
            WHERE 
              ptf."id" = f_dp_resolution_exdatecur."access_object_id" 
              AND (
                ptf."security_stamp" IS NULL 
                OR ptf."security_stamp" IN (
                  SELECT 
                    "stamp" 
                  FROM 
                    "person_stamp_values"
                )
              )
          ) 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase_read" r 
            WHERE 
              r."group_id" IN (
                SELECT 
                  "parent_group_id" 
                FROM 
                  "cur_user_groups"
              ) 
              AND r."object_id" = f_dp_resolution_exdatecur."access_object_id" 
            LIMIT 
              1
          )
      ) executor_deadline ON executor."idx" = executor_deadline."idx" 
      AND responsible_executor."owner" = executor_deadline."owner" 
      AND responsible_executor."owner_type" = executor_deadline."owner_type" 
      JOIN "so_beard" executor_beard ON responsible_executor."executorresp" = executor_beard."id" 
      AND responsible_executor."executorresp_type" = executor_beard."id_type" 
      AND (
        SELECT 
          report."execdate" 
        FROM 
          "approved_report" report 
        WHERE 
          responsible_executor."owner" = report."hierparent" 
          AND responsible_executor."owner_type" = report."hierparent_type" 
          AND executor_beard."id" = report."author" 
          AND executor_beard."id_type" = report."author_type" 
        LIMIT 
          1
      ) IS NULL 
      JOIN "f_dp_rkkbase" rkk_base ON resolution."hierroot" = rkk_base."id" 
      AND resolution."hierroot_type" = rkk_base."id_type" 
      LEFT JOIN "f_dp_resltnbase_cntrller" resolution_controller ON resolution_base."id" = resolution_controller."owner" 
      AND resolution_base."id_type" = resolution_controller."owner_type" 
      LEFT JOIN "so_beard" controller ON resolution_controller."controller" = controller."id" 
      AND resolution_controller."controller_type" = controller."id_type" 
    WHERE 
      (
        0 <> 1 
        OR (
          author."id" IN (0) 
          AND author."id_type" = 0
        )
      ) 
      AND (
        0 <> 1 
        OR (
          controller."id" IN (0) 
          AND controller."id_type" = 0
        )
      ) 
      AND (
        1 <> 1 
        OR (
          COALESCE(
            CASE WHEN executor_deadline."execdatecurr" IS NULL 
            OR executor_deadline."execdatecurr" :: date = '0001-1-1' :: date THEN NULL ELSE executor_deadline."execdatecurr" :: date END, 
            resolution."ctrldeadline" :: date
          ) BETWEEN '2023-08-01' 
          AND '2023-08-31'
        )
      ) 
      AND resolution_base."ctrliscontrolled" = 1 
      AND resolution."isproject" <> 1 
      AND resolution."executioncanceldate" IS NULL 
      AND resolution."ctrldateexecution" IS NULL 
      AND resolution_base."isdeleted" <> 1 
    UNION ALL 
    SELECT 
      resolution."id" "id", 
      resolution."id_type" "id_type", 
      author."id" "authorid", 
      author."id_type" "authorid_type", 
      author."orig_shortname" "authorname", 
      executor_beard."id" "executorid", 
      executor_beard."id_type" "executorid_type", 
      SUBSTRING(executor_beard."cmjunid", 0, 33) "executorunid", 
      executor_beard."orig_shortname" "executorname", 
      COALESCE(
        CASE WHEN executor_deadline."execdatecurr" IS NULL 
        OR executor_deadline."execdatecurr" :: date = '0001-1-1' :: date THEN NULL ELSE executor_deadline."execdatecurr" :: date END, 
        resolution."ctrldeadline" :: date
      ) "deadline", 
      CASE WHEN (
        CASE WHEN executor_deadline."execdatecurr" IS NULL 
        OR executor_deadline."execdatecurr" :: date = '0001-1-1' :: date THEN NULL ELSE executor_deadline."execdatecurr" :: date END
      ) IS NULL THEN 0 ELSE 1 END "personaldeadline", 
      resolution."id" "rkkid", 
      resolution."id_type" "rkkid_type" 
    FROM 
      (
        SELECT 
          f_dp_tasksresolution.* 
        FROM 
          "f_dp_tasksresolution" f_dp_tasksresolution 
        WHERE 
          1 = 1 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase" root_type 
            WHERE 
              root_type."id" = f_dp_tasksresolution."id" 
              AND (
                root_type."security_stamp" IS NULL 
                OR root_type."security_stamp" IN (
                  SELECT 
                    "stamp" 
                  FROM 
                    "person_stamp_values"
                )
              )
          ) 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase_read" r 
              INNER JOIN "f_dp_rkkbase" rt ON r."object_id" = rt."access_object_id" 
            WHERE 
              r."group_id" IN (
                SELECT 
                  "parent_group_id" 
                FROM 
                  "cur_user_groups"
              ) 
              AND rt."id" = f_dp_tasksresolution."id" 
            LIMIT 
              1
          )
      ) resolution 
      JOIN "so_beard" author ON resolution."author" = author."id" 
      AND resolution."author_type" = author."id_type" 
      JOIN "f_dp_tasksresolution_er" responsible_executor ON resolution."id" = responsible_executor."owner" 
      AND resolution."id_type" = responsible_executor."owner_type" 
      JOIN (
        SELECT 
          f_dp_tasksresolution_ec.* 
        FROM 
          "f_dp_tasksresolution_ec" f_dp_tasksresolution_ec 
        WHERE 
          1 = 1 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase" ptf 
            WHERE 
              ptf."id" = f_dp_tasksresolution_ec."access_object_id" 
              AND (
                ptf."security_stamp" IS NULL 
                OR ptf."security_stamp" IN (
                  SELECT 
                    "stamp" 
                  FROM 
                    "person_stamp_values"
                )
              )
          ) 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase_read" r 
            WHERE 
              r."group_id" IN (
                SELECT 
                  "parent_group_id" 
                FROM 
                  "cur_user_groups"
              ) 
              AND r."object_id" = f_dp_tasksresolution_ec."access_object_id" 
            LIMIT 
              1
          )
      ) executor ON responsible_executor."executorresp" = executor."executorcurr" 
      AND responsible_executor."executorresp_type" = executor."executorcurr_type" 
      AND responsible_executor."owner" = executor."owner" 
      AND responsible_executor."owner_type" = executor."owner_type" 
      JOIN (
        SELECT 
          f_dp_tasksresolution_edc.* 
        FROM 
          "f_dp_tasksresolution_edc" f_dp_tasksresolution_edc 
        WHERE 
          1 = 1 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase" ptf 
            WHERE 
              ptf."id" = f_dp_tasksresolution_edc."access_object_id" 
              AND (
                ptf."security_stamp" IS NULL 
                OR ptf."security_stamp" IN (
                  SELECT 
                    "stamp" 
                  FROM 
                    "person_stamp_values"
                )
              )
          ) 
          AND EXISTS (
            SELECT 
              1 
            FROM 
              "f_dp_rkkbase_read" r 
            WHERE 
              r."group_id" IN (
                SELECT 
                  "parent_group_id" 
                FROM 
                  "cur_user_groups"
              ) 
              AND r."object_id" = f_dp_tasksresolution_edc."access_object_id" 
            LIMIT 
              1
          )
      ) executor_deadline ON executor."idx" = executor_deadline."idx" 
      AND responsible_executor."owner" = executor_deadline."owner" 
      AND responsible_executor."owner_type" = executor_deadline."owner_type" 
      JOIN "so_beard" executor_beard ON responsible_executor."executorresp" = executor_beard."id" 
      AND responsible_executor."executorresp_type" = executor_beard."id_type" 
      AND (
        SELECT 
          report."execdate" 
        FROM 
          "approved_report" report 
        WHERE 
          responsible_executor."owner" = report."hierparent" 
          AND responsible_executor."owner_type" = report."hierparent_type" 
          AND executor_beard."id" = report."author" 
          AND executor_beard."id_type" = report."author_type" 
        LIMIT 
          1
      ) IS NULL 
      JOIN "f_dp_rkkbase" rkk_base ON resolution."id" = rkk_base."id" 
      AND resolution."id_type" = rkk_base."id_type" 
      JOIN "ss_module" module ON rkk_base."module" = module."id" 
      AND rkk_base."module_type" = module."id_type" 
      JOIN "ss_moduletype" module_type ON module."type" = module_type."id" 
      AND module."type_type" = module_type."id_type" 
      AND module_type."alias" NOT IN ('TempStorage', 'checkencoding') 
      LEFT JOIN "f_dp_rkkbase_controller" resolution_controller ON resolution."id" = resolution_controller."owner" 
      AND resolution."id_type" = resolution_controller."owner_type" 
      LEFT JOIN "so_beard" controller ON resolution_controller."controller" = controller."id" 
      AND resolution_controller."controller_type" = controller."id_type" 
    WHERE 
      (
        0 <> 1 
        OR (
          author."id" IN (0) 
          AND author."id_type" = 0
        )
      ) 
      AND (
        0 <> 1 
        OR (
          controller."id" IN (0) 
          AND controller."id_type" = 0
        )
      ) 
      AND (
        1 <> 1 
        OR (
          COALESCE(
            CASE WHEN executor_deadline."execdatecurr" IS NULL 
            OR executor_deadline."execdatecurr" :: date = '0001-1-1' :: date THEN NULL ELSE executor_deadline."execdatecurr" IS NULL 
            OR executor_deadline."execdatecurr" :: date = '0001-1-1' :: date THEN NULL ELSE executor_deadline."execdatecurr" :: date END, 
            resolution."ctrldeadline" :: date
          ) BETWEEN '2023-08-01' 
          AND '2023-08-31'
        )
      ) 
      AND resolution."ctrliscontrolled" = 1 
      AND resolution."isproject" <> 1 
      AND resolution."executioncanceldate" IS NULL 
      AND resolution."ctrldateexecution" IS NULL 
      AND rkk_base."isdeleted" <> 1
  ) AS resolution 
GROUP BY 
  resolution."authorid", 
  resolution."authorid_type", 
  resolution."authorname", 
  resolution."executorid", 
  resolution."executorid_type", 
  resolution."executorunid", 
  resolution."executorname", 
  resolution."deadline", 
  resolution."personaldeadline", 
  resolution."rkkid", 
  resolution."rkkid_type", 
  resolution."id", 
  resolution."id_type" 
ORDER BY 
  resolution."authorid", 
  resolution."executorid", 
  resolution."deadline"
