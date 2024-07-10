EXPLAIN ANALYZE
WITH person_stamp_values AS (
  SELECT 
    "stamp" 
  FROM 
    "person_stamp" 
  WHERE 
    "person" = 10153
), 
cur_user_groups AS (
  SELECT 
    DISTINCT gg."parent_group_id" 
  FROM 
    "group_member" gm 
    INNER JOIN "group_group" gg ON gg."child_group_id" = gm."usergroup" 
  WHERE 
    gm."person_id" = 10153
), 
rkkvisa AS (
  SELECT 
    t."regregistr_res", 
    t."regregistr_res_type", 
    COUNT(t."id_res") "visa", 
    t."complect" "complect" 
  FROM 
    (
      SELECT 
        CASE WHEN visaauthor."id" IS NOT NULL THEN visaauthor."id" WHEN brdauth."id" IS NOT NULL THEN brdauth."id" END "regregistr_res", 
        CASE WHEN visaauthor."id" IS NOT NULL THEN visaauthor."id_type" WHEN brdauth."id" IS NOT NULL THEN brdauth."id_type" END "regregistr_res_type", 
        rkk."id" "id_res", 
        rkk."id_type" "id_res_type", 
        rkk."id" "id_res_type", 
        rkk."id_type" "id_res_type_type", 
        rkkb."complect" "complect" 
      FROM 
        (
          SELECT 
            f_dp_rkkbase.* 
          FROM 
            "f_dp_rkkbase" f_dp_rkkbase 
          WHERE 
            1 = 1 
            AND EXISTS (
              SELECT 
                1 
              FROM 
                "f_dp_rkkbase" ptf 
              WHERE 
                ptf."id" = f_dp_rkkbase."access_object_id" 
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
                AND r."object_id" = f_dp_rkkbase."access_object_id" 
                AND (
                  r."module" IS NULL 
                  OR f_dp_rkkbase."module" = r."module"
                ) 
              LIMIT 
                1
            )
        ) rkkb 
        JOIN "f_dp_rkk" rkk ON rkk."id" = rkkb."id" 
        JOIN (
          SELECT 
            apr_list.* 
          FROM 
            "apr_list" apr_list 
          WHERE 
            1 = 1 
            AND EXISTS (
              SELECT 
                1 
              FROM 
                "apr_listortempl_read" r 
                INNER JOIN "apr_listortempl" rt ON r."object_id" = rt."access_object_id" 
              WHERE 
                r."group_id" IN (
                  SELECT 
                    "parent_group_id" 
                  FROM 
                    "cur_user_groups"
                ) 
                AND rt."id" = apr_list."id" 
              LIMIT 
                1
            )
        ) al ON al."hierparent" = rkk."id" 
        LEFT JOIN "apr_apprlist_options" options ON al."id" = options."owner" 
        LEFT JOIN "apr_apprlist_delegatefrom" delegate ON delegate."owner" = al."id" 
        JOIN "so_beard" sb2 ON options."sendtosh" = sb2."id" 
        LEFT JOIN "apr_answer" aprans ON sb2."id" = aprans."realvise" 
        AND aprans."hierparent" = options."owner" 
        LEFT JOIN "apr_appranswer" apaprans ON aprans."id" = apaprans."id" 
        LEFT JOIN "so_beard" visaauthor ON visaauthor."id" = aprans."idauthor_answere" 
        AND aprans."idauthor_answere" != aprans."realvise" 
        LEFT JOIN "ss_module" ON SS_Module."id" = aprans."module" 
        LEFT JOIN "ss_moduletype" ON SS_Module."type" = SS_ModuleType."id" 
        LEFT JOIN (
          SELECT 
            Apr_ApprList_Delegate.* 
          FROM 
            "apr_apprlist_delegate" Apr_ApprList_Delegate 
          WHERE 
            1 = 1 
            AND EXISTS (
              SELECT 
                1 
              FROM 
                "apr_listortempl_read" r 
              WHERE 
                r."group_id" IN (
                  SELECT 
                    "parent_group_id" 
                  FROM 
                    "cur_user_groups"
                ) 
                AND r."object_id" = apr_apprlist_delegate."access_object_id" 
              LIMIT 
                1
            )
        ) Deleg ON Deleg."owner" = options."owner" 
        LEFT JOIN "person" ON Deleg."created_by" = person."id" 
        LEFT JOIN "so_personsys" sp ON person."id" = sp."platformperson" 
        LEFT JOIN "so_appointment" so_app ON sp."id" = so_app."person" 
        LEFT JOIN "so_beard" brdauth ON brdauth."id" = so_app."beard" 
        JOIN "clerk" ON clerk."clerk_id" = visaauthor."id" 
        OR brdauth."id" = clerk."clerk_id" 
      WHERE 
        (
          (
            '-4' = '-4' 
            AND rkkb."initbranch" IS NOT NULL
          ) 
          OR rkkb."initbranch" IN (11, -4)
        ) 
        AND rkkb."isdeleted" = 0 
        AND al."inprocess" <> 'Stoped' 
        AND rkk."regnumcnt" IS NOT NULL 
        AND (
          aprans."crdate" :: date BETWEEN '-4' :: date 
          AND '-4' :: date 
          OR delegate."created_date" :: date BETWEEN '-4' :: date 
          AND '11,-4' :: date
        ) 
        AND (
          (
            SS_ModuleType."alias" <> 'TempStorage' 
            AND apaprans."result" IS NOT NULL
          ) 
          OR aprans."id" IS NULL
        )
    ) AS t 
  GROUP BY 
    t."regregistr_res", 
    t."regregistr_res_type", 
    t."complect"
)
SELECT * FROM rkkvisa;
