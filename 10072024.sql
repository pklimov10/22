WITH rkkvisa AS (
  SELECT  
    t."regregistr_res",  
    t."regregistr_res_type",  
    COUNT(t."id_res") AS "visa",  
    t."complect" AS "complect"
  FROM (
    SELECT  
      visaauthor."id" AS "regregistr_res",  
      visaauthor."id_type" AS "regregistr_res_type",  
      rkk."id" AS "id_res",  
      rkk."id_type" AS "id_res_type",  
      rkkb."complect" AS "complect"
    FROM (
      SELECT f_dp_rkkbase.*
      FROM "f_dp_rkkbase" f_dp_rkkbase  
      WHERE EXISTS (
          SELECT 1
          FROM "f_dp_rkkbase" ptf  
          WHERE ptf."id" = f_dp_rkkbase."access_object_id"  
            AND (
              ptf."security_stamp" IS NULL  
              OR ptf."security_stamp" IN ( 
                SELECT "stamp"  
                FROM "person_stamp_values"
              ) 
            ) 
        )  
        AND EXISTS (
          SELECT 1
          FROM "f_dp_rkkbase_read" r  
          WHERE r."group_id" IN ( 
              SELECT "parent_group_id"  
              FROM "cur_user_groups"
            )  
            AND r."object_id" = f_dp_rkkbase."access_object_id"  
            AND (r."module" IS NULL OR f_dp_rkkbase."module" = r."module")  
          LIMIT 1 
        ) 
    ) rkkb  
    JOIN "f_dp_rkk" rkk ON rkk."id" = rkkb."id"  
    JOIN (
      SELECT apr_list.*
      FROM "apr_list" apr_list  
      WHERE EXISTS (
        SELECT 1
        FROM "apr_listortempl_read" r  
        INNER JOIN "apr_listortempl" rt ON r."object_id" = rt."access_object_id"  
        WHERE r."group_id" IN (
            SELECT "parent_group_id"  
            FROM "cur_user_groups"
          )  
          AND rt."id" = apr_list."id"  
        LIMIT 1 
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
    LEFT JOIN "ss_module" SS_Module ON SS_Module."id" = aprans."module"  
    LEFT JOIN "ss_moduletype" SS_ModuleType ON SS_Module."type" = SS_ModuleType."id"  
    LEFT JOIN (
      SELECT Apr_ApprList_Delegate.*
      FROM "apr_apprlist_delegate" Apr_ApprList_Delegate  
      WHERE EXISTS (
        SELECT 1
        FROM "apr_listortempl_read" r  
        WHERE r."group_id" IN (
            SELECT "parent_group_id"  
            FROM "cur_user_groups"
          )  
          AND r."object_id" = apr_apprlist_delegate."access_object_id"
        LIMIT 1 
      ) 
    ) Deleg ON Deleg."owner" = options."owner"  
    LEFT JOIN "person" person ON Deleg."created_by" = person."id"  
    LEFT JOIN "so_personsys" sp ON person."id" = sp."platformperson"  
    LEFT JOIN "so_appointment" so_app ON sp."id" = so_app."person"  
    JOIN "clerk" ON clerk."clerk_id" = visaauthor."id"
    WHERE (
      ('-4'  = '-4'  
      AND rkkb."initbranch" IS NOT NULL)
      OR rkkb."initbranch" IN (11, -4)
    )  
    AND rkkb."isdeleted" = 0  
    AND al."inprocess" <> 'Stopped'  
    AND rkk."regnumcnt" IS NOT NULL  
    AND (
      (SS_ModuleType."alias" <> 'TempStorage'  
      AND apaprans."result" IS NOT NULL)
      OR aprans."id" IS NULL
    )

    UNION ALL

    SELECT  
      brdauth."id" AS "regregistr_res",  
      brdauth."id_type" AS "regregistr_res_type",  
      rkk."id" AS "id_res",  
      rkk."id_type" AS "id_res_type",  
      rkkb."complect" AS "complect"
    FROM (
      SELECT f_dp_rkkbase.*
      FROM "f_dp_rkkbase" f_dp_rkkbase  
      WHERE EXISTS (
          SELECT 1
          FROM "f_dp_rkkbase" ptf  
          WHERE ptf."id" = f_dp_rkkbase."access_object_id"  
            AND (
              ptf."security_stamp" IS NULL  
              OR ptf."security_stamp" IN ( 
                SELECT "stamp"  
                FROM "person_stamp_values"
              ) 
            ) 
        )  
        AND EXISTS (
          SELECT 1
          FROM "f_dp_rkkbase_read" r  
          WHERE r."group_id" IN ( 
              SELECT "parent_group_id"  
              FROM "cur_user_groups"
            )  
            AND r."object_id" = f_dp_rkkbase."access_object_id"  
            AND (r."module" IS NULL OR f_dp_rkkbase."module" = r."module")  
          LIMIT 1 
        ) 
    ) rkkb  
    JOIN "f_dp_rkk" rkk ON rkk."id" = rkkb."id"  
    JOIN (
      SELECT apr_list.*
      FROM "apr_list" apr_list  
      WHERE EXISTS (
        SELECT 1
        FROM "apr_listortempl_read" r  
        INNER JOIN "apr_listortempl" rt ON r."object_id" = rt."access_object_id"  
        WHERE r."group_id" IN (
            SELECT "parent_group_id"  
            FROM "cur_user_groups"
          )  
          AND rt."id" = apr_list."id"  
        LIMIT 1 
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
    LEFT JOIN "ss_module" SS_Module ON SS_Module."id" = aprans."module"  
    LEFT JOIN "ss_moduletype" SS_ModuleType ON SS_Module."type" = SS_ModuleType."id"  
    LEFT JOIN (
      SELECT Apr_ApprList_Delegate.*
      FROM "apr_apprlist_delegate" Apr_ApprList_Delegate  
      WHERE EXISTS (
        SELECT 1
        FROM "apr_listortempl_read" r  
        WHERE r."group_id" IN (
            SELECT "parent_group_id"  
            FROM "cur_user_groups"
          )  
          AND r."object_id" = apr_apprlist_delegate."access_object_id"
        LIMIT 1 
      ) 
    ) Deleg ON Deleg."owner" = options."owner"  
    LEFT JOIN "person" person ON Deleg."created_by" = person."id"  
    LEFT JOIN "so_personsys" sp ON person."id" = sp."platformperson"  
    LEFT JOIN "so_appointment" so_app ON sp."id" = so_app."person"  
    JOIN "clerk" ON clerk."clerk_id" = brdauth."id"
    WHERE (
      ('-4'  = '-4'  
      AND rkkb."initbranch" IS NOT NULL)
      OR rkkb."initbranch" IN (11, -4)
    )  
    AND rkkb."isdeleted" = 0  
    AND al."inprocess" <> 'Stopped'  
    AND rkk."regnumcnt" IS NOT NULL  
    AND (
      (SS_ModuleType."alias" <> 'TempStorage'  
      AND apaprans."result" IS NOT NULL)
      OR aprans."id" IS NULL
    )
  ) AS t  
  GROUP BY  
    t."regregistr_res",  
    t."regregistr_res_type",  
    t."complect" 
)
