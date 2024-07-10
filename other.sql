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
names AS (
  SELECT 
    parph."id" "par_id", 
    parph."id_type" "par_id_type", 
    CASE WHEN b."orig_shortname" = '<<VACANCY>>' THEN 'Вакансия, ' || b."orig_postname" ELSE b."orig_shortname" || ', ' || b."orig_postname" END "name" 
  FROM 
    "so_parent_ph" parph 
    JOIN "so_posthead" posth ON posth."id" = parph."owner" 
    JOIN "so_appointmenthead" apphead ON apphead."post" = posth."id" 
    AND apphead."accessredirect" IS NULL 
    JOIN "so_appointment" app ON app."id" = apphead."id" 
    JOIN "so_beard" b ON b."id" = app."beard" 
  UNION ALL 
  SELECT 
    parsu."id" "par_id", 
    parsu."id_type" "par_id_type", 
    sunit."shortname" "name" 
  FROM 
    "so_parent_su" parsu 
    JOIN "so_structureunit" sunit ON sunit."id" = parsu."owner"
), 
mod AS (
  SELECT 
    nam."name" "rp_name", 
    clpost."clerk", 
    clpost."clerk_type", 
    mtype."name", 
    mtype."id" "moduletype_id", 
    mtype."id_type" "moduletype_id_type" 
  FROM 
    "so_rsregplaceclerk_post" clpost NATURAL 
    JOIN "so_rsregplaceclerk" cl 
    JOIN "so_rsregplace" rp ON cl."owner" = rp."id" 
    JOIN "so_parent" ON SO_Parent."id" = rp."owner" 
    LEFT JOIN "so_parent_su" ON SO_Parent_SU."id" = SO_Parent."id" 
    LEFT JOIN "so_parent_ph" ON SO_Parent_PH."id" = SO_Parent."id" 
    JOIN "so_posthead" head ON COALESCE(
      SO_Parent_SU."owner", SO_Parent_PH."owner"
    ) = head."id" 
    JOIN "so_appointmenthead" sa3 ON head."id" = sa3."post" 
    JOIN "so_appointment" sa ON sa3."id" = sa."id" 
    JOIN "so_beard" sb ON sb."id" = sa."beard" 
    JOIN "ss_moduletype" mtype ON rp."moduletype" = mtype."id" 
    JOIN "names" nam ON nam."par_id" = rp."owner" 
  WHERE 
    (
      (
        '11,-4' = '-4' 
        AND '-4' <> '-4' 
        AND sb."id" IN (-4)
      ) 
      OR (
        '-4' = '-4' 
        AND (
          '11,-4' <> '-4' 
          AND sb."id" IN (
            WITH RECURSIVE tree AS (
              SELECT 
                so_beard."id", 
                so_beard."hierparent" 
              FROM 
                "so_beard" 
              WHERE 
                so_beard."id" IN (11, -4) 
              UNION ALL 
              SELECT 
                so_beard."id", 
                so_beard."hierparent" 
              FROM 
                "so_beard" 
                JOIN "tree" ON tree."id" = so_beard."hierparent" 
              WHERE 
                (
                  so_beard."orig_type" = 1 
                  OR (
                    so_beard."orig_type" = 2 
                    AND (
                      SELECT 
                        "isisolated" 
                      FROM 
                        "so_department" 
                        JOIN "so_structureunit" ON so_structureunit."id" = so_department."id" 
                      WHERE 
                        so_structureunit."beard" = so_beard."id"
                    ) <> 1
                  )
                ) 
                AND so_beard."isactive" = 1
            ) 
            SELECT 
              tree."id" 
            FROM 
              "tree"
          )
        )
      ) 
      OR (
        '-4' <> '-4' 
        AND '11,-4' <> '-4' 
        AND sb."id" IN (-4)
      ) 
      OR (
        '-4' = '-4' 
        AND '11,-4' = '-4'
      )
    ) 
    AND (
      mtype."alias" IN ('-4') 
      OR (
        '''-4''' LIKE '%-4%' 
        AND mtype."alias" IN (
          'InternalDocs', 'InputDocs', 'OutputDocs', 
          'Missions', 'Protocols', 'ContractsLite', 
          'Directives', 'AttorneyDocs'
        )
      )
    ) 
  UNION 
  SELECT 
    nam."name" "rp_name", 
    clpost."clerk", 
    clpost."clerk_type", 
    mtype."name", 
    mtype."id" "moduletype_id", 
    mtype."id_type" "moduletype_id_type" 
  FROM 
    "so_rsregplaceclerk_post" clpost NATURAL 
    JOIN "so_rsregplaceclerk" cl 
    JOIN "so_rsregplace" rp ON cl."owner" = rp."id" 
    JOIN "so_parent" ON SO_Parent."id" = rp."owner" 
    LEFT JOIN "so_parent_su" ON SO_Parent_SU."id" = SO_Parent."id" 
    LEFT JOIN "so_parent_ph" ON SO_Parent_PH."id" = SO_Parent."id" 
    JOIN "so_unit" ON COALESCE(
      SO_Parent_SU."owner", SO_Parent_PH."owner"
    ) = SO_Unit."id" 
    LEFT JOIN "so_structureunit" ON SO_Unit."id" = SO_StructureUnit."id" 
    JOIN "so_beard" sb ON sb."id" = SO_StructureUnit."beard" 
    JOIN "ss_moduletype" mtype ON rp."moduletype" = mtype."id" 
    JOIN "names" nam ON nam."par_id" = rp."owner" 
  WHERE 
    (
      (
        '11,-4' = '-4' 
        AND '-4' <> '-4' 
        AND sb."id" IN (-4)
      ) 
      OR (
        '-4' 1 = '-4' 
        AND (
          '-4' 2 <> '-4' 
          AND sb."id" IN (
            WITH RECURSIVE tree AS (
              SELECT 
                so_beard."id", 
                so_beard."hierparent" 
              FROM 
                "so_beard" 
              WHERE 
                so_beard."id" IN (11, -4) 
              UNION ALL 
              SELECT 
                so_beard."id", 
                so_beard."hierparent" 
              FROM 
                "so_beard" 
                JOIN "tree" ON tree."id" = so_beard."hierparent" 
              WHERE 
                (
                  so_beard."orig_type" = 1 
                  OR (
                    so_beard."orig_type" = 2 
                    AND (
                      SELECT 
                        "isisolated" 
                      FROM 
                        "so_department" 
                        JOIN "so_structureunit" ON so_structureunit."id" = so_department."id" 
                      WHERE 
                        so_structureunit."beard" = so_beard."id"
                    ) <> 1
                  )
                ) 
                AND so_beard."isactive" = 1
            ) 
            SELECT 
              tree."id" 
            FROM 
              "tree"
          )
        )
      ) 
      OR (
        '-4' 3 <> '-4' 
        AND '-4' 4 <> '-4' 
        AND sb."id" IN (-4)
      ) 
      OR (
        '-4' 5 = '-4' 
        AND '-4' 6 = '-4'
      )
    ) 
    AND (
      mtype."alias" IN ('-4') 
      OR (
        '-4' 7 LIKE '%-4%' 
        AND mtype."alias" IN (
          'InternalDocs', 'InputDocs', 'OutputDocs', 
          'Missions', 'Protocols', 'ContractsLite', 
          'Directives', 'AttorneyDocs'
        )
      )
    )
), 
clerk AS (
  SELECT 
    temp."id" "clerk_id", 
    temp."id_type" "clerk_id_type" 
  FROM 
    (
      SELECT 
        b."id", 
        b."id_type" 
      FROM 
        "so_postplain" postpl NATURAL 
        JOIN "so_post" post NATURAL 
        JOIN "so_unit" unit 
        JOIN "so_appointmentplain" appplain ON appplain."post" = post."id" 
        JOIN "so_appointment" app ON app."id" = appplain."id" 
        JOIN "so_beard" b ON b."id" = app."beard" 
        JOIN "mod" mType ON mtype."clerk" = post."id" 
        JOIN "so_rspost" rsp ON rsp."owner" = post."id" 
        AND rsp."moduletype" = mtype."moduletype_id" 
      UNION ALL 
      SELECT 
        b."id", 
        b."id_type" 
      FROM 
        "so_posthead" ph NATURAL 
        JOIN "so_post" post NATURAL 
        JOIN "so_unit" unit 
        JOIN "so_appointmenthead" apphead ON apphead."post" = post."id" 
        JOIN "so_appointment" app ON app."id" = apphead."id" 
        JOIN "so_beard" b ON b."id" = app."beard" 
        JOIN "mod" mType ON mtype."clerk" = post."id" 
        JOIN "so_rspost" rsp ON rsp."owner" = post."id" 
        AND rsp."moduletype" = mtype."moduletype_id"
    ) temp 
  WHERE 
    '-4' 8 = '-2' 
    OR '-4' 9 = '-4' 
    OR temp."id" IN (11333, -2) 
  GROUP BY 
    temp."id", 
    temp."id_type"
), 
docs AS (
  SELECT 
    t."regregistr_res", 
    t."regregistr_res_type", 
    COUNT(t."id_res") "documents", 
    t."complect" "complect" 
  FROM 
    (
      SELECT 
        rkk."id" "id_res", 
        rkk."id_type" "id_res_type", 
        rkk."id" "id_res_type", 
        rkk."id_type" "id_res_type_type", 
        rkk."regregistrator" "regregistr_res", 
        rkk."regregistrator_type" "regregistr_res_type", 
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
      WHERE 
        (
          (
            '11,-4' = '-4' 
            AND rkkb."initbranch" IS NOT NULL
          ) 
          OR rkkb."initbranch" IN (11, -4)
        ) 
        AND "isdeleted" = 0 
        AND rkk."regnumcnt" IS NOT NULL 
        AND rkk."regdate" :: DATE BETWEEN '2024-05-13 00:00:00' :: DATE 
        AND '-4' 2 :: DATE
    ) AS t 
  GROUP BY 
    t."regregistr_res", 
    t."regregistr_res_type", 
    t."complect", 
    t."regregistr_res_type"
), 
rkk AS (
  SELECT 
    COUNT(rkk."id") "resolutions", 
    creators."created_id" "author", 
    creators."created_id_type" "author_type", 
    rkkb."complect" "complect" 
  FROM 
    (
      SELECT 
        f_dp_rkk.* 
      FROM 
        "f_dp_rkk" f_dp_rkk 
      WHERE 
        1 = 1 
        AND EXISTS (
          SELECT 
            1 
          FROM 
            "f_dp_rkkbase" root_type 
          WHERE 
            root_type."id" = f_dp_rkk."id" 
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
            AND rt."id" = f_dp_rkk."id" 
            AND (
              r."module" IS NULL 
              OR rt."module" = r."module"
            ) 
          LIMIT 
            1
        )
    ) rkk 
    JOIN "f_dp_rkkbase" rkkb ON rkk."id" = rkkb."id" 
    JOIN (
      SELECT 
        brdauth."orig_shortname" "craetorshortname", 
        fdr2."created_by", 
        fdr2."created_by_type", 
        fdr2."author" "autor_res", 
        fdr2."author_type" "autor_res_type", 
        fdr."hierroot", 
        fdr."hierroot_type", 
        brdauth."id" "created_id", 
        brdauth."id_type" "created_id_type" 
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
        ) fdr 
        JOIN "f_dp_resltnbase" fdr2 ON fdr2."id" = fdr."id" 
        LEFT JOIN "person" ON fdr2."created_by" = person."id" 
        LEFT JOIN "so_personsys" sp ON person."id" = sp."platformperson" 
        LEFT JOIN "so_appointment" so_app ON sp."id" = so_app."person" 
        LEFT JOIN "so_beard" brdauth ON brdauth."id" = so_app."beard" 
        JOIN "clerk" ON clerk."clerk_id" = brdauth."id" 
      WHERE 
        fdr2."isdeleted" = 0 
        AND fdr2."created_date" :: DATE BETWEEN '-4' 3 :: DATE 
        AND '-4' 4 :: DATE
    ) creators ON creators."hierroot" = rkkb."id" 
  WHERE 
    (
      (
        '-4' 5 = '-4' 
        AND rkkb."initbranch" IS NOT NULL
      ) 
      OR rkkb."initbranch" IN (11, -4)
    ) 
    AND rkkb."isdeleted" = 0 
    AND rkk."regnumcnt" IS NOT NULL 
  GROUP BY 
    rkkb."complect", 
    creators."created_id", 
    creators."created_id_type"
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
            '-4' 6 = '-4' 
            AND rkkb."initbranch" IS NOT NULL
          ) 
          OR rkkb."initbranch" IN (11, -4)
        ) 
        AND rkkb."isdeleted" = 0 
        AND al."inprocess" <> 'Stoped' 
        AND rkk."regnumcnt" IS NOT NULL 
        AND (
          aprans."crdate" :: date BETWEEN '-4' 7 :: date 
          AND '-4' 8 :: date 
          OR delegate."created_date" :: date BETWEEN '-4' 9 :: date 
          AND '11,-4' 0 :: date
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
), 
Realauthor AS (
  SELECT 
    STRING_AGG(
      t."value" || t."resolutions", ', '
    ) "creators", 
    sum(t."resolutions") "sumresolution", 
    t."created_id" "autor", 
    t."created_id_type" "autor_type", 
    t."complect" "complect" 
  FROM 
    (
      SELECT 
        COUNT(creators."id") "resolutions", 
        rkkb."complect" "complect", 
        creators."value" "value", 
        creators."created_id" "created_id", 
        creators."created_id_type" "created_id_type" 
      FROM 
        (
          SELECT 
            f_dp_rkk.* 
          FROM 
            "f_dp_rkk" f_dp_rkk 
          WHERE 
            1 = 1 
            AND EXISTS (
              SELECT 
                1 
              FROM 
                "f_dp_rkkbase" root_type 
              WHERE 
                root_type."id" = f_dp_rkk."id" 
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
                AND rt."id" = f_dp_rkk."id" 
                AND (
                  r."module" IS NULL 
                  OR rt."module" = r."module"
                ) 
              LIMIT 
                1
            )
        ) rkk 
        JOIN "f_dp_rkkbase" rkkb ON rkk."id" = rkkb."id" 
        JOIN (
          SELECT 
            CASE WHEN res."created_id" = res."autor_res" THEN (res."craetorshortname" || ' - ') WHEN res."created_id" != res."autor_res" THEN (res."author" || ' - ') END "value", 
            res."id" "id", 
            res."id_type" "id_type", 
            res."hierroot", 
            res."hierroot_type", 
            res."created_id", 
            res."created_id_type" 
          FROM 
            (
              SELECT 
                brdauth."orig_shortname" "craetorshortname", 
                fdr."id", 
                fdr."id_type", 
                fdr2."created_by", 
                fdr2."created_by_type", 
                fdr2."author" "autor_res", 
                fdr2."author_type" "autor_res_type", 
                fdr."hierroot", 
                fdr."hierroot_type", 
                brdauth."id" "created_id", 
                brdauth."id_type" "created_id_type", 
                authorres."orig_shortname" "author" 
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
                ) fdr 
                JOIN "f_dp_resltnbase" fdr2 ON fdr2."id" = fdr."id" 
                LEFT JOIN "person" ON fdr2."created_by" = person."id" 
                LEFT JOIN "so_personsys" sp ON person."id" = sp."platformperson" 
                LEFT JOIN "so_appointment" so_app ON sp."id" = so_app."person" 
                LEFT JOIN "so_beard" brdauth ON brdauth."id" = so_app."beard" 
                LEFT JOIN "so_beard" authorres ON authorres."id" = fdr2."author" 
                JOIN "clerk" ON clerk."clerk_id" = brdauth."id" 
              WHERE 
                fdr2."isdeleted" = 0 
                AND fdr2."created_date" :: DATE BETWEEN '11,-4' 1 :: DATE 
                AND '11,-4' 2 :: DATE
            ) AS res
        ) AS creators ON creators."hierroot" = rkkb."id" 
      WHERE 
        rkkb."isdeleted" = 0 
        AND rkk."regnumcnt" IS NOT NULL 
      GROUP BY 
        rkkb."complect", 
        creators."value", 
        creators."created_id", 
        creators."created_id_type"
    ) AS t 
  GROUP BY 
    t."created_id", 
    t."complect", 
    t."created_id_type"
) 
SELECT 
  CASE WHEN sp."id" IS NOT NULL THEN concat(
    sp."lastname", ' ', sp."firstname", 
    ' ', sp."middlename"
  ) ELSE sb."orig_shortname" END "fio", 
  clerk."clerk_id" "id", 
  clerk."clerk_id_type" "id_type", 
  coalesce(docsInternal."documents", 0) "docsinternal", 
  coalesce(
    RealauthorInternal."sumresolution", 
    0
  ) "rkkinternal", 
  coalesce(rkkvisaInternal."visa", 0) "visainternal", 
  RealauthorInternal."creators" "realauthorinternal", 
  coalesce(docsInternal."documents", 0) + coalesce(
    RealauthorInternal."sumresolution", 
    0
  ) + coalesce(rkkvisaInternal."visa", 0) "suminternal", 
  coalesce(docsInput."documents", 0) "docsinput", 
  coalesce(
    RealauthorInput."sumresolution", 
    0
  ) "rkkinput", 
  RealauthorInput."creators" "realauthorinput", 
  coalesce(docsInput."documents", 0) + coalesce(
    RealauthorInput."sumresolution", 
    0
  ) "suminput", 
  coalesce(docsOutput."documents", 0) "docsoutput", 
  coalesce(
    RealauthorOutput."sumresolution", 
    0
  ) "rkkoutput", 
  coalesce(rkkvisaOutput."visa", 0) "visaoutput", 
  RealauthorOutput."creators" "realauthoroutput", 
  coalesce(docsOutput."documents", 0) + coalesce(
    RealauthorOutput."sumresolution", 
    0
  ) + coalesce(rkkvisaOutput."visa", 0) "sumoutput", 
  coalesce(docsMission."documents", 0) "docsmission", 
  coalesce(
    RealauthorMission."sumresolution", 
    0
  ) "rkkmission", 
  coalesce(rkkvisaMission."visa", 0) "visamission", 
  RealauthorMission."creators" "realauthormission", 
  coalesce(docsMission."documents", 0) + coalesce(
    RealauthorMission."sumresolution", 
    0
  ) + coalesce(rkkvisaMission."visa", 0) "summission", 
  coalesce(docsProtocol."documents", 0) "docsprotocol", 
  coalesce(
    RealauthorProtocols."sumresolution", 
    0
  ) "rkkprotocol", 
  coalesce(rkkvisaProtocol."visa", 0) "visaprotocol", 
  RealauthorProtocols."creators" "realauthorprotocols", 
  coalesce(docsProtocol."documents", 0) + coalesce(
    RealauthorProtocols."sumresolution", 
    0
  ) + coalesce(rkkvisaProtocol."visa", 0) "sumprotocol", 
  coalesce(docsContract."documents", 0) "docscontract", 
  coalesce(
    RealauthorContract."sumresolution", 
    0
  ) "rkkcontract", 
  coalesce(rkkvisaContract."visa", 0) "visacontract", 
  RealauthorContract."creators" "realauthorcontract", 
  coalesce(docsContract."documents", 0) + coalesce(
    RealauthorContract."sumresolution", 
    0
  ) + coalesce(rkkvisaContract."visa", 0) "sumcontract", 
  coalesce(docsDirect."documents", 0) "docsdirect", 
  coalesce(
    RealauthorDirect."sumresolution", 
    0
  ) "rkkdirect", 
  coalesce(rkkvisaDirect."visa", 0) "visadirect", 
  RealauthorDirect."creators" "realauthordirect", 
  coalesce(docsDirect."documents", 0) + coalesce(
    RealauthorDirect."sumresolution", 
    0
  ) + coalesce(rkkvisaDirect."visa", 0) "sumdirect", 
  coalesce(docsWorkNote."documents", 0) "docsworknote", 
  coalesce(
    RealauthorWorkNote."sumresolution", 
    0
  ) "rkkworknote", 
  coalesce(rkkvisaWorkNote."visa", 0) "visaworknote", 
  RealauthorWorkNote."creators" "realauthorworknote", 
  coalesce(docsWorkNote."documents", 0) + coalesce(
    RealauthorWorkNote."sumresolution", 
    0
  ) + coalesce(rkkvisaWorkNote."visa", 0) "sumworknote", 
  coalesce(docsAttorney."documents", 0) "docsattorney", 
  coalesce(
    RealauthorAttorney."sumresolution", 
    0
  ) "rkkattorney", 
  coalesce(rkkvisaAttorney."visa", 0) "visaattorney", 
  RealauthorAttorney."creators" "realauthorattorney", 
  coalesce(docsAttorney."documents", 0) + coalesce(
    RealauthorAttorney."sumresolution", 
    0
  ) + coalesce(rkkvisaAttorney."visa", 0) "sumattorney" 
FROM 
  "clerk" 
  JOIN "so_beard" sb ON sb."id" = clerk."clerk_id" 
  LEFT JOIN "so_beard" registr ON registr."id" = clerk."clerk_id" 
  LEFT JOIN "docs" docsInternal ON docsInternal."regregistr_res" = clerk."clerk_id" 
  AND docsInternal."complect" = 'InternalDocs' 
  LEFT JOIN "rkk" rkkInternal ON clerk."clerk_id" = rkkInternal."author" 
  AND rkkInternal."complect" = 'InternalDocs' 
  LEFT JOIN "realauthor" RealauthorInternal ON clerk."clerk_id" = RealauthorInternal."autor" 
  AND RealauthorInternal."complect" = 'InternalDocs' 
  LEFT JOIN "rkkvisa" rkkvisaInternal ON clerk."clerk_id" = rkkvisaInternal."regregistr_res" 
  AND rkkvisaInternal."complect" = 'InternalDocs' 
  LEFT JOIN "docs" docsInput ON docsInput."regregistr_res" = clerk."clerk_id" 
  AND docsInput."complect" = 'InputDocs' 
  LEFT JOIN "rkk" rkkInput ON clerk."clerk_id" = rkkInput."author" 
  AND rkkInput."complect" = 'InputDocs' 
  LEFT JOIN "realauthor" RealauthorInput ON clerk."clerk_id" = RealauthorInput."autor" 
  AND RealauthorInput."complect" = 'InputDocs' 
  LEFT JOIN "docs" docsOutput ON docsOutput."regregistr_res" = clerk."clerk_id" 
  AND docsOutput."complect" = 'OutputDocs' 
  LEFT JOIN "rkk" rkkOutput ON clerk."clerk_id" = rkkOutput."author" 
  AND rkkOutput."complect" = 'OutputDocs' 
  LEFT JOIN "realauthor" RealauthorOutput ON clerk."clerk_id" = RealauthorOutput."autor" 
  AND RealauthorOutput."complect" = 'OutputDocs' 
  LEFT JOIN "rkkvisa" rkkvisaOutput ON clerk."clerk_id" = rkkvisaOutput."regregistr_res" 
  AND rkkvisaOutput."complect" = 'OutputDocs' 
  LEFT JOIN "docs" docsMission ON docsMission."regregistr_res" = clerk."clerk_id" 
  AND docsMission."complect" = 'Missions' 
  LEFT JOIN "rkk" rkkMission ON clerk."clerk_id" = rkkMission."author" 
  AND rkkMission."complect" = 'Missions' 
  LEFT JOIN "realauthor" RealauthorMission ON clerk."clerk_id" = RealauthorMission."autor" 
  AND RealauthorMission."complect" = 'Missions' 
  LEFT JOIN "rkkvisa" rkkvisaMission ON clerk."clerk_id" = rkkvisaMission."regregistr_res" 
  AND rkkvisaMission."complect" = 'Missions' 
  LEFT JOIN "docs" docsProtocol ON docsProtocol."regregistr_res" = clerk."clerk_id" 
  AND docsProtocol."complect" = 'Protocols' 
  LEFT JOIN "rkk" rkkProtocol ON clerk."clerk_id" = rkkProtocol."author" 
  AND rkkProtocol."complect" = 'Protocols' 
  LEFT JOIN "realauthor" RealauthorProtocols ON clerk."clerk_id" = RealauthorProtocols."autor" 
  AND RealauthorProtocols."complect" = 'Protocols' 
  LEFT JOIN "rkkvisa" rkkvisaProtocol ON clerk."clerk_id" = rkkvisaProtocol."regregistr_res" 
  AND rkkvisaProtocol."complect" = 'Protocols' 
  LEFT JOIN "docs" docsContract ON docsContract."regregistr_res" = clerk."clerk_id" 
  AND docsContract."complect" = 'ContractsLite' 
  LEFT JOIN "rkk" rkkContract ON clerk."clerk_id" = rkkContract."author" 
  AND rkkContract."complect" = 'ContractsLite' 
  LEFT JOIN "realauthor" RealauthorContract ON clerk."clerk_id" = RealauthorContract."autor" 
  AND RealauthorContract."complect" = 'ContractsLite' 
  LEFT JOIN "rkkvisa" rkkvisaContract ON clerk."clerk_id" = rkkvisaContract."regregistr_res" 
  AND rkkvisaContract."complect" = 'ContractsLite' 
  LEFT JOIN "docs" docsDirect ON docsDirect."regregistr_res" = clerk."clerk_id" 
  AND docsDirect."complect" = 'Directives' 
  LEFT JOIN "rkk" rkkDirect ON clerk."clerk_id" = rkkDirect."author" 
  AND rkkDirect."complect" = 'Directives' 
  LEFT JOIN "realauthor" RealauthorDirect ON clerk."clerk_id" = RealauthorDirect."autor" 
  AND RealauthorDirect."complect" = 'Directives' 
  LEFT JOIN "rkkvisa" rkkvisaDirect ON clerk."clerk_id" = rkkvisaDirect."regregistr_res" 
  AND rkkvisaDirect."complect" = 'Directives' 
  LEFT JOIN "docs" docsAttorney ON docsAttorney."regregistr_res" = clerk."clerk_id" 
  AND docsAttorney."complect" = 'AttorneyDocs' 
  LEFT JOIN "rkk" rkkAttorney ON clerk."clerk_id" = rkkAttorney."author" 
  AND rkkAttorney."complect" = 'AttorneyDocs' 
  LEFT JOIN "realauthor" RealauthorAttorney ON clerk."clerk_id" = RealauthorAttorney."autor" 
  AND RealauthorAttorney."complect" = 'AttorneyDocs' 
  LEFT JOIN "rkkvisa" rkkvisaAttorney ON clerk."clerk_id" = rkkvisaAttorney."regregistr_res" 
  AND rkkvisaAttorney."complect" = 'AttorneyDocs' 
  LEFT JOIN "docs" docsWorkNote ON docsWorkNote."regregistr_res" = clerk."clerk_id" 
  AND docsWorkNote."complect" = 'WorkNote' 
  LEFT JOIN "rkk" rkkWorkNote ON clerk."clerk_id" = rkkWorkNote."author" 
  AND rkkWorkNote."complect" = 'WorkNote' 
  LEFT JOIN "realauthor" RealauthorWorkNote ON clerk."clerk_id" = RealauthorWorkNote."autor" 
  AND RealauthorWorkNote."complect" = 'WorkNote' 
  LEFT JOIN "rkkvisa" rkkvisaWorkNote ON clerk."clerk_id" = rkkvisaWorkNote."regregistr_res" 
  AND rkkvisaWorkNote."complect" = 'WorkNote' 
  LEFT JOIN "so_appointment" app ON app."beard" = sb."id" 
  LEFT JOIN "so_person" sp ON sp."id" = app."person" 
WHERE 
  sb."orig_shortname" <> '<<VACANCY>>' 
  AND sb."isactive" = '1' 
ORDER BY 
  registr."orig_shortname" 
LIMIT 
  5001
