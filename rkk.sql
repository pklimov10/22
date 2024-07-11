WITH rkk AS (
  SELECT 
    COUNT(rkk."id") AS "resolutions", 
    creators."created_id" AS "author", 
    creators."created_id_type" AS "author_type", 
    rkkb."complect" AS "complect" 
  FROM 
    (
      SELECT 
        f_dp_rkk.* 
      FROM 
        "f_dp_rkk" f_dp_rkk 
      WHERE 
        EXISTS (
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
        brdauth."orig_shortname" AS "craetorshortname", 
        fdr2."created_by", 
        fdr2."created_by_type", 
        fdr2."author" AS "autor_res", 
        fdr2."author_type" AS "autor_res_type", 
        fdr."hierroot", 
        fdr."hierroot_type", 
        brdauth."id" AS "created_id", 
        brdauth."id_type" AS "created_id_type" 
      FROM 
        (
          SELECT 
            f_dp_resolution.* 
          FROM 
            "f_dp_resolution" f_dp_resolution 
          WHERE 
            EXISTS (
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
      
      UNION ALL
      
      SELECT 
        visaauthor."orig_shortname" AS "craetorshortname", 
        fdr2."created_by", 
        fdr2."created_by_type", 
        fdr2."author" AS "autor_res", 
        fdr2."author_type" AS "autor_res_type", 
        fdr."hierroot", 
        fdr."hierroot_type", 
        visaauthor."id" AS "created_id", 
        visaauthor."id_type" AS "created_id_type" 
      FROM 
        (
          SELECT 
            f_dp_resolution.* 
          FROM 
            "f_dp_resolution" f_dp_resolution 
          WHERE 
            EXISTS (
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
        LEFT JOIN "so_beard" visaauthor ON visaauthor."id" = so_app."beard" 
        JOIN "clerk" ON clerk."clerk_id" = visaauthor."id" 
      WHERE 
        fdr2."isdeleted" = 0 
    ) creators ON creators."hierroot" = rkkb."id" 
  WHERE 
    (
      (
        '-4'  = '-4' 
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
)
