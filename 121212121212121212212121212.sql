WITH reqid AS (
    SELECT 
        item AS id, 
        item_type AS id_type 
    FROM 
        qr_id_list 
    WHERE 
        request = 1583013 
        AND request_type = 2108 
    LIMIT 1
)
SELECT 
    rkk.id, 
    rkk.id_type, 
    module.replica || ':' || nunid2punid_map.nunid AS unid, 
    rkk.id_type AS rkkidtype, 
    module.replica, 
    COALESCE(rkk.regnumprist, '') || COALESCE(CAST(rkk.regnumcnt AS varchar), '') || COALESCE(rkk.regnumfin, '') AS regnumber, 
    CASE 
        WHEN regPlace.orig_type = 0 THEN regPlace.orig_shortname 
        WHEN regPlace.orig_type = 1 THEN regPlace.orig_shortname || ', ' || regPlace.orig_postname 
        WHEN regPlace.orig_type = 2 THEN (
            SELECT SO_StructureUnit.fullname 
            FROM so_structureunit 
            WHERE SO_StructureUnit.beard = regPlace.id
        )
    END AS registrationplace, 
    CASE 
        WHEN sf.value = 'Project' THEN to_char(timezone('0', rkkbase.created_date), 'HH24:MI') 
        ELSE to_char(timezone('0', df_sending.value), 'HH24:MI')
    END AS sendingtime, 
    CASE 
        WHEN sf.value = 'Project' THEN to_char(rkkbase.created_date, 'dd.MM.yyyy') 
        ELSE to_char(df_sending.value, 'dd.MM.yyyy')
    END AS sendingdate, 
    to_char(timezone('0', df_receiving.value), 'HH24:MI') AS receivingtime, 
    to_char(df_receiving.value, 'dd.MM.yyyy') AS receivingdate, 
    sr_sender.value AS sender, 
    sr_receiver.value AS receiver, 
    (
        SELECT SO_StructureUnit.fullname 
        FROM so_beard 
        JOIN so_structureunit ON SO_StructureUnit.beard = SO_Beard.id 
        JOIN tn_field tf ON tf.owner = rkkbase.id 
        JOIN tn_field_string tfs ON tf.access_object_id = tfs.id 
        WHERE tf.cmjfield = 'receiverDepBeard' 
        AND SO_Beard.cmjunid = concat(split_part(tfs.value, '%', 4), split_part(tfs.value, '%', 5))
    ) AS receiverdep, 
    dh_sender.value AS senderdephierarchy, 
    dh_receiver.value AS receiverdephierarchy, 
    tf_total_sent.value AS totalsent, 
    tf_total_not_received.value AS totalnotreceived, 
    tf_total_received.value AS totalreceived, 
    tf_total_received_by_fact.value AS totalreceivedbyfact 
FROM 
    f_dp_rkkbase rkkbase 
    NATURAL JOIN f_dp_rkk rkk 
    JOIN f_dp_intrkk inrrkk ON rkkbase.id = inrrkk.id 
    LEFT JOIN f_dp_rkkbase_theme theme ON theme.owner = inrrkk.id 
    LEFT JOIN so_beard regPlace ON regPlace.id = rkkbase.regcode 
    LEFT JOIN so_beard signer ON signer.id = inrrkk.signsigner 
    LEFT JOIN ss_module module ON module.id = rkkbase.module 
    LEFT JOIN ss_moduletype moduletype ON moduletype.id = module.type 
    JOIN nunid2punid_map ON left(nunid2punid_map.punid, 16) = to_char(CAST((rkk.id_type * 10 ^ 12) AS bigint) + rkk.id, 'FM0000000000000000') 
    JOIN reqid rq ON rq.id = rkk.id 
    LEFT JOIN tn_field_string sf ON sf.owner = rkkbase.id AND sf.cmjfield = 'state'
    LEFT JOIN tn_field_datetime df_sending ON df_sending.owner = rkkbase.id AND df_sending.cmjfield = 'sendingDateTime'
    LEFT JOIN tn_field_datetime df_receiving ON df_receiving.owner = rkkbase.id AND df_receiving.cmjfield = 'receivingDateTime'
    LEFT JOIN tn_field_string sr_sender ON sr_sender.owner = rkkbase.id AND sr_sender.cmjfield = 'sender'
    LEFT JOIN tn_field_string sr_receiver ON sr_receiver.owner = rkkbase.id AND sr_receiver.cmjfield = 'receiver'
    LEFT JOIN tn_field_string dh_sender ON dh_sender.owner = rkkbase.id AND dh_sender.cmjfield = 'senderDepHierarchy'
    LEFT JOIN tn_field_string dh_receiver ON dh_receiver.owner = rkkbase.id AND dh_receiver.cmjfield = 'receiverDepHierarchy'
    LEFT JOIN tn_field_decimal tf_total_sent ON tf_total_sent.owner = rkkbase.id AND tf_total_sent.cmjfield = 'totalSent'
    LEFT JOIN tn_field_decimal tf_total_not_received ON tf_total_not_received.owner = rkkbase.id AND tf_total_not_received.cmjfield = 'totalNotReceived'
    LEFT JOIN tn_field_decimal tf_total_received ON tf_total_received.owner = rkkbase.id AND tf_total_received.cmjfield = 'totalReceived'
    LEFT JOIN tn_field_decimal tf_total_received_by_fact ON tf_total_received_by_fact.owner = rkkbase.id AND tf_total_received_by_fact.cmjfield = 'totalReceivedByFact'
WHERE 
    moduletype.alias = 'DTR'