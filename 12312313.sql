WITH reqid AS 
  (SELECT item AS id, 
          item_type AS id_type 
   FROM qr_id_list 
   WHERE request = 1583013 
     AND request_type = 2108 
   LIMIT 1),
tn_field_values AS (
  SELECT tf.owner,
         tf.cmjfield,
         tfs.value AS string_value,
         tfd.value AS datetime_value,
         tfdc.value AS decimal_value
  FROM tn_field tf
  LEFT JOIN tn_field_string tfs ON tfs.id = tf.id
  LEFT JOIN tn_field_datetime tfd ON tfd.id = tf.id
  LEFT JOIN tn_field_decimal tfdc ON tfdc.id = tf.id
  WHERE tf.owner = (SELECT id FROM reqid)
)
SELECT rkk.id, 
       rkk.id_type, 
       concat(module.replica, ':', nunid2punid_map.nunid) AS unid, 
       rkk.id_type AS rkkidtype, 
       module.replica, 
       concat(COALESCE(rkk.regnumprist, ''), COALESCE(CAST(rkk.regnumcnt AS varchar), ''), COALESCE(rkk.regnumfin, '')) AS regnumber, 
       CASE 
           WHEN regPlace.orig_type = 0 THEN regPlace.orig_shortname 
           WHEN regPlace.orig_type = 1 THEN concat(regPlace.orig_shortname, ', ', regPlace.orig_postname)
           WHEN regPlace.orig_type = 2 THEN 
                  (SELECT SO_StructureUnit.fullname 
                   FROM so_structureunit AS SO_StructureUnit
                   WHERE SO_StructureUnit.beard = regPlace.id) 
       END AS registrationplace, 
       CASE 
           WHEN tf_state.string_value = 'Project' THEN to_char(timezone('0', rkkbase.created_date), 'HH24:MI') 
           ELSE to_char(timezone('0', tf_sending.datetime_value), 'HH24:MI') 
       END AS sendingtime, 
       CASE 
           WHEN tf_state.string_value = 'Project' THEN to_char(rkkbase.created_date, 'dd.MM.yyyy') 
           ELSE to_char(tf_sending.datetime_value, 'dd.MM.yyyy') 
       END AS sendingdate, 
       to_char(timezone('0', tf_receiving.datetime_value), 'HH24:MI') AS receivingtime, 
       to_char(tf_receiving.datetime_value, 'dd.MM.yyyy') AS receivingdate, 
       tf_sender.string_value AS sender, 
       tf_receiver.string_value AS receiver, 
       (SELECT SO_StructureUnit.fullname 
        FROM so_beard AS SO_Beard
        JOIN so_structureunit AS SO_StructureUnit ON SO_StructureUnit.beard = SO_Beard.id 
        WHERE SO_Beard.cmjunid = concat(split_part(tf_receiverdep.string_value, '%', 4), split_part(tf_receiverdep.string_value, '%', 5))) AS receiverdep, 
       tf_senderdep.string_value AS senderdephierarchy, 
       tf_receiverdep_hierarchy.string_value AS receiverdephierarchy, 
       tf_totalsent.decimal_value AS totalsent, 
       tf_totalnotreceived.decimal_value AS totalnotreceived, 
       tf_totalreceived.decimal_value AS totalreceived, 
       tf_totalreceivedbyfact.decimal_value AS totalreceivedbyfact 
FROM f_dp_rkkbase AS rkkBase 
NATURAL JOIN f_dp_rkk AS rkk 
JOIN f_dp_intrkk AS inrRkk ON rkkBase.id = inrRkk.id 
LEFT JOIN f_dp_rkkbase_theme AS theme ON theme.owner = inrRkk.id 
LEFT JOIN so_beard AS regPlace ON regPlace.id = rkkBase.regcode 
LEFT JOIN so_beard AS signer ON signer.id = inrRkk.signsigner 
LEFT JOIN ss_module AS module ON module.id = rkkbase.module 
LEFT JOIN ss_moduletype AS moduletype ON moduletype.id = module.type 
JOIN nunid2punid_map ON nunid2punid_map.punid = CAST(((rkk.id_type * power(10, 12)) + rkk.id) AS varchar(16))
LEFT JOIN tn_field_values tf_state ON tf_state.cmjfield = 'state'
LEFT JOIN tn_field_values tf_sending ON tf_sending.cmjfield = 'sendingDateTime'
LEFT JOIN tn_field_values tf_receiving ON tf_receiving.cmjfield = 'receivingDateTime'
LEFT JOIN tn_field_values tf_sender ON tf_sender.cmjfield = 'sender'
LEFT JOIN tn_field_values tf_receiver ON tf_receiver.cmjfield = 'receiver'
LEFT JOIN tn_field_values tf_receiverdep ON tf_receiverdep.cmjfield = 'receiverDepBeard'
LEFT JOIN tn_field_values tf_senderdep ON tf_senderdep.cmjfield = 'senderDepHierarchy'
LEFT JOIN tn_field_values tf_receiverdep_hierarchy ON tf_receiverdep_hierarchy.cmjfield = 'receiverDepHierarchy'
LEFT JOIN tn_field_values tf_totalsent ON tf_totalsent.cmjfield = 'totalSent'
LEFT JOIN tn_field_values tf_totalnotreceived ON tf_totalnotreceived.cmjfield = 'totalNotReceived'
LEFT JOIN tn_field_values tf_totalreceived ON tf_totalreceived.cmjfield = 'totalReceived'
LEFT JOIN tn_field_values tf_totalreceivedbyfact ON tf_totalreceivedbyfact.cmjfield = 'totalReceivedByFact'
WHERE moduletype.alias = 'DTR' 
  AND rkk.id = (SELECT id FROM reqid)
