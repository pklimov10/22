WITH reqid AS 
  (SELECT item AS id, 
          item_type AS id_type 
   FROM qr_id_list 
   WHERE request = 1583013 
     AND request_type = 2108 
   LIMIT 1)
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
           WHEN 
                  (SELECT tfs.value 
                   FROM tn_field tf 
                   JOIN tn_field_string tfs ON tfs.id = tf.id 
                   WHERE tf.owner = rkkbase.id 
                     AND tf.cmjfield = 'state') = 'Project' THEN to_char(timezone('0', rkkbase.created_date), 'HH24:MI') 
           ELSE to_char(timezone('0', 
                                   (SELECT tfd.value 
                                    FROM tn_field tf 
                                    JOIN tn_field_datetime tfd ON tfd.id = tf.id 
                                    WHERE tf.owner = rkkbase.id 
                                      AND tf.cmjfield = 'sendingDateTime')), 'HH24:MI') 
       END AS sendingtime, 
       CASE 
           WHEN 
                  (SELECT tfs.value 
                   FROM tn_field tf 
                   JOIN tn_field_string tfs ON tfs.id = tf.id 
                   WHERE tf.owner = rkkbase.id 
                     AND tf.cmjfield = 'state') = 'Project' THEN to_char(rkkbase.created_date, 'dd.MM.yyyy') 
           ELSE to_char( 
                          (SELECT tfd.value 
                           FROM tn_field tf 
                           JOIN tn_field_datetime tfd ON tfd.id = tf.id 
                           WHERE tf.owner = rkkbase.id 
                             AND tf.cmjfield = 'sendingDateTime'), 'dd.MM.yyyy') 
       END AS sendingdate, 
       to_char(timezone('0', 
                          (SELECT tfd.value 
                           FROM tn_field tf 
                           JOIN tn_field_datetime tfd ON tfd.id = tf.id 
                           WHERE tf.owner = rkkbase.id 
                             AND tf.cmjfield = 'receivingDateTime')), 'HH24:MI') AS receivingtime, 
       to_char( 
                 (SELECT tfd.value 
                  FROM tn_field tf 
                  JOIN tn_field_datetime tfd ON tfd.id = tf.id 
                  WHERE tf.owner = rkkbase.id 
                    AND tf.cmjfield = 'receivingDateTime'), 'dd.MM.yyyy') AS receivingdate, 
 
  (SELECT tfs.value 
   FROM tn_field tf 
   JOIN tn_field_string tfs ON tfs.id = tf.id 
   WHERE tf.owner = rkkbase.id 
     AND tf.cmjfield = 'sender') AS sender, 
 
  (SELECT tfs.value 
   FROM tn_field tf 
   JOIN tn_field_string tfs ON tfs.id = tf.id 
   WHERE tf.owner = rkkbase.id 
     AND tf.cmjfield = 'receiver') AS receiver, 
 
  (SELECT SO_StructureUnit.fullname 
   FROM so_beard AS SO_Beard
   JOIN so_structureunit AS SO_StructureUnit ON SO_StructureUnit.beard = SO_Beard.id 
   JOIN tn_field tf ON tf.owner = rkkbase.id 
   JOIN tn_field_string tfs ON tf.access_object_id = tfs.id 
   WHERE tf.cmjfield = 'receiverDepBeard' 
     AND SO_Beard.cmjunid = concat(split_part(tfs.value, '%', 4), split_part(tfs.value, '%', 5))) AS receiverdep, 
 
  (SELECT tfs.value 
   FROM tn_field tf 
   JOIN tn_field_string tfs ON tfs.id = tf.id 
   WHERE tf.owner = rkkbase.id 
     AND tf.cmjfield = 'senderDepHierarchy') AS senderdephierarchy, 
 
  (SELECT tfs.value FROM tn_field tf 
   JOIN tn_field_string tfs ON tfs.id = tf.id 
   WHERE tf.owner = rkkbase.id 
     AND tf.cmjfield = 'receiverDepHierarchy') AS receiverdephierarchy, 
 
  (SELECT tfs.value 
   FROM tn_field tf 
   JOIN tn_field_decimal tfs ON tfs.id = tf.id 
   WHERE tf.owner = rkkbase.id 
     AND tf.cmjfield = 'totalSent') AS totalsent, 
 
  (SELECT tfs.value 
   FROM tn_field tf 
   JOIN tn_field_decimal tfs ON tfs.id = tf.id 
   WHERE tf.owner = rkkbase.id 
     AND tf.cmjfield = 'totalNotReceived') AS totalnotreceived, 
 
  (SELECT tfs.value 
   FROM tn_field tf 
   JOIN tn_field_decimal tfs ON tfs.id = tf.id 
   WHERE tf.owner = rkkbase.id 
     AND tf.cmjfield = 'totalReceived') AS totalreceived, 
 
  (SELECT tfs.value 
   FROM tn_field tf 
   JOIN tn_field_decimal tfs ON tfs.id = tf.id 
   WHERE tf.owner = rkkbase.id 
     AND tf.cmjfield = 'totalReceivedByFact') AS totalreceivedbyfact 
FROM f_dp_rkkbase AS rkkBase 
NATURAL JOIN f_dp_rkk AS rkk 
JOIN f_dp_intrkk AS inrRkk ON rkkBase.id = inrRkk.id 
LEFT JOIN f_dp_rkkbase_theme AS theme ON theme.owner = inrRkk.id 
LEFT JOIN so_beard AS regPlace ON regPlace.id = rkkBase.regcode 
LEFT JOIN so_beard AS signer ON signer.id = inrRkk.signsigner 
LEFT JOIN ss_module AS module ON module.id = rkkbase.module 
LEFT JOIN ss_moduletype AS moduletype ON moduletype.id = module.type 
JOIN nunid2punid_map ON nunid2punid_map.punid = LPAD(CAST(((rkk.id_type * power(10, 12)) + rkk.id) AS varchar), 16, '0')
WHERE moduletype.alias = 'DTR' 
  AND rkk.id = (SELECT id FROM reqid)
