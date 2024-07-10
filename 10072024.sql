--1. Получение person_stamp_values
CREATE TEMP TABLE person_stamp_values AS
SELECT stamp
FROM person_stamp
WHERE person = 10153;
--2. Получение cur_user_groups

CREATE TEMP TABLE cur_user_groups AS
SELECT DISTINCT gg.parent_group_id
FROM group_member gm
INNER JOIN group_group gg ON gg.child_group_id = gm.usergroup
WHERE gm.person_id = 10153;

-- Получение names

CREATE TEMP TABLE names AS
SELECT parph.id AS par_id, parph.id_type AS par_id_type, 
       CASE 
         WHEN b.orig_shortname = '<<VACANCY>>' THEN 'Вакансия, ' || b.orig_postname 
         ELSE b.orig_shortname || ', ' || b.orig_postname 
       END AS name
FROM so_parent_ph parph
JOIN so_posthead posth ON posth.id = parph.owner
JOIN so_appointmenthead apphead ON apphead.post = posth.id AND apphead.accessredirect IS NULL
JOIN so_appointment app ON app.id = apphead.id
JOIN so_beard b ON b.id = app.beard
UNION ALL
SELECT parsu.id AS par_id, parsu.id_type AS par_id_type, sunit.shortname AS name
FROM so_parent_su parsu
JOIN so_structureunit sunit ON sunit.id = parsu.owner;

-- Получение mod

CREATE TEMP TABLE mod AS
SELECT nam.name AS rp_name, clpost.clerk, clpost.clerk_type, mtype.name, 
       mtype.id AS moduletype_id, mtype.id_type AS moduletype_id_type
FROM so_rsregplaceclerk_post clpost
NATURAL JOIN so_rsregplaceclerk cl
JOIN so_rsregplace rp ON cl.owner = rp.id
JOIN so_parent ON so_parent.id = rp.owner
LEFT JOIN so_parent_su ON so_parent_su.id = so_parent.id
LEFT JOIN so_parent_ph ON so_parent_ph.id = so_parent.id
JOIN so_posthead head ON COALESCE(so_parent_su.owner, so_parent_ph.owner) = head.id
JOIN so_appointmenthead sa3 ON head.id = sa3.post
JOIN so_appointment sa ON sa3.id = sa.id
JOIN so_beard sb ON sb.id = sa.beard
JOIN ss_moduletype mtype ON rp.moduletype = mtype.id
JOIN names nam ON nam.par_id = rp.owner
WHERE (...conditions...)
UNION
SELECT nam.name AS rp_name, clpost.clerk, clpost.clerk_type, mtype.name, 
       mtype.id AS moduletype_id, mtype.id_type AS moduletype_id_type
FROM so_rsregplaceclerk_post clpost
NATURAL JOIN so_rsregplaceclerk cl
JOIN so_rsregplace rp ON cl.owner = rp.id
JOIN so_parent ON so_parent.id = rp.owner
LEFT JOIN so_parent_su ON so_parent_su.id = so_parent.id
LEFT JOIN so_parent_ph ON so_parent_ph.id = so_parent.id
JOIN so_unit ON COALESCE(so_parent_su.owner, so_parent_ph.owner) = so_unit.id
LEFT JOIN so_structureunit ON so_unit.id = so_structureunit.id
JOIN so_beard sb ON sb.id = so_structureunit.beard
JOIN ss_moduletype mtype ON rp.moduletype = mtype.id
JOIN names nam ON nam.par_id = rp.owner
WHERE (...conditions...);

-- Получение clerk

CREATE TEMP TABLE clerk AS
SELECT temp.id AS clerk_id, temp.id_type AS clerk_id_type
FROM (
  SELECT b.id, b.id_type
  FROM so_postplain postpl
  NATURAL JOIN so_post
  NATURAL JOIN so_unit
  JOIN so_appointmentplain appplain ON appplain.post = post.id
  JOIN so_appointment app ON app.id = appplain.id
  JOIN so_beard b ON b.id = app.beard
  JOIN mod mType ON mtype.clerk = post.id
  JOIN so_rspost rsp ON rsp.owner = post.id AND rsp.moduletype = mtype.moduletype_id
  UNION ALL
  SELECT b.id, b.id_type
  FROM so_posthead ph
  NATURAL JOIN so_post
  NATURAL JOIN so_unit
  JOIN so_appointmenthead apphead ON apphead.post = post.id
  JOIN so_appointment app ON app.id = apphead.id
  JOIN so_beard b ON b.id = app.beard
  JOIN mod mType ON mtype.clerk = post.id
  JOIN so_rspost rsp ON rsp.owner = post.id AND rsp.moduletype = mtype.moduletype_id
) temp
WHERE '-4' = '-2' OR '-4' = '-4' OR temp.id IN (11333, -2)
GROUP BY temp.id, temp.id_type;
--6. Получение docs
CREATE TEMP TABLE docs AS
SELECT t.regregistr_res, t.regregistr_res_type, COUNT(t.id_res) AS documents, t.complect
FROM (
  SELECT rkk.id AS id_res, rkk.id_type AS id_res_type, rkk.regregistrator AS regregistr_res,
         rkk.regregistrator_type AS regregistr_res_type, rkkb.complect
  FROM (
    SELECT f_dp_rkkbase.*
    FROM f_dp_rkkbase
    WHERE 1 = 1
    AND EXISTS (
      SELECT 1
      FROM f_dp_rkkbase ptf
      WHERE ptf.id = f_dp_rkkbase.access_object_id
      AND (ptf.security_stamp IS NULL OR ptf.security_stamp IN (SELECT stamp FROM person_stamp_values))
    )
    AND EXISTS (
      SELECT 1
      FROM f_dp_rkkbase_read r
      WHERE r.group_id IN (SELECT parent_group_id FROM cur_user_groups)
      AND r.object_id = f_dp_rkkbase.access_object_id
      AND (r.module IS NULL OR f_dp_rkkbase.module = r.module)
      LIMIT 1
    )
  ) rkkb
  JOIN f_dp_rkk rkk ON rkk.id = rkkb.id
  WHERE (('11,-4' = '-4' AND rkkb.initbranch IS NOT NULL) OR rkkb.initbranch IN (11, -4))
  AND isdeleted = 0
  AND rkk.regnumcnt IS NOT NULL
  AND rkk.regdate :: DATE BETWEEN '2024-05-13 00:00:00' :: DATE AND '-4' 2 :: DATE
) AS t
GROUP BY t.regregistr_res, t.regregistr_res_type, t.complect, t.regregistr_res_type;

--7. Получение rkk

CREATE TEMP TABLE rkk AS
SELECT COUNT(rkk.id) AS resolutions, creators.created_id AS author, creators.created_id_type AS author_type, rkkb.complect
FROM (
  SELECT f_dp_rkk.*
  FROM f_dp_rkk
  WHERE 1 = 1
  AND EXISTS (
    SELECT 1
    FROM f_dp_rkkbase root_type
    WHERE root_type.id = f_dp_rkk.id
    AND (root_type.security_stamp IS NULL OR root_type.security_stamp IN (SELECT stamp FROM person_stamp_values))
  )
  AND EXISTS (
    SELECT 1
    FROM f_dp_rkkbase_read r
    INNER JOIN f_dp_rkkbase rt ON r.object_id = rt.access_object_id
    WHERE r.group_id IN (SELECT parent_group_id FROM cur_user_groups)
    AND rt.id = f_dp_rkk.id
    AND (r.module IS NULL OR rt.module = r.module)
    LIMIT 1
  )
) rkk
JOIN f_dp_rkkbase rkkb ON rkk.id = rkkb.id
JOIN (
  SELECT brdauth.orig_shortname AS craetorshortname, fdr2.created_by, fdr2.created_by_type, fdr2.author AS autor_res, fdr2.author_type AS autor_res_type, fdr.hierroot, fdr.hierroot_type, brdauth.id AS created_id, brdauth.id_type AS created_id_type
  FROM (
    SELECT f_dp_resolution.*
    FROM f_dp_resolution
    WHERE 1 = 1
    AND EXISTS (
      SELECT 1
      FROM f_dp_rkkbase ptf
      INNER JOIN f_dp_resltnbase rt ON ptf.id = rt.access_object_id
      WHERE rt.id = f_dp_resolution.id
      AND ptf.id = rt.access_object_id
      AND (ptf.security_stamp IS NULL OR ptf.security_stamp IN (SELECT stamp FROM person_stamp_values))
    )
    AND EXISTS (
      SELECT 1
      FROM f_dp_rkkbase_read r
      INNER JOIN f_dp_resltnbase rt ON r.object_id = rt.access_object_id
      WHERE r.group_id IN (SELECT parent_group_id FROM cur_user_groups)
      AND rt.id = f_dp_resolution.id
      LIMIT 1
    )
  ) fdr
  JOIN f_dp_resltnbase fdr2 ON fdr2.id = fdr.id
  LEFT JOIN person ON fdr2.created_by = person.id
  LEFT JOIN so_personsys sp ON person.id = sp.platformperson
  LEFT JOIN so_appointment so_app ON sp.id = so_app.person
  LEFT JOIN so_beard brdauth ON brdauth.id = so_app.beard
  JOIN clerk ON clerk.clerk_id = brdauth.id
  WHERE fdr2.isdeleted = 0
  AND fdr2.created_date :: DATE BETWEEN '-4' 3 :: DATE AND '-4' 4 :: DATE
) creators ON creators.hierroot = rkkb.id
WHERE (
  (
    '-4' 5 = '-4'
    AND rkkb.initbranch IS NOT NULL
  )
  OR rkkb.initbranch IN (11, -4)
)
AND rkkb.isdeleted = 0
AND rkk.regnumcnt IS NOT NULL
GROUP BY rkkb.complect, creators.created_id, creators.created_id_type;

--Получение rkkvisa
CREATE TEMP TABLE rkkvisa AS
SELECT t.regregistr_res, t.regregistr_res_type, COUNT(t.id_res) AS visa, t.complect
FROM (
  SELECT CASE WHEN visaauthor.id IS NOT NULL THEN visaauthor.id WHEN brdauth_1.id IS NOT NULL THEN brdauth_1.id END AS regregistr_res,
         CASE WHEN visaauthor.id IS NOT NULL THEN visaauthor.id_type WHEN brdauth_1.id IS NOT NULL THEN brdauth_1.id_type END AS regregistr_res_type,
         rkk.id AS id_res, rkk.id_type AS id_res_type, rkkb.complect
  FROM (
    SELECT f_dp_rkkbase.*
    FROM f_dp_rkkbase
    WHERE 1 = 1
    AND EXISTS (
      SELECT 1
      FROM f_dp_rkkbase ptf
      WHERE ptf.id = f_dp_rkkbase.access_object_id
      AND (ptf.security_stamp IS NULL OR ptf.security_stamp IN (SELECT stamp FROM person_stamp_values))
    )
    AND EXISTS (
      SELECT 1
      FROM f_dp_rkkbase_read r
      WHERE r.group_id IN (SELECT parent_group_id FROM cur_user_groups)
      AND r.object_id = f_dp_rkkbase.access_object_id
      AND (r.module IS NULL OR f_dp_rkkbase.module = r.module)
      LIMIT 1
    )
  ) rkkb
  JOIN f_dp_rkk rkk ON rkk.id = rkkb.id
  JOIN (
    SELECT apr_list.*
    FROM apr_list
    WHERE 1 = 1
    AND EXISTS (
      SELECT 1
      FROM apr_listortempl_read r
      INNER JOIN apr_listortempl rt ON r.object_id = rt.access_object_id
      WHERE r.group_id IN (SELECT parent_group_id FROM cur_user_groups)
      AND rt.id = apr_list.id
      LIMIT 1
    )
  ) al ON al.hierparent = rkk.id
  LEFT JOIN apr_apprlist_options options ON al.id = options.owner
  LEFT JOIN apr_apprlist_delegatefrom delegate ON delegate.owner = al.id
  JOIN so_beard sb2 ON options.sendtosh = sb2.id
  LEFT JOIN apr_answer aprans ON sb2.id = aprans.realvise
  AND aprans.hierparent = options.owner
  LEFT JOIN apr_appranswer apaprans ON aprans.id = apaprans.id
  LEFT JOIN so_beard visaauthor ON visaauthor.id = aprans.idauthor_answere
  AND aprans.idauthor_answere != aprans.realvise
  LEFT JOIN ss_module ON ss_module.id = aprans.module
  LEFT JOIN ss_moduletype ON ss_module.type = ss_moduletype.id
  LEFT JOIN (
    SELECT apr_apprlist_delegate.*
    FROM apr_apprlist_delegate
    WHERE 1 = 1
    AND EXISTS (
      SELECT 1
      FROM apr_listortempl_read r
      WHERE r.group_id IN (SELECT parent_group_id FROM cur_user_groups)
      AND r.object_id = apr_apprlist_delegate.access_object_id
      LIMIT 1
    )
  ) deleg ON deleg.owner = options.owner
  LEFT JOIN person ON deleg.created_by = person.id
  LEFT JOIN so_personsys sp ON person.id = sp.platformperson
  LEFT JOIN so_appointment so_app ON sp.id = so_app.person
  LEFT JOIN so_beard brdauth ON brdauth.id = so_app.beard
  JOIN clerk ON clerk.clerk_id = visaauthor.id
  OR brdauth.id = clerk.clerk_id
  WHERE (
    (
      '-4' 6 = '-4'
      AND rkkb.initbranch IS NOT NULL
    ) 
    OR rkkb.initbranch IN (11, -4)
  ) 
  AND rkkb.isdeleted = 0 
  AND al.inprocess <> 'Stopped'
) AS t
GROUP BY t.regregistr_res, t.regregistr_res_type, t.complect;

--final
SELECT 
  CASE WHEN sp.id IS NOT NULL THEN concat(sp.lastname, ' ', sp.firstname, ' ', sp.middlename)
       ELSE sb.orig_shortname 
  END AS fio,
  clerk.clerk_id AS id, 
  clerk.clerk_id_type AS id_type, 
  COALESCE(docsInternal.documents, 0) AS docsinternal, 
  COALESCE(RealauthorInternal.sumresolution, 0) AS rkkinternal, 
  COALESCE(rkkvisaInternal.visa, 0) AS visainternal, 
  RealauthorInternal.creators AS realauthorinternal, 
  COALESCE(docsInternal.documents, 0) + COALESCE(RealauthorInternal.sumresolution, 0) + COALESCE(rkkvisaInternal.visa, 0) AS suminternal, 
  COALESCE(docsInput.documents, 0) AS docsinput, 
  COALESCE(RealauthorInput.sumresolution, 0) AS rkkinput, 
  RealauthorInput.creators AS realauthorinput, 
  COALESCE(docsInput.documents, 0) + COALESCE(RealauthorInput.sumresolution, 0) AS suminput, 
  COALESCE(docsOutput.documents, 0) AS docsoutput, 
  COALESCE(RealauthorOutput.sumresolution, 0) AS rkkoutput, 
  COALESCE(rkkvisaOutput.visa, 0) AS visaoutput, 
  RealauthorOutput.creators AS realauthoroutput, 
  COALESCE(docsOutput.documents, 0) + COALESCE(RealauthorOutput.sumresolution, 0) + COALESCE(rkkvisaOutput.visa, 0) AS sumoutput, 
  COALESCE(docsMission.documents, 0) AS docsmission, 
  COALESCE(RealauthorMission.sumresolution, 0) AS rkkmission, 
  COALESCE(rkkvisaMission.visa, 0) AS visamission, 
  RealauthorMission.creators AS realauthormission, 
  COALESCE(docsMission.documents, 0) + COALESCE(RealauthorMission.sumresolution, 0) + COALESCE(rkkvisaMission.visa, 0) AS summission, 
  COALESCE(docsProtocol.documents, 0) AS docsprotocol, 
  COALESCE(RealauthorProtocols.sumresolution, 0) AS rkkprotocol, 
  COALESCE(rkkvisaProtocol.visa, 0) AS visaprotocol, 
  RealauthorProtocols.creators AS realauthorprotocols, 
  COALESCE(docsProtocol.documents, 0) + COALESCE(RealauthorProtocols.sumresolution, 0) + COALESCE(rkkvisaProtocol.visa, 0) AS sumprotocol, 
  COALESCE(docsContract.documents, 0) AS docscontract, 
  COALESCE(RealauthorContract.sumresolution, 0) AS rkkcontract, 
  COALESCE(rkkvisaContract.visa, 0) AS visacontract, 
  RealauthorContract.creators AS realauthorcontract, 
  COALESCE(docsContract.documents, 0) + COALESЦЕ(RealauthorContract.sumresolution, 0) + COALESCE(rkkvisaContract.visа, 0) AS sumcontract, 
  COALESCE(docsDirect.documents, 0) AS docsdirect, 
  COALESЦЕ(RealauthorDirect.sumresolution, 0) AS rkkdirect, 
  COALESCE(rkkvisaDirect.visa, 0) AS visadirect, 
  RealauthorDirect.creators AS realauthordirect, 
  COALESЦЕ(docsDirect.documents, 0) + COALESЦЕ(RealauthorDirect.sumresolution, 0) + COALESCE(rkkvisaDirect.visa, 0) AS sumdirect, 
  COALESCE(docsWorkNote.documents, 0) AS docsworknote, 
  COALESCE(RealauthorWorkNote.sumresolution, 0) AS rkkworknote, 
  COALESЦЕ(rkkvisaWorkNote.visa, 0) AS visaworknote, 
  RealauthorWorkNote.creators AS realauthorworknote, 
  COALESЦЕ(docsWorkNote.documents, 0) + COALESЦЕ(RealauthorWorkNote.sumresolution, 0) + COALESЦЕ(rkkvisaWorkNote.visa, 0) AS sumworknote, 
  COALESCE(docsAttorney.documents, 0) AS docsattorney, 
  COALESЦЕ(RealauthorAttorney.sumresolution, 0) AS rkkattorney, 
  COALESCE(rkkvisaAttorney.visa, 0) AS visaattorney, 
  RealauthorAttorney.creators AS realauthorattorney, 
  COALESЦЕ(docsAttorney.documents, 0) + COALESЦЕ(RealauthorAttorney.sumresolution, 0) + COALESЦЕ(rkkvisaAttorney.visa, 0) AS sumattorney
FROM 
  clerk
  JOIN so_beard sb ON sb.id = clerk.clerk_id
  LEFT JOIN so_beard registr ON registr.id = clerk.clerk_id
  LEFT JOIN docs docsInternal ON docsInternal.regregistr_res = clerk.clerk_id AND docsInternal.complect = 'InternalDocs'
  LEFT JOIN rkk rkkInternal ON clerk.clerk_id = rkkInternal.author AND rkkInternal.complect = 'InternalDocs'
  LEFT JOIN realauthor RealauthorInternal ON clerk.clerk_id = RealauthorInternal.autor AND RealauthorInternal.complect = 'InternalDocs'
  LEFT JOIN rkkvisa rkkvisaInternal ON clerk.clerk_id = rkkvisaInternal.regregistr_res AND rkkvisaInternal.complect = 'InternalDocs'
  LEFT JOIN docs docsInput ON docsInput.regregistr_res = clerk.clerk_id AND docsInput.complect = 'InputDocs'
  LEFT JOIN rkk rkkInput ON clerk.clerk_id = rkkInput.author AND rkkInput.complect = 'InputDocs'
  LEFT JOIN realauthor RealauthorInput ON clerk.clerk_id = RealauthorInput.autor AND RealauthorInput.complect = 'InputDocs'
  LEFT JOIN docs docsOutput ON docsOutput.regregistr_res = clerk.clerk_id AND docsOutput.complect = 'OutputDocs'
  LEFT JOIN rkk rkkOutput ON clerk.clerk_id = rkkOutput.author AND rkkOutput.complect = 'OutputDocs'
  LEFT JOIN realauthor RealauthorOutput ON clerk.clerk_id = RealauthorOutput.autor AND RealauthorOutput.complect = 'OutputDocs'
  LEFT JOIN rkkvisa rkkvisaOutput ON clerk.clerk_id = rkkvisaOutput.regregistr_res AND rkkvisaOutput.complect = 'OutputDocs'
  LEFT JOIN docs docsMission ON docsMission.regregistr_res = clerk.clerk_id AND docsMission.complect = 'Missions'
  LEFT JOIN rkk rkkMission ON clerk.clerk_id = rkkMission.author AND rkkMission.complect = 'Missions'
  LEFT JOIN realauthor RealauthorMission ON clerk.clerk_id = RealauthorMission.autor AND RealauthorMission.complect = 'Missions'
  LEFT JOIN rkkvisa rkkvisaMission ON clerk.clerk_id = rkkvisaMission.regregistr_res AND rkkvisaMission.complect = 'Missions'
  LEFT JOIN docs docsProtocol ON docsProtocol.regregistr_res = clerk.clerk_id AND docsProtocol.complect = 'Protocols'
  LEFT JOIN rkk rkkProtocol ON clerk.clerk_id = rkkProtocol.author AND rkkProtocol.complect = 'Protocols'
  LEFT JOIN realauthor RealauthorProtocols ON clerk.clerk_id = RealauthorProtocols.autor AND RealauthorProtocols.complect = 'Protocols'
  LEFT JOIN rkkvisa rkkvisaProtocol ON clerk.clerk_id = rkkvisaProtocol.regregistr_res AND rkkvisaProtocol.complect = 'Protocols'
  LEFT JOIN docs docsContract ON docsContract.regregistr_res = clerk.clerk_id AND docsContract.complect = 'ContractsLite'
  LEFT JOIN rkk rkkContract ON clerk.clerk_id = rkkContract.author AND rkkContract.complect = 'ContractsLite'
  LEFT JOIN realauthor RealauthorContract ON clerk.clerk_id = RealauthorContract.autor AND RealauthorContract.complect = 'ContractsLite'
  LEFT JOIN rkkvisa rkkvisaContract ON clerk.clerk_id = rkkvisaContract.regregistr_res AND rkkvisaContract.complect = 'ContractsLite'
  LEFT JOIN docs docsDirect ON docsDirect.regregistr_res = clerk.clerk_id AND docsDirect.complect = 'Directives'
  LEFT JOIN rkk rkkDirect ON clerk.clerk_id = rkkDirect.author AND rkkDirect.complect = 'Directives'
  LEFT JOIN realauthor RealauthorDirect ON clerk.clerk_id = RealauthorDirect.autor AND RealauthorDirect.complect = 'Directives'
  LEFT JOIN rkkvisa rkkvisaDirect ON clerk.clerk_id = rkkvisaDirect.regregistr_res AND rkkvisaDirect.complect = 'Directives'
  LEFT JOIN docs docsWorkNote ON docsWorkNote.regregistr_res = clerk.clerk_id AND docsWorkNote.complect = 'WorkNote'
  LEFT JOIN rkk rkkWorkNote ON clerk.clerk_id = rkkWorkNote.author AND rkkWorkNote.complect = 'WorkNote'
  LEFT JOIN realauthor RealauthorWorkNote ON clerk.clerk_id = RealauthorWorkNote.autor AND RealauthorWorkNote.complect = 'WorkNote'
  LEFT JOIN rkkvisa rkkvisaWorkNote ON clerk.clerk_id = rkkvisaWorkNote.regregistr_res AND rkkvisaWorkNote.complect = 'WorkNote'
  LEFT JOIN docs docsAttorney ON docsAttorney.regregistr_res = clerk.clerk_id AND docsAttorney.complect = 'AttorneyDocs'
  LEFT JOIN rkk rkkAttorney ON clerk.clerk_id = rkkAttorney.author AND rkkAttorney.complect = 'AttorneyDocs'
  LEFT JOIN realauthor RealauthorAttorney ON clerk.clerk_id = RealauthorAttorney.autor AND RealauthorAttorney.complect = 'AttorneyDocs'
  LEFT JOIN rkkvisa rkkvisaAttorney ON clerk.clerk_id = rkkvisaAttorney.regregistr_res AND rkkvisaAttorney.complect = 'AttorneyDocs'
  LEFT JOIN so_appointment app ON app.beard = sb.id
  LEFT JOIN so_person sp ON sp.id = app.person
WHERE 
  sb.orig_shortname <> '<<VACANCY>>' 
  AND sb.isactive = '1'
ORDER BY registr.orig_shortname 
LIMIT 5001;
