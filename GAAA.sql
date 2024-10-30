WITH approvdoc AS (
    SELECT DISTINCT 
        beard.orig_departmentname AS depparty, 
        rkkkbase.complect AS doctype, 
        rkkkbase.subject AS docname, 
        rkkkbase.id AS rkkid, 
        beard.id AS idInit, 
        apr_list.id AS aprid, 
        CASE 
            WHEN (apr_voresult.refvisareplicaid NOT IN (
                SELECT replica 
                FROM SS_Module ss_m
                JOIN SS_ModuleType ss_t ON ss_m.type = ss_t.id 
                WHERE ss_t.alias IN ('ApproveIssueFixing', 'Review')
            )) 
            THEN 'На согласование' 
        END AS approv, 
        CASE 
            WHEN rkkkbase.complect = 'InternalDocs' THEN 'Внутренние документы' 
            WHEN rkkkbase.complect = 'OutputDocs' THEN 'Исходящие документы' 
            WHEN rkkkbase.complect = 'Missions' THEN 'ОРД' 
            WHEN rkkkbase.complect = 'Protocols' THEN 'Протоколы УОБ' 
            WHEN rkkkbase.complect = 'Directives' THEN 'Директивы' 
            WHEN rkkkbase.complect = 'AttorneyDocs' THEN 'Доверенности' 
            WHEN rkkkbase.complect = 'ContractsLite' THEN 'Договоры' 
            WHEN rkkkbase.complect = 'Documents' THEN 'Документы' 
        END AS aliasdoc 
    FROM apr_voresult_reply avr 
    LEFT JOIN f_dp_rkkbase rkkkbase ON rkkkbase.id = avr.access_object_id 
    JOIN apr_voresult ON rkkkbase.id = apr_voresult.hierroot AND rkkkbase.id_type = apr_voresult.hierroot_type 
    JOIN so_beard beard ON avr.participant = beard.id AND avr.participant_type = beard.id_type 
    JOIN so_appointment ON beard.id = so_appointment.beard AND beard.id_type = so_appointment.beard_type 
    LEFT JOIN SO_AppointmentHead ON SO_AppointmentHead.id = SO_Appointment.id 
    LEFT JOIN SO_AppointmentPlain ON SO_AppointmentPlain.id = SO_Appointment.id 
    JOIN SO_Post ON SO_Post.id = COALESCE(SO_AppointmentHead.post, SO_AppointmentPlain.post) 
    JOIN so_unit ON so_post.id = so_unit.id 
    JOIN so_structureunit headOrBranchDep ON so_unit.headOrBranchDep = headOrBranchDep.id 
    LEFT JOIN so_person ON so_person.id = so_post.id 
    LEFT JOIN so_department dd ON headOrBranchDep.id = dd.id 
    LEFT JOIN so_parent_ph parph ON parph.id = dd.hierparent AND parph.id_type = dd.hierparent_type AND dd.accessredirect IS NULL 
    LEFT JOIN so_posthead posth ON posth.id = parph.owner 
    JOIN ss_moduletype mt ON mt.alias = rkkkbase.complect 
    JOIN apr_list ON apr_list.hierparent = rkkkbase.id 
    JOIN apr_apprlist ON apr_list.id = apr_apprlist.id 
    WHERE apr_list.begin::date BETWEEN '2024-08-06' AND '2024-08-06' 
        AND headOrBranchDep.beard = 73 
        AND beard.hierparent = 6692 
        AND so_post.isleader = 1 
        AND apr_voresult.refvisareplicaid NOT IN ('44257BFF004F7802', '7C1EFD4A2D3F43DB') 
        AND beard.isactive = 1
) 

SELECT 
    approvdoc.depparty, 
    approvdoc.docname, 
    approvdoc.doctype, 
    approvdoc.rkkid, 
    approvdoc.idInit, 
    COUNT(*) FILTER (WHERE approvdoc.doctype = 'InternalDocs') AS internalapprov, 
    COUNT(*) FILTER (WHERE approvdoc.doctype = 'OutputDocs') AS outputapprov, 
    COUNT(*) FILTER (WHERE approvdoc.doctype = 'Missions') AS missionsapprov, 
    COUNT(*) FILTER (WHERE approvdoc.doctype = 'Documents') AS documentsapprov, 
    COUNT(*) FILTER (WHERE approvdoc.doctype = 'Protocols') AS protocolsapprov, 
    COUNT(*) FILTER (WHERE approvdoc.doctype = 'AttorneyDocs') AS attorneyapprov, 
    COUNT(*) FILTER (WHERE approvdoc.doctype = 'Directives') AS directivesapprov, 
    COUNT(*) FILTER (WHERE approvdoc.doctype = 'ContractsLite') AS contractsapprov 
FROM 
    approvdoc 
WHERE 
    approvdoc.aliasdoc = 'Внутренние документы' 
    AND approvdoc.approv = 'На согласование' 
GROUP BY 
    approvdoc.depparty, 
    approvdoc.docname, 
    approvdoc.doctype, 
    approvdoc.rkkid, 
    approvdoc.idInit;
