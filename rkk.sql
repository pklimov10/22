rkkbase.id = rkk.id


EXPLAIN ANALYZE
SELECT
	*
FROM
	(
		SELECT
			id
		,	id_type
		,	created_date
		,	updated_date
		,	module
		,	module_type
		,	self_1
		,	self_2
		,	self_3
		,	CASE
				WHEN isdeleted = 1 THEN 'Удален'
				WHEN state = 'Project' OR state = '' OR state IS NULL THEN 'Проект'
				WHEN state = 'Transmitted' THEN 'Передано'
				WHEN state = 'Received' THEN 'Получено'
				WHEN state = 'ReceivedPartially' THEN 'Получено частично'
				ELSE ''
			END state
		,	regnumberdtr
		,	rnumber number
		,	CASE
				WHEN state = 'Project' THEN date(created_date)
				WHEN sendingdate IS NULL AND state <> 'Project' AND NOT regdate IS NULL THEN date(regdate)
				WHEN NOT sendingdate IS NULL THEN date(sendingdate)
				ELSE NULL
			END sendingdate
		,	senderdep
		,	sender
		,	CASE WHEN NOT receivingdate IS NULL THEN date(receivingdate) ELSE NULL END receivingdate
		,	receiverdep
		,	receiver
		FROM
			(
				SELECT
					rkkbase.id
				,	rkkbase.id_type
				,	rkkbase.created_date
				,	rkkbase.updated_date
				,	rkkbase.module module
				,	rkkbase.module_type module_type
				,	'<id>' self_1
				,	':' self_2
				,	'</>' self_3
				,	rkkbase.isdeleted isdeleted
				,	(
						SELECT
							tfs.value v
						FROM
							tn_field_string tfs
						JOIN
							tn_field
								ON tn_field.id = tfs.id
						WHERE
							tn_field.owner = rkkbase.id AND
							tn_field.owner_type = rkkbase.id_type AND
							tn_field.cmjfield = 'state'
					) state
				,	'<regNumberPrefix>' || coalesce(rkk.regnumprist, '') || '</>' || '<regNumberCounter#Number>' || coalesce(rkk.regnumcnt::varchar, '') || '</>' || '<regNumberSuffix>' || coalesce(rkk.regnumfin, '') || '</>' regnumberdtr
				,	(
						SELECT
							coalesce(
								string_agg('<id>' || link.docreplid || ':' || link.docunid || '</>', ';')
							,	'<id></>'
							)
						FROM
							f_dp_rkkworegandctrl_ulnk link
						WHERE
							link.owner = rkk.id
					) linkeddoc
				,	rkk.regnumcnt rnumber
				,	rkk.regdate regdate
				,	(
						SELECT
							tfdt.value v
						FROM
							tn_field_datetime tfdt
						JOIN
							tn_field
								ON tn_field.id = tfdt.id
						WHERE
							tn_field.owner = rkkbase.id AND
							tn_field.owner_type = rkkbase.id_type AND
							tn_field.cmjfield = 'sendingDateTime'
					) sendingdate
				,	coalesce(
						(
							SELECT
								CASE
									WHEN regplace.orig_shortname IS NULL THEN 'Не указано'
									ELSE regplace.orig_shortname
								END
							FROM
								so_beard regplace
							WHERE
								regplace.id = rkkbase.regcode
						)
					,	CASE WHEN rkkbase.regcode IS NULL THEN 'Не указано' END
					) regplace
				,	(
						SELECT
							tfs.value v
						FROM
							tn_field_string tfs
						JOIN
							tn_field
								ON tn_field.id = tfs.id
						WHERE
							tn_field.owner = rkkbase.id AND
							tn_field.owner_type = rkkbase.id_type AND
							tn_field.cmjfield = 'sender'
					) sender
				,	(
						SELECT
							tfs.value v
						FROM
							tn_field_string tfs
						JOIN
							tn_field
								ON tn_field.id = tfs.id
						WHERE
							tn_field.owner = rkkbase.id AND
							tn_field.owner_type = rkkbase.id_type AND
							tn_field.cmjfield = 'senderDepHierarchy'
					) senderdep
				,	(
						SELECT
							tfdt.value v
						FROM
							tn_field_datetime tfdt
						JOIN
							tn_field
								ON tn_field.id = tfdt.id
						WHERE
							tn_field.owner = rkkbase.id AND
							tn_field.owner_type = rkkbase.id_type AND
							tn_field.cmjfield = 'receivingDateTime'
					) receivingdate
				,	(
						SELECT
							tfs.value v
						FROM
							tn_field_string tfs
						JOIN
							tn_field
								ON tn_field.id = tfs.id
						WHERE
							tn_field.owner = rkkbase.id AND
							tn_field.owner_type = rkkbase.id_type AND
							tn_field.cmjfield = 'receiverDepHierarchy'
					) receiverdep
				,	(
						SELECT
							tfs.value v
						FROM
							tn_field_string tfs
						JOIN
							tn_field
								ON tn_field.id = tfs.id
						WHERE
							tn_field.owner = rkkbase.id AND
							tn_field.owner_type = rkkbase.id_type AND
							tn_field.cmjfield = 'receiver'
					) receiver
				FROM
					f_dp_rkkbase rkkbase
				JOIN
					f_dp_intrkk internalrkk
						ON rkkbase.id = internalrkk.id
				JOIN
					f_dp_rkk rkk
						ON rkk.id = rkkbase.id
				WHERE
					rkkbase.isdeleted <> 1
			) s
	) r
WHERE
	1 = 1 AND
	(
		state = 'Передано' OR
		state = 'Получено' OR
		state = 'Получено частично'
	) AND
	module = 68 AND
	module_type = 1015
ORDER BY
	number DESC
,	id DESC
LIMIT 101;
