WITH reqid AS (
	SELECT
		item id
	,	item_type id_type
	FROM
		qr_id_list
	WHERE
		request = 1583013 AND
		request_type = 2108
	LIMIT 1
)
, tn_fields AS (
	SELECT
		tf.owner
	,	tf.cmjfield
	,	tfs.value string_value
	,	tfd.value datetime_value
	,	tfdc.value decimal_value
	FROM
		tn_field tf
	LEFT JOIN
		tn_field_string tfs
			ON tfs.id = tf.id
	LEFT JOIN
		tn_field_datetime tfd
			ON tfd.id = tf.id
	LEFT JOIN
		tn_field_decimal tfdc
			ON tfdc.id = tf.id
)
SELECT
	rkk.id
,	rkk.id_type
,	concat(m.replica, ':', n.nunid) unid
,	rkk.id_type rkkidtype
,	m.replica
,	concat(
		coalesce(rkk.regnumprist, '')
	,	coalesce(rkk.regnumcnt::varchar, '')
	,	coalesce(rkk.regnumfin, '')
	) regnumber
,	CASE
		WHEN rp.orig_type = 0 THEN rp.orig_shortname
		WHEN rp.orig_type = 1 THEN concat(rp.orig_shortname, ', ', rp.orig_postname)
		WHEN rp.orig_type = 2
			THEN (
				SELECT
					su.fullname
				FROM
					so_structureunit su
				WHERE
					su.beard = rp.id
			)
	END registrationplace
,	CASE
		WHEN tf_state.string_value = 'Project'
			THEN to_char(timezone('0', rb.created_date), 'HH24:MI')
		ELSE to_char(timezone('0', tf_sending.datetime_value), 'HH24:MI')
	END sendingtime
,	CASE
		WHEN tf_state.string_value = 'Project' THEN to_char(rb.created_date, 'dd.MM.yyyy')
		ELSE to_char(tf_sending.datetime_value, 'dd.MM.yyyy')
	END sendingdate
,	to_char(timezone('0', tf_receiving.datetime_value), 'HH24:MI') receivingtime
,	to_char(tf_receiving.datetime_value, 'dd.MM.yyyy') receivingdate
,	tf_sender.string_value sender
,	tf_receiver.string_value receiver
,	(
		SELECT
			su.fullname
		FROM
			so_beard sb
		JOIN
			so_structureunit su
				ON su.beard = sb.id
		WHERE
			sb.cmjunid = concat(
				split_part(tf_receiverdep.string_value, '%', 4)
			,	split_part(tf_receiverdep.string_value, '%', 5)
			)
	) receiverdep
,	tf_senderdep.string_value senderdephierarchy
,	tf_receiverdep.string_value receiverdephierarchy
,	tf_totalsent.decimal_value totalsent
,	tf_totalnotreceived.decimal_value totalnotreceived
,	tf_totalreceived.decimal_value totalreceived
,	tf_totalreceivedbyfact.decimal_value totalreceivedbyfact
FROM
	f_dp_rkkbase rb
NATURAL JOIN
	f_dp_rkk rkk
JOIN
	f_dp_intrkk ir
		ON rb.id = ir.id
LEFT JOIN
	f_dp_rkkbase_theme theme
		ON theme.owner = ir.id
LEFT JOIN
	so_beard rp
		ON rp.id = rb.regcode
LEFT JOIN
	so_beard signer
		ON signer.id = ir.signsigner
LEFT JOIN
	ss_module m
		ON m.id = rb.module
LEFT JOIN
	ss_moduletype mt
		ON mt.id = m.type AND
		mt.alias = 'DTR'
JOIN
	nunid2punid_map n
		ON left(n.punid, 16) = to_char((rkk.id_type * power(10, 12))::bigint + rkk.id, 'FM0000000000000000')
LEFT JOIN
	tn_fields tf_state
		ON tf_state.owner = rb.id AND
		tf_state.cmjfield = 'state'
LEFT JOIN
	tn_fields tf_sending
		ON tf_sending.owner = rb.id AND
		tf_sending.cmjfield = 'sendingDateTime'
LEFT JOIN
	tn_fields tf_receiving
		ON tf_receiving.owner = rb.id AND
		tf_receiving.cmjfield = 'receivingDateTime'
LEFT JOIN
	tn_fields tf_sender
		ON tf_sender.owner = rb.id AND
		tf_sender.cmjfield = 'sender'
LEFT JOIN
	tn_fields tf_receiver
		ON tf_receiver.owner = rb.id AND
		tf_receiver.cmjfield = 'receiver'
LEFT JOIN
	tn_fields tf_receiverdep
		ON tf_receiverdep.owner = rb.id AND
		tf_receiverdep.cmjfield = 'receiverDepBeard'
LEFT JOIN
	tn_fields tf_senderdep
		ON tf_senderdep.owner = rb.id AND
		tf_senderdep.cmjfield = 'senderDepHierarchy'
LEFT JOIN
	tn_fields tf_receiverdep_hierarchy
		ON tf_receiverdep_hierarchy.owner = rb.id AND
		tf_receiverdep_hierarchy.cmjfield = 'receiverDepHierarchy'
LEFT JOIN
	tn_fields tf_totalsent
		ON tf_totalsent.owner = rb.id AND
		tf_totalsent.cmjfield = 'totalSent'
LEFT JOIN
	tn_fields tf_totalnotreceived
		ON tf_totalnotreceived.owner = rb.id AND
		tf_totalnotreceived.cmjfield = 'totalNotReceived'
LEFT JOIN
	tn_fields tf_totalreceived
		ON tf_totalreceived.owner = rb.id AND
		tf_totalreceived.cmjfield = 'totalReceived'
LEFT JOIN
	tn_fields tf_totalreceivedbyfact
		ON tf_totalreceivedbyfact.owner = rb.id AND
		tf_totalreceivedbyfact.cmjfield = 'totalReceivedByFact'
WHERE
	rkk.id = (
		SELECT
			id
		FROM
			reqid
	);
