-- unit-register-sub
SELECT

--fru.*,
-- электронное или бумажное
--fr.efile                                      as efile,
-- ведется в по
--fre.inpo                                      as inpo,
-- сопровождается в эле. виде
--fre.electronic                                as electronic,
-- формируется вне системы
--fre.outofsystem                               as outofsustem,
-- комментарий
--fr.comment                                    as comment,
--sb.orig_shortname,
--sb.orig_postname,

--fr.inchange as inchange,
--fr.hascopies as hascopies,
--fr.isdeleted as isdeleted,
--fr.spdused as spdused,
--fr.spepk as spepk,
--
--
--fre.reserved as reservnoe,


--fre.*,
--fru.*,
--fr.*,
--setting.*,
--files.*,

-- главный мужик
--fre.storage as storage,
--p.*,
--sp.*,
--sb.*,
--fre.*,
--ffr.*,
--tn.*,
--fr.*,
--fru.*,
--files.*,
--sb.orig_shortname,
--p.firstname,
--p.lastname,

-- в зависимости от статуса у нас либо indexfile либо поля-индексы в условии else
CASE
    WHEN fru.statusGeneral = 'ACTIVE' OR (fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'PROJECT')
        THEN fr.indexfile
    ELSE COALESCE(
            NULLIF(CONCAT(
                           files.indexPrefixNew,
                           COALESCE(setting.prefixSplitter_file, fr.separatorPref),
                           files.indexNumNew,
                           COALESCE(setting.suffixSplitter_file, fr.separatorSuff)
                               || NULLIF(files.indexSuffixNew, '')), ''),
            fr.indexfile)
    END                                       AS indexfile,
-- достаем титул
CASE
    WHEN fru.statusGeneral = 'ACTIVE' OR (fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'PROJECT')
        THEN fr.titleFile
    WHEN fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'FORMED'
        THEN COALESCE(NULLIF(files.titleNew, ''), fr.titleFile)
    ELSE files.titleNew
    END                                       AS titleFile,
-- кол-во секций томов?
--fre.countTomeSections,
CONCAT_WS(', ',
          CASE
              WHEN fru.statusGeneral = 'ACTIVE' OR (fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'PROJECT')
                  THEN NULLIF(COALESCE(spd.period, fr.storagePeriod), '')
              WHEN fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'FORMED'
                  THEN NULLIF(COALESCE(spd.period, NULLIF(files.storagePeriodNew, ''), fr.storagePeriod), '')
              ELSE NULLIF(COALESCE(spd.period, files.storagePeriodNew), '')
              END,
          CASE WHEN fre.ek = 1 THEN 'ЭК' END) AS storagePeriod,
CASE
    WHEN fru.statusGeneral = 'ACTIVE' OR (fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'PROJECT')
        THEN COALESCE(spd.articleNum, fr.spDescription, '')
    WHEN fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'FORMED'
        THEN COALESCE(spd.articleNum, NULLIF(files.spDescriptionNew, ''), fr.spDescription, '')
    ELSE COALESCE(spd.articleNum, files.spDescriptionNew, '')
    END                                       AS spDescription,
-- электронное или бумажное
    CASE
        WHEN fre.inpo = 1 THEN 'Ведется в эл. виде'
        ELSE ' '
        END
        || CASE
               WHEN fre.inpo = 1 AND fre.electronic = 1 THEN ' / '
               ELSE ''
        END
        || CASE
               WHEN fre.electronic = 1 THEN 'Сопровождается в эл. виде'
               ELSE ' '
        END
        || CASE
               WHEN fre.electronic = 1 AND fre.outofsystem = 1 THEN ' / '
               ELSE ''
        END
        || CASE
               WHEN fre.outofsystem = 1 THEN 'Формируется вне Системы'
               ELSE ' '
        END
        || CASE
               WHEN fre.outofsystem = 1 AND sb.orig_shortname IS NOT NULL THEN ' / '
               ELSE ''
        END AS comment,
    -- Имена участников через запятую
    STRING_AGG(DISTINCT sb.orig_shortname, ', ') AS participants
FROM FR_UnitRegister fru
         JOIN FR_UR_File_Register files ON files.owner = fru.id
         JOIN FR_File fr ON fr.id = files.file
         JOIN FR_File_Extended fre ON fre.parent = fr.id
         join fr_file_responsibles ffr on fr.id = ffr.owner and fr.id_type = ffr.owner_type
         join so_beard sb on ffr.responsible = sb.id and ffr.responsible_type = sb.id_type
         LEFT JOIN FR_FileSettings setting ON setting.organization = fru.organization and setting.isDeleted <> 0
         LEFT JOIN nunid2punid_map n2pNewStoragePeriod ON n2pNewStoragePeriod.nunid = files.shelfLifeNew
         LEFT JOIN SPD_Period spd ON spd.id_type || LPAD(spd.id || '', 12, '0') =
                                     CASE
                                         WHEN fru.statusGeneral = 'ACTIVE' OR
                                              (fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'PROJECT')
                                             THEN fr.shelfLife_type || LPAD(fr.shelfLife || '', 12, '0')
                                         WHEN fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'FORMED'
                                             THEN COALESCE(SUBSTRING(n2pNewStoragePeriod.punid, 1, 16),
                                                           fr.shelfLife_type || LPAD(fr.shelfLife || '', 12, '0'))
                                         ELSE SUBSTRING(n2pNewStoragePeriod.punid, 1, 16)
                                         END



where fr.indexfile = '100-02'
--where fr.indexfile = '100-01'
GROUP BY
    fru.statusGeneral,
    fre.countTomeSections,
    fre.ek,
    fre.inpo,
    fre.outofsystem,
    fre.electronic,
    spd.articleNum,
    spd.period,
    fr.spDescription,
    fr.statusgeneral,
    fr.separatorpref,
    fr.separatorsuff,
    fr.storageperiod,
    fr.indexfile,
    fr.titleFile,
    fr.comment,
    setting.prefixSplitter_file,
    files.indexNumNew,
    files.fileNumber,
    files.titlenew,
    files.spdescriptionnew,
    files.storageperiodnew,
    files.indexSuffixNew,
    files.indexPrefixNew,
    setting.suffixSplitter_file,
    sb.orig_shortname
ORDER BY
    files.fileNumber





