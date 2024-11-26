SELECT
    -- Индекс файла
    STRING_AGG(
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
        END, ', ') AS indexfile,
    -- Титул
    STRING_AGG(
        CASE
            WHEN fru.statusGeneral = 'ACTIVE' OR (fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'PROJECT')
                THEN fr.titleFile
            WHEN fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'FORMED'
                THEN COALESCE(NULLIF(files.titleNew, ''), fr.titleFile)
            ELSE files.titleNew
        END, ', ') AS titleFile,
    -- Период хранения
    STRING_AGG(
        CONCAT_WS(', ',
                  CASE
                      WHEN fru.statusGeneral = 'ACTIVE' OR (fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'PROJECT')
                          THEN NULLIF(COALESCE(spd.period, fr.storagePeriod), '')
                      WHEN fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'FORMED'
                          THEN NULLIF(COALESCE(spd.period, NULLIF(files.storagePeriodNew, ''), fr.storagePeriod), '')
                      ELSE NULLIF(COALESCE(spd.period, files.storagePeriodNew), '')
                      END,
                  CASE WHEN fre.ek = 1 THEN 'ЭК' END), ', ') AS storagePeriod,
    -- Описание
    STRING_AGG(
        CASE
            WHEN fru.statusGeneral = 'ACTIVE' OR (fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'PROJECT')
                THEN COALESCE(spd.articleNum, fr.spDescription, '')
            WHEN fru.statusGeneral = 'PROJECT' AND fr.statusGeneral = 'FORMED'
                THEN COALESCE(spd.articleNum, NULLIF(files.spDescriptionNew, ''), fr.spDescription, '')
            ELSE COALESCE(spd.articleNum, files.spDescriptionNew, '')
        END, ', ') AS spDescription,
    -- Электронное или бумажное
    STRING_AGG(
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
        END, ', ') AS comment,
    -- Все имена участников
    STRING_AGG(sb.orig_shortname, ', ') AS participants
FROM FR_UnitRegister fru
         JOIN FR_UR_File_Register files ON files.owner = fru.id
         JOIN FR_File fr ON fr.id = files.file
         JOIN FR_File_Extended fre ON fre.parent = fr.id
         JOIN fr_file_responsibles ffr ON fr.id = ffr.owner AND fr.id_type = ffr.owner_type
         JOIN so_beard sb ON ffr.responsible = sb.id AND ffr.responsible_type = sb.id_type
         LEFT JOIN FR_FileSettings setting ON setting.organization = fru.organization AND setting.isDeleted <> 0
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
WHERE fr.indexfile = '100-02'
GROUP BY 1
ORDER BY 1;
