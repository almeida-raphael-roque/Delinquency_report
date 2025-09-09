SELECT DISTINCT 
CONCAT(CAST(ir.id AS VARCHAR), CAST(irs.id AS VARCHAR), '1') AS id,
ir.id AS matricula,
irs.id AS conjunto,
'Segtruck' AS cooperativa,
CAST(irs.date_activation AS DATE) AS "data_ativacao"

FROM silver.insurance_registration ir 
LEFT JOIN silver.insurance_reg_set irs ON irs.parent = ir.id
LEFT JOIN silver.insurance_status ins ON ins.id = irs.id_status

WHERE ins.id IN (7,11)

UNION ALL

SELECT DISTINCT 
CONCAT(CAST(ir.id AS VARCHAR), CAST(irs.id AS VARCHAR), '2') AS id,
ir.id AS matricula,
irs.id AS conjunto,
'Stcoop' AS cooperativa,
CAST(irs.date_activation AS DATE) AS "data_ativacao"

FROM stcoop.insurance_registration ir 
LEFT JOIN stcoop.insurance_reg_set irs ON irs.parent = ir.id
LEFT JOIN stcoop.insurance_status ins ON ins.id = irs.id_status

WHERE ins.id IN (7,11)

UNION ALL

SELECT DISTINCT 
CONCAT(CAST(ir.id AS VARCHAR), CAST(irs.id AS VARCHAR), '3') AS id,
ir.id AS matricula,
irs.id AS conjunto,
'Viavante' AS cooperativa,
CAST(irs.date_activation AS DATE) AS "data_ativacao"

FROM viavante.insurance_registration ir 
LEFT JOIN viavante.insurance_reg_set irs ON irs.parent = ir.id
LEFT JOIN viavante.insurance_status ins ON ins.id = irs.id_status

WHERE ins.id IN (7,11)

UNION ALL

SELECT DISTINCT 
CONCAT(CAST(ir.id AS VARCHAR), CAST(irs.id AS VARCHAR), '3') AS id,
ir.id AS matricula,
irs.id AS conjunto,
'Tag' AS cooperativa,
CAST(irs.date_activation AS DATE) AS "data_ativacao"

FROM tag.insurance_registration ir 
LEFT JOIN tag.insurance_reg_set irs ON irs.parent = ir.id
LEFT JOIN tag.insurance_status ins ON ins.id = irs.id_status

WHERE ins.id IN (7,11)
AND CAST(irs.date_activation AS DATE) >= DATE('2025-08-01')