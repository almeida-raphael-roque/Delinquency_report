SELECT 
    numero_documento,
    'segtruck' as empresa,
    COUNT(DISTINCT CASE WHEN historico = 1 THEN ponteiro END) AS qtd_historico_1,
    COUNT(DISTINCT CASE WHEN historico NOT IN (1,5) THEN ponteiro END) AS qtd_historico_outros
FROM silver.titulo_movimento
GROUP BY numero_documento
HAVING 
    COUNT(DISTINCT CASE WHEN historico = 1 THEN ponteiro END) = 
    COUNT(DISTINCT CASE WHEN historico NOT IN (1,5) THEN ponteiro END)

UNION ALL

SELECT 
    numero_documento,
    'stcoop' as empresa,
    COUNT(DISTINCT CASE WHEN historico = 1 THEN ponteiro END) AS qtd_historico_1,
    COUNT(DISTINCT CASE WHEN historico NOT IN (1,5) THEN ponteiro END) AS qtd_historico_outros
FROM stcoop.titulo_movimento
GROUP BY numero_documento
HAVING 
    COUNT(DISTINCT CASE WHEN historico = 1 THEN ponteiro END) = 
    COUNT(DISTINCT CASE WHEN historico NOT IN (1,5) THEN ponteiro END)

UNION ALL

SELECT 
    numero_documento,
    'viavante' as empresa,
    COUNT(DISTINCT CASE WHEN historico = 1 THEN ponteiro END) AS qtd_historico_1,
    COUNT(DISTINCT CASE WHEN historico NOT IN (1,5) THEN ponteiro END) AS qtd_historico_outros
FROM viavante.titulo_movimento
GROUP BY numero_documento
HAVING 
    COUNT(DISTINCT CASE WHEN historico = 1 THEN ponteiro END) = 
    COUNT(DISTINCT CASE WHEN historico NOT IN (1,5) THEN ponteiro END)









