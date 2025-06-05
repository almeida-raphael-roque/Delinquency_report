SELECT 
'Segtruck' AS cooperativa,
a.codigo AS aplicacao,
tm.ponteiro,
tm.numero_boleto,
tm.nosso_numero,
irs.parent AS matricula,
irs.id AS conjunto,
cat.fantasia AS unidade,
cata.nome AS associado,
bx.valor_baixa, 
CAST(bx.data_baixa AS date) AS data_baixa,
CAST(tm.data_vencimento AS date) AS data_vencimento,
DATE_DIFF('day', CAST(bx.data_baixa AS date), CAST(tm.data_vencimento AS date)) AS dias_atraso

FROM silver.titulo_movimento tm
LEFT JOIN silver.titulo t ON t.ponteiro = tm.ponteiro
INNER JOIN silver.titulo_comissao tc on tm.id_titulo_movimento = tc.id_titulo_movimento 
LEFT JOIN silver.invoice_item ii ON ii.id_title_moviment = tm.id_titulo_movimento
LEFT JOIN silver.invoice i ON i.id = ii.parent
LEFT JOIN silver.insurance_reg_set irs ON irs.id = i.id_set
LEFT JOIN silver.insurance_reg_set_coverage irsc ON irsc.parent = irs.id--joins principais

LEFT JOIN silver.representante r ON r.codigo = i.id_unity
LEFT JOIN silver.catalogo cat ON cat.cnpj_cpf = r.cnpj_cpf--unidade cat

LEFT JOIN silver.cliente cli ON cli.codigo = i.customer_id
LEFT JOIN silver.catalogo cata ON cata.cnpj_cpf = cli.cnpj_cpf--cliente cata
INNER JOIN silver.aplicacao_recurso_financeiro a ON tm.codigo_aplicacao_recurso_fin = a.codigo 
AND a.codigo_empresa = tm.codigo_empresa
AND a.codigo IN (166,1)

INNER JOIN (
    SELECT 
    MAX(data_lancamento) AS data_baixa,
    SUM(valor_baixa) AS valor_baixa,
    tb.ponteiro
    FROM silver.titulo_movimento tb
    INNER JOIN silver.situacao_documento stb ON stb.codigo = tb.codigo_situacao_documento
    WHERE tb.historico NOT IN (1,5)
    AND (tb.ponteiro_consolidado IS NULL OR tb.ponteiro_consolidado = 0 )
    AND stb.entra_fluxo_caixa ='S'
    AND tb.crc_cpg = 'R'
    GROUP BY tb.ponteiro 
) bx ON bx.ponteiro = tm.ponteiro and a.taxa_comissao > 0 AND (tm.ponteiro_consolidado IS NULL OR tm.ponteiro_consolidado = 0)


---------------------------------------------------------------------------------------
UNION ALL
---------------------------------------------------------------------------------------
 

SELECT 
'Stcoop' AS cooperativa,
a.codigo AS aplicacao,
tm.ponteiro,
tm.numero_boleto,
tm.nosso_numero,
irs.parent AS matricula,
irs.id AS conjunto,
cat.fantasia AS unidade,
cata.nome AS associado,
bx.valor_baixa, 
CAST(bx.data_baixa AS date) AS data_baixa,
CAST(tm.data_vencimento AS date) AS data_vencimento,
DATE_DIFF('day', CAST(bx.data_baixa AS date), CAST(tm.data_vencimento AS date)) AS dias_atraso

FROM stcoop.titulo_movimento tm
LEFT JOIN stcoop.titulo t ON t.ponteiro = tm.ponteiro
INNER JOIN stcoop.titulo_comissao tc on tm.id_titulo_movimento = tc.id_titulo_movimento 
LEFT JOIN stcoop.invoice_item ii ON ii.id_title_moviment = tm.id_titulo_movimento
LEFT JOIN stcoop.invoice i ON i.id = ii.parent
LEFT JOIN stcoop.insurance_reg_set irs ON irs.id = i.id_set
LEFT JOIN stcoop.insurance_reg_set_coverage irsc ON irsc.parent = irs.id--joins principais

LEFT JOIN stcoop.representante r ON r.codigo = i.id_unity
LEFT JOIN stcoop.catalogo cat ON cat.cnpj_cpf = r.cnpj_cpf--unidade cat

LEFT JOIN stcoop.cliente cli ON cli.codigo = i.customer_id
LEFT JOIN stcoop.catalogo cata ON cata.cnpj_cpf = cli.cnpj_cpf--cliente cata
INNER JOIN stcoop.aplicacao_recurso_financeiro a ON tm.codigo_aplicacao_recurso_fin = a.codigo 
AND a.codigo_empresa = tm.codigo_empresa
AND a.codigo IN (166,1)

INNER JOIN (
    SELECT 
    MAX(data_lancamento) AS data_baixa,
    SUM(valor_baixa) AS valor_baixa,
    tb.ponteiro
    FROM stcoop.titulo_movimento tb
    INNER JOIN stcoop.situacao_documento stb ON stb.codigo = tb.codigo_situacao_documento
    WHERE tb.historico NOT IN (1,5)
    AND (tb.ponteiro_consolidado IS NULL OR tb.ponteiro_consolidado = 0 )
    AND stb.entra_fluxo_caixa ='S'
    AND tb.crc_cpg = 'R'
    GROUP BY tb.ponteiro 
) bx ON bx.ponteiro = tm.ponteiro and a.taxa_comissao > 0 AND (tm.ponteiro_consolidado IS NULL OR tm.ponteiro_consolidado = 0)


---------------------------------------------------------------------------------------
UNION ALL
---------------------------------------------------------------------------------------

SELECT 
'Viavante' AS cooperativa,
a.codigo AS aplicacao,
tm.ponteiro,
tm.numero_boleto,
tm.nosso_numero,
irs.parent AS matricula,
irs.id AS conjunto,
cat.fantasia AS unidade,
cata.nome AS associado,
bx.valor_baixa, 
CAST(bx.data_baixa AS date) AS data_baixa,
CAST(tm.data_vencimento AS date) AS data_vencimento,
DATE_DIFF('day', CAST(bx.data_baixa AS date), CAST(tm.data_vencimento AS date)) AS dias_atraso

FROM viavante.titulo_movimento tm
LEFT JOIN viavante.titulo t ON t.ponteiro = tm.ponteiro
INNER JOIN viavante.titulo_comissao tc on tm.id_titulo_movimento = tc.id_titulo_movimento 
LEFT JOIN viavante.invoice_item ii ON ii.id_title_moviment = tm.id_titulo_movimento
LEFT JOIN viavante.invoice i ON i.id = ii.parent
LEFT JOIN viavante.insurance_reg_set irs ON irs.id = i.id_set
LEFT JOIN viavante.insurance_reg_set_coverage irsc ON irsc.parent = irs.id--joins principais

LEFT JOIN viavante.representante r ON r.codigo = i.id_unity
LEFT JOIN viavante.catalogo cat ON cat.cnpj_cpf = r.cnpj_cpf--unidade cat

LEFT JOIN viavante.cliente cli ON cli.codigo = i.customer_id
LEFT JOIN viavante.catalogo cata ON cata.cnpj_cpf = cli.cnpj_cpf--cliente cata
INNER JOIN viavante.aplicacao_recurso_financeiro a ON tm.codigo_aplicacao_recurso_fin = a.codigo 
AND a.codigo_empresa = tm.codigo_empresa
AND a.codigo IN (166,1)

INNER JOIN (
    SELECT 
    MAX(data_lancamento) AS data_baixa,
    SUM(valor_baixa) AS valor_baixa,
    tb.ponteiro
    FROM viavante.titulo_movimento tb
    INNER JOIN viavante.situacao_documento stb ON stb.codigo = tb.codigo_situacao_documento
    WHERE tb.historico NOT IN (1,5)
    AND (tb.ponteiro_consolidado IS NULL OR tb.ponteiro_consolidado = 0 )
    AND stb.entra_fluxo_caixa ='S'
    AND tb.crc_cpg = 'R'
    GROUP BY tb.ponteiro 
) bx ON bx.ponteiro = tm.ponteiro and a.taxa_comissao > 0 AND (tm.ponteiro_consolidado IS NULL OR tm.ponteiro_consolidado = 0)



