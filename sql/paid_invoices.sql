SELECT DISTINCT

tm.codigo_cadastro,
tm.ponteiro,
tm.numero_documento,
tm.numero_boleto,
tm.nosso_numero,
tm.sequencia_documento,
a.descricao AS aplicacao_financeira,
(tm.valor_titulo_movimento + tm.valor_acrescimo - tm.valor_desconto) AS valor_titulo,
bx.valor_baixa, --
CAST(CAST(tm.data_emissao AS TIMESTAMP) AS DATE) AS data_emissao,
CAST(bx.data_baixa AS date) AS data_baixa,--
CAST(CAST(tm.data_vencimento AS TIMESTAMP) AS DATE) AS data_vencimento, 
i.id_set AS conjunto,
i.id_registration AS matricula,
cata.fantasia AS unidade, 
'Segtruck' AS empresa,
cat.nome AS associado,
COALESCE(v.descricao,'OUTROS') AS vendedor,
ins.description AS status_conjunto,
g.descricao AS grupo

FROM silver.titulo_movimento tm
	
INNER JOIN silver.catalogo cat ON cat.pessoa = tm.pessoa
AND cat.cnpj_cpf = tm.cnpj_cpf

INNER JOIN silver.aplicacao_recurso_financeiro a ON tm.codigo_aplicacao_recurso_fin = a.codigo
AND tm.codigo_empresa = a.codigo_empresa
AND a.codigo IN (166,1)

INNER JOIN silver.grupo_aplic_rec_financeiro g ON a.codigo_grupo = g.codigo
AND g.codigo_empresa = a.codigo_empresa

LEFT JOIN silver.invoice_item ii ON tm.id_titulo_movimento = ii.id_title_moviment
LEFT JOIN silver.invoice i ON ii.parent = i.id
LEFT JOIN silver.insurance_reg_set ir ON ir.id = i.id_set
LEFT JOIN silver.insurance_registration irs ON irs.id = ir.parent
LEFT JOIN silver.insurance_status ins ON irs.id_status = ins.id
LEFT JOIN silver.insurance_reg_set_coverage irsc ON irsc.parent = ir.id

LEFT JOIN silver.vendedor v ON v.codigo = ir.id_consultant
LEFT JOIN silver.representante r ON r.codigo = i.id_unity
LEFT JOIN silver.catalogo cata ON cata.cnpj_cpf = r.cnpj_cpf

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
	
WHERE (tm.ponteiro_consolidado IS NULL OR tm.ponteiro_consolidado= 0)
AND bx.data_baixa >= date_add('day',-4,current_date)
AND tm.crc_cpg = 'R' 

---------------------------------------------------------------------------------------
UNION ALL
---------------------------------------------------------------------------------------

SELECT DISTINCT

tm.codigo_cadastro,
tm.ponteiro,
tm.numero_documento,
tm.numero_boleto,
tm.nosso_numero,
tm.sequencia_documento,
a.descricao AS aplicacao_financeira,
(tm.valor_titulo_movimento + tm.valor_acrescimo - tm.valor_desconto) AS valor_titulo,
bx.valor_baixa, --
CAST(CAST(tm.data_emissao AS TIMESTAMP) AS DATE) AS data_emissao,
CAST(bx.data_baixa AS date) AS data_baixa,--
CAST(CAST(tm.data_vencimento AS TIMESTAMP) AS DATE) AS data_vencimento, 
i.id_set AS conjunto,
i.id_registration AS matricula,
cata.fantasia AS unidade, 
'Stcoop' AS empresa,
cat.nome AS associado,
COALESCE(v.descricao,'OUTROS') AS vendedor,
ins.description AS status_conjunto,
g.descricao AS grupo

FROM stcoop.titulo_movimento tm
	
INNER JOIN stcoop.catalogo cat ON cat.pessoa = tm.pessoa
AND cat.cnpj_cpf = tm.cnpj_cpf

INNER JOIN stcoop.aplicacao_recurso_financeiro a ON tm.codigo_aplicacao_recurso_fin = a.codigo
AND tm.codigo_empresa = a.codigo_empresa
AND a.codigo IN (166,1)

INNER JOIN stcoop.grupo_aplic_rec_financeiro g ON a.codigo_grupo = g.codigo
AND g.codigo_empresa = a.codigo_empresa

LEFT JOIN stcoop.invoice_item ii ON tm.id_titulo_movimento = ii.id_title_moviment
LEFT JOIN stcoop.invoice i ON ii.parent = i.id
LEFT JOIN stcoop.insurance_reg_set ir ON ir.id = i.id_set
LEFT JOIN stcoop.insurance_registration irs ON irs.id = ir.parent
LEFT JOIN stcoop.insurance_status ins ON irs.id_status = ins.id
LEFT JOIN stcoop.insurance_reg_set_coverage irsc ON irsc.parent = ir.id

LEFT JOIN stcoop.vendedor v ON v.codigo = ir.id_consultant
LEFT JOIN stcoop.representante r ON r.codigo = i.id_unity
LEFT JOIN stcoop.catalogo cata ON cata.cnpj_cpf = r.cnpj_cpf

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
	
WHERE (tm.ponteiro_consolidado IS NULL OR tm.ponteiro_consolidado= 0)
AND bx.data_baixa >= date_add('day',-4,current_date)
AND tm.crc_cpg = 'R' 

---------------------------------------------------------------------------------------
UNION ALL
---------------------------------------------------------------------------------------

SELECT DISTINCT

tm.codigo_cadastro,
tm.ponteiro,
tm.numero_documento,
tm.numero_boleto,
tm.nosso_numero,
tm.sequencia_documento,
a.descricao AS aplicacao_financeira,
(tm.valor_titulo_movimento + tm.valor_acrescimo - tm.valor_desconto) AS valor_titulo,
bx.valor_baixa, --
CAST(CAST(tm.data_emissao AS TIMESTAMP) AS DATE) AS data_emissao,
CAST(bx.data_baixa AS date) AS data_baixa,--
CAST(CAST(tm.data_vencimento AS TIMESTAMP) AS DATE) AS data_vencimento, 
i.id_set AS conjunto,
i.id_registration AS matricula,
cata.fantasia AS unidade, 
'Viavante' AS empresa,
cat.nome AS associado,
COALESCE(v.descricao,'OUTROS') AS vendedor,
ins.description AS status_conjunto,
g.descricao AS grupo

FROM viavante.titulo_movimento tm
	
INNER JOIN viavante.catalogo cat ON cat.pessoa = tm.pessoa
AND cat.cnpj_cpf = tm.cnpj_cpf

INNER JOIN viavante.aplicacao_recurso_financeiro a ON tm.codigo_aplicacao_recurso_fin = a.codigo
AND tm.codigo_empresa = a.codigo_empresa
AND a.codigo IN (166,1)

INNER JOIN viavante.grupo_aplic_rec_financeiro g ON a.codigo_grupo = g.codigo
AND g.codigo_empresa = a.codigo_empresa

LEFT JOIN viavante.invoice_item ii ON tm.id_titulo_movimento = ii.id_title_moviment
LEFT JOIN viavante.invoice i ON ii.parent = i.id
LEFT JOIN viavante.insurance_reg_set ir ON ir.id = i.id_set
LEFT JOIN viavante.insurance_registration irs ON irs.id = ir.parent
LEFT JOIN viavante.insurance_status ins ON irs.id_status = ins.id
LEFT JOIN viavante.insurance_reg_set_coverage irsc ON irsc.parent = ir.id

LEFT JOIN viavante.vendedor v ON v.codigo = ir.id_consultant
LEFT JOIN viavante.representante r ON r.codigo = i.id_unity
LEFT JOIN viavante.catalogo cata ON cata.cnpj_cpf = r.cnpj_cpf

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
	
WHERE (tm.ponteiro_consolidado IS NULL OR tm.ponteiro_consolidado= 0)
AND bx.data_baixa >= date_add('day',-4,current_date)
AND tm.crc_cpg = 'R' 