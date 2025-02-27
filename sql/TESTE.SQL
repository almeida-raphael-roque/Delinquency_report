SELECT 
'Viavante' AS cooperativa,
tm.ponteiro,
irs.parent AS matricula,
irs.id AS conjunto,
irsc.id AS cobertura,
cat.fantasia AS unidade,
cata.nome AS associado,
bx.valor_baixa, 
bx.data_baixa,
tm.data_vencimento,
DATE_DIFF('day', bx.data_baixa, tm.data_vencimento) AS dias_atraso



FROM viavante.titulo_movimento tm 
LEFT JOIN viavante.invoice_item ii ON ii.id_title_moviment = tm.id_titulo_movimento
LEFT JOIN viavante.invoice i ON i.id = ii.parent
LEFT JOIN viavante.insurance_reg_set irs ON irs.id = i.id_set
LEFT JOIN viavante.insurance_reg_set_coverage irsc ON irsc.parent = irs.id--joins principais

LEFT JOIN viavante.representante r ON r.codigo = i.id_unity
LEFT JOIN viavante.catalogo cat ON cat.cnpj_cpf = r.cnpj_cpf--unidade cat

LEFT JOIN viavante.cliente cli ON cli.codigo = i.customer_id
LEFT JOIN viavante.catalogo cata ON cata.cnpj_cpf = cli.cnpj_cpf--cliente cata

INNER JOIN viavante.(
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
) bx ON bx.ponteiro = tm.ponteiro AND (tm.ponteiro_consolidado IS NULL OR tm.ponteiro_consolidado = 0)