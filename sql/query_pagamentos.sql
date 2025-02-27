SELECT * FROM (
SELECT 
'Segtruck' AS cooperativa,
--cat.fantasia AS unidade,
--i.id_registration AS matricula,
--i.id_set AS conjunto,
cata.nome AS associado,
tm.codigo_cadastro,
tm.ponteiro,
tm.numero_boleto,
tm.nosso_numero,
tm.historico,
(tm.valor_titulo_movimento+tm.valor_acrescimo-tm.valor_desconto) AS valor_titulo,
CAST(CAST(tm.data_lancamento AS timestamp) AS date) AS data_pagamento, --pegar de TM para fazer a conferência
CAST(CAST(tm.data_vencimento AS timestamp) AS date) AS data_vencimento,
CAST(CAST(tm.data_emissao AS timestamp) AS date) AS data_emissao,
date_diff('day',
    CAST(CAST(tm.data_lancamento AS timestamp) AS date),
    CAST(CAST(tm.data_vencimento AS timestamp) AS date)
    ) AS dias_atraso, --lembrar que tem contas pagas, porém vencidas (com dias_atraso>0)
--v.codigo,
COALESCE(v.descricao,'OUTROS') AS vendedor,
garf.descricao AS grupo,
arf.descricao AS aplicacao_financeira

FROM silver.titulo_movimento tm

LEFT JOIN silver.aplicacao_recurso_financeiro arf ON arf.codigo = tm.codigo_aplicacao_recurso_fin --adesão/mensalidade...
AND arf.codigo_empresa = tm.codigo_empresa --tem 3 empresas na mesma database

LEFT JOIN silver.grupo_aplic_rec_financeiro garf ON garf.codigo = arf.codigo_grupo --receitas/gastos(?)
AND garf.codigo_empresa=arf.codigo_empresa

LEFT JOIN silver.invoice_item ii ON ii.id_title_moviment = tm.id_titulo_movimento --moviment mesmo 
LEFT JOIN silver.invoice i ON i.id = ii.parent

LEFT JOIN silver.insurance_reg_set irs ON irs.id = i.id_set

LEFT JOIN silver.vendedor v ON v.codigo = irs.id_consultant

LEFT JOIN silver.representante r ON r.codigo = i.id_unity
LEFT JOIN silver.catalogo cat ON cat.cnpj_cpf = r.cnpj_cpf
LEFT JOIN silver.catalogo cata ON cata.cnpj_cpf = tm.cnpj_cpf --no titulo dá pra juntar direto pra pegar o nome do associado
LEFT JOIN silver.insurance_status ins ON ins.id=irs.id_status


WHERE tm.crc_cpg = 'R'
AND (tm.ponteiro_consolidado IS NULL OR tm.ponteiro_consolidado=0)
 
--não filtrar historico por código 1 para pegar apenas os baixados posteriormente (1 é lançados)
--pegar historico 3,4 e 8 que são os pagos (visto por padrão)
--não filtrar por ativo pq uma vez que ele é pago muda de ATIVO para outro status   

----------------------------------------------------------------------------------------
UNION ALL
----------------------------------------------------------------------------------------

SELECT 
'Stcoop' AS cooperativa,
--cat.fantasia AS unidade,
--i.id_registration AS matricula,
--i.id_set AS conjunto,
cata.nome AS associado,
tm.codigo_cadastro,
tm.ponteiro,
tm.numero_boleto,
tm.nosso_numero,
tm.historico,
(tm.valor_titulo_movimento+tm.valor_acrescimo-tm.valor_desconto) AS valor_titulo,
CAST(CAST(tm.data_lancamento AS timestamp) AS date) AS data_pagamento, --pegar de TM para fazer a conferência
CAST(CAST(tm.data_vencimento AS timestamp) AS date) AS data_vencimento,
CAST(CAST(tm.data_emissao AS timestamp) AS date) AS data_emissao,
date_diff('day',
    CAST(CAST(tm.data_lancamento AS timestamp) AS date),
    CAST(CAST(tm.data_vencimento AS timestamp) AS date)
    ) AS dias_atraso, --lembrar que tem contas pagas, porém vencidas (com dias_atraso>0)
--v.codigo,
COALESCE(v.descricao,'OUTROS') AS vendedor,
garf.descricao AS grupo,
arf.descricao AS aplicacao_financeira

FROM stcoop.titulo_movimento tm

LEFT JOIN stcoop.aplicacao_recurso_financeiro arf ON arf.codigo = tm.codigo_aplicacao_recurso_fin --adesão/mensalidade...
AND arf.codigo_empresa = tm.codigo_empresa --tem 3 empresas na mesma database

LEFT JOIN stcoop.grupo_aplic_rec_financeiro garf ON garf.codigo = arf.codigo_grupo --receitas/gastos(?)
AND garf.codigo_empresa=arf.codigo_empresa

LEFT JOIN stcoop.invoice_item ii ON ii.id_title_moviment = tm.id_titulo_movimento --moviment mesmo 
LEFT JOIN stcoop.invoice i ON i.id = ii.parent

LEFT JOIN stcoop.insurance_reg_set irs ON irs.id = i.id_set

LEFT JOIN stcoop.vendedor v ON v.codigo = irs.id_consultant

LEFT JOIN stcoop.representante r ON r.codigo = i.id_unity
LEFT JOIN stcoop.catalogo cat ON cat.cnpj_cpf = r.cnpj_cpf
LEFT JOIN stcoop.catalogo cata ON cata.cnpj_cpf = tm.cnpj_cpf --no titulo dá pra juntar direto pra pegar o nome do associado
LEFT JOIN stcoop.insurance_status ins ON ins.id=irs.id_status


WHERE tm.crc_cpg = 'R'
AND (tm.ponteiro_consolidado IS NULL OR tm.ponteiro_consolidado=0)
 
--não filtrar historico por código 1 para pegar apenas os baixados posteriormente (1 é lançados)
--pegar historico 3,4 e 8 que são os pagos (visto por padrão)
--não filtrar por ativo pq uma vez que ele é pago muda de ATIVO para outro status   

----------------------------------------------------------------------------------------
UNION ALL
----------------------------------------------------------------------------------------

SELECT 
'Viavante' AS cooperativa,
--cat.fantasia AS unidade,
--i.id_registration AS matricula,
--i.id_set AS conjunto,
cata.nome AS associado,
tm.codigo_cadastro,
tm.ponteiro,
tm.numero_boleto,
tm.nosso_numero,
tm.historico,
(tm.valor_titulo_movimento+tm.valor_acrescimo-tm.valor_desconto) AS valor_titulo,
CAST(CAST(tm.data_lancamento AS timestamp) AS date) AS data_pagamento, --pegar de TM para fazer a conferência
CAST(CAST(tm.data_vencimento AS timestamp) AS date) AS data_vencimento,
CAST(CAST(tm.data_emissao AS timestamp) AS date) AS data_emissao,
date_diff('day',
    CAST(CAST(tm.data_lancamento AS timestamp) AS date),
    CAST(CAST(tm.data_vencimento AS timestamp) AS date)
    ) AS dias_atraso, --lembrar que tem contas pagas, porém vencidas (com dias_atraso>0)
--v.codigo,
COALESCE(v.descricao,'OUTROS') AS vendedor,
garf.descricao AS grupo,
arf.descricao AS aplicacao_financeira

FROM viavante.titulo_movimento tm

LEFT JOIN viavante.aplicacao_recurso_financeiro arf ON arf.codigo = tm.codigo_aplicacao_recurso_fin --adesão/mensalidade...
AND arf.codigo_empresa = tm.codigo_empresa --tem 3 empresas na mesma database

LEFT JOIN viavante.grupo_aplic_rec_financeiro garf ON garf.codigo = arf.codigo_grupo --receitas/gastos(?)
AND garf.codigo_empresa=arf.codigo_empresa

LEFT JOIN viavante.invoice_item ii ON ii.id_title_moviment = tm.id_titulo_movimento --moviment mesmo 
LEFT JOIN viavante.invoice i ON i.id = ii.parent

LEFT JOIN viavante.insurance_reg_set irs ON irs.id = i.id_set

LEFT JOIN viavante.vendedor v ON v.codigo = irs.id_consultant

LEFT JOIN viavante.representante r ON r.codigo = i.id_unity
LEFT JOIN viavante.catalogo cat ON cat.cnpj_cpf = r.cnpj_cpf
LEFT JOIN viavante.catalogo cata ON cata.cnpj_cpf = tm.cnpj_cpf --no titulo dá pra juntar direto pra pegar o nome do associado
LEFT JOIN viavante.insurance_status ins ON ins.id=irs.id_status


WHERE tm.crc_cpg = 'R'
AND (tm.ponteiro_consolidado IS NULL OR tm.ponteiro_consolidado=0)
 
--não filtrar historico por código 1 para pegar apenas os baixados posteriormente (1 é lançados)
--pegar historico 3,4 e 8 que são os pagos (visto por padrão)
--não filtrar por ativo pq uma vez que ele é pago muda de ATIVO para outro status   
)

ORDER BY data_pagamento DESC