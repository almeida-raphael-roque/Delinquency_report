-- a base de títulos movimento mostra a movimentação de títulos, por isso é necessário fazer um join com a base de títulos (que não tem informações fato de movimentações financeiras)
-- na titulos movimento tem tanto boletos baixados quanto boletos emitidos
-- o historico 1 mostra boleto emitido, tanto é que não apresentam informações de baixa (valor de baixa = 0)
-- obs: existem alguns ponteiros (boletos) consolidados, o que também não é o foco aqui

select distinct

T.CODIGO_CADASTRO,
CAT.NOME as ASSOCIADO,
G.DESCRICAO as GRUPO,
ireg.id AS matricula,
IR.id AS conjunto,
A.DESCRICAO AS APLICACAO_FINANCEIRA,
cast(cast(T.DATA_EMISSAO as timestamp) as date) as DATA_EMISSAO,-- 2 casts necessários para rodar no ANSI SQL 
cast(cast(T.DATA_VENCIMENTO as timestamp) as date) as DATA_VENCIMENTO, 
(T.VALOR_TITULO + T.VALOR_ACRESCIMO - T.VALOR_DESCONTO) AS VALOR_TITULO, 
V.CODIGO, 
coalesce(V.DESCRICAO,'OUTROS') as DESCRICAO,
cata.fantasia as UNIDADE,
date_diff('day',cast(cast(T.DATA_VENCIMENTO as timestamp) as date), current_date) as DIAS_ATRASADO,  
TM.PONTEIRO,
TM.numero_boleto,
TM.nosso_numero,
ins.description AS STATUS,
IFF.ID_SET,
IFF.ID_REGISTRATION AS PARENT,
'Segtruck' as COOPERATIVA,
coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi"


from silver.TITULO T
	
	INNER JOIN silver.TITULO_MOVIMENTO TM ON TM.PONTEIRO = T.PONTEIRO
	AND TM.HISTORICO =1
	AND (TM.PONTEIRO_CONSOLIDADO IS NULL OR TM.PONTEIRO_CONSOLIDADO= 0)

	INNER JOIN silver.CATALOGO CAT ON CAT.PESSOA = T.PESSOA
	AND CAT.CNPJ_CPF = T.CNPJ_CPF

	INNER JOIN silver.APLICACAO_RECURSO_FINANCEIRO A ON T.CODIGO_APLICACAO_RECURSO_FIN = A.CODIGO
	AND T.CODIGO_EMPRESA = A.CODIGO_EMPRESA

	INNER JOIN silver.GRUPO_APLIC_REC_FINANCEIRO G ON A.CODIGO_GRUPO = G.CODIGO
	AND G.CODIGO_EMPRESA = A.CODIGO_EMPRESA

	LEFT OUTER JOIN silver.INVOICE_ITEM I ON TM.ID_TITULO_MOVIMENTO = I.ID_TITLE_MOVIMENT
	LEFT OUTER JOIN silver.INVOICE IFF ON I.PARENT = IFF.ID
	LEFT OUTER JOIN silver.INSURANCE_REG_SET IR ON IR.ID = IFF.ID_SET
	LEFT JOIN silver.insurance_registration ireg ON ireg.id = IR.parent
	LEFT JOIN silver.insurance_status ins ON IR.id_status = ins.id
	LEFT JOIN silver.insurance_reg_set_coverage irsc ON irsc.parent = IR.id
	LEFT JOIN silver.insurance_reg_set_cov_trailer irsct ON irsct.parent = irsc.id
	LEFT JOIN silver.insurance_vehicle iv ON iv.id = irsc.id_vehicle
	LEFT JOIN silver.insurance_trailer it ON it.id = irsc.id_trailer
	LEFT JOIN silver.insurance_trailer itt ON it.id = irsct.id_trailer
	
	


	LEFT OUTER JOIN silver.VENDEDOR V ON V.CODIGO = IR.ID_CONSULTANT
	LEFT JOIN silver.representante r ON r.codigo = iff.id_unity
	LEFT JOIN silver.catalogo cata ON cata.cnpj_cpf = r.cnpj_cpf
	
	



where date_diff('day',cast(cast(T.DATA_VENCIMENTO as timestamp) as date),current_date) > 0
and t.CRC_CPG = 'R' --R de recebimento e P de pagamento
and ins.id = 7


------------------------------------------------------------------------

union all

------------------------------------------------------------------------

select distinct

T.CODIGO_CADASTRO,
CAT.NOME as ASSOCIADO,
G.DESCRICAO as GRUPO,
ireg.id AS matricula,
IR.id AS conjunto,
A.DESCRICAO AS APLICACAO_FINANCEIRA,
cast(cast(T.DATA_EMISSAO as timestamp) as date) as DATA_EMISSAO,-- 2 casts necessários para rodar no ANSI SQL 
cast(cast(T.DATA_VENCIMENTO as timestamp) as date) as DATA_VENCIMENTO, 
(T.VALOR_TITULO + T.VALOR_ACRESCIMO - T.VALOR_DESCONTO) AS VALOR_TITULO, 
V.CODIGO, 
coalesce(V.DESCRICAO,'OUTROS') as DESCRICAO,
cata.fantasia as UNIDADE,
date_diff('day',cast(cast(T.DATA_VENCIMENTO as timestamp) as date), current_date) as DIAS_ATRASADO,  
TM.PONTEIRO,
TM.numero_boleto,
TM.nosso_numero,
ins.description AS STATUS,
IFF.ID_SET,
IFF.ID_REGISTRATION AS PARENT,
'Stcoop' as COOPERATIVA,
coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi"


from stcoop.TITULO T
	
	INNER JOIN stcoop.TITULO_MOVIMENTO TM ON TM.PONTEIRO = T.PONTEIRO
	AND TM.HISTORICO =1
	AND (TM.PONTEIRO_CONSOLIDADO IS NULL OR TM.PONTEIRO_CONSOLIDADO= 0)
	INNER JOIN stcoop.CATALOGO CAT ON CAT.PESSOA = T.PESSOA
	AND CAT.CNPJ_CPF = T.CNPJ_CPF
	INNER JOIN stcoop.APLICACAO_RECURSO_FINANCEIRO A ON T.CODIGO_APLICACAO_RECURSO_FIN = A.CODIGO
	AND T.CODIGO_EMPRESA = A.CODIGO_EMPRESA
	INNER JOIN stcoop.GRUPO_APLIC_REC_FINANCEIRO G ON A.CODIGO_GRUPO = G.CODIGO
	AND G.CODIGO_EMPRESA = A.CODIGO_EMPRESA
	LEFT OUTER JOIN stcoop.INVOICE_ITEM I ON TM.ID_TITULO_MOVIMENTO = I.ID_TITLE_MOVIMENT
	LEFT OUTER JOIN stcoop.INVOICE IFF ON I.PARENT = IFF.ID
	LEFT OUTER JOIN stcoop.INSURANCE_REG_SET IR ON IR.ID = IFF.ID_SET
	LEFT JOIN stcoop.insurance_registration ireg ON ireg.id = IR.parent
	LEFT OUTER JOIN stcoop.VENDEDOR V ON V.CODIGO = IR.ID_CONSULTANT
	LEFT JOIN stcoop.representante r ON r.codigo = iff.id_unity
	LEFT JOIN stcoop.catalogo cata ON cata.cnpj_cpf = r.cnpj_cpf
	LEFT JOIN stcoop.insurance_status ins ON IR.id_status = ins.id

	LEFT JOIN stcoop.insurance_reg_set_coverage irsc ON irsc.parent = IR.id
	LEFT JOIN stcoop.insurance_reg_set_cov_trailer irsct ON irsct.parent = irsc.id
	LEFT JOIN stcoop.insurance_vehicle iv ON iv.id = irsc.id_vehicle
	LEFT JOIN stcoop.insurance_trailer it ON it.id = irsc.id_trailer
	LEFT JOIN stcoop.insurance_trailer itt ON it.id = irsct.id_trailer
	


where date_diff('day',cast(cast(T.DATA_VENCIMENTO as timestamp) as date),current_date) > 0
and t.CRC_CPG = 'R'
and ins.id = 7


------------------------------------------------------------------------

union all

------------------------------------------------------------------------


select distinct

T.CODIGO_CADASTRO,
CAT.NOME as ASSOCIADO,
G.DESCRICAO as GRUPO,
ireg.id AS matricula,
IR.id AS conjunto,
A.DESCRICAO AS APLICACAO_FINANCEIRA,
cast(cast(T.DATA_EMISSAO as timestamp) as date) as DATA_EMISSAO,-- 2 casts necessários para rodar no ANSI SQL 
cast(cast(T.DATA_VENCIMENTO as timestamp) as date) as DATA_VENCIMENTO, 
(T.VALOR_TITULO + T.VALOR_ACRESCIMO - T.VALOR_DESCONTO) AS VALOR_TITULO, 
V.CODIGO, 
coalesce(V.DESCRICAO,'OUTROS') as DESCRICAO,
cata.fantasia as UNIDADE,
date_diff('day',cast(cast(T.DATA_VENCIMENTO as timestamp) as date), current_date) as DIAS_ATRASADO,  
TM.PONTEIRO,
TM.numero_boleto,
TM.nosso_numero,
ins.description AS STATUS,
IFF.ID_SET,
IFF.ID_REGISTRATION AS PARENT,
'Viavante' as COOPERATIVA,
coalesce(iv.BOARD,it.BOARD,itt.BOARD) as "placa",
coalesce(iv.chassi,it.chassi,itt.chassi) as "chassi"


from viavante.TITULO T
	
	INNER JOIN viavante.TITULO_MOVIMENTO TM ON TM.PONTEIRO = T.PONTEIRO
	AND TM.HISTORICO =1
	AND (TM.PONTEIRO_CONSOLIDADO IS NULL OR TM.PONTEIRO_CONSOLIDADO= 0)
	
	INNER JOIN viavante.CATALOGO CAT ON CAT.PESSOA = T.PESSOA
	AND CAT.CNPJ_CPF = T.CNPJ_CPF
	INNER JOIN viavante.APLICACAO_RECURSO_FINANCEIRO A ON T.CODIGO_APLICACAO_RECURSO_FIN = A.CODIGO
	AND T.CODIGO_EMPRESA = A.CODIGO_EMPRESA
	INNER JOIN viavante.GRUPO_APLIC_REC_FINANCEIRO G ON A.CODIGO_GRUPO = G.CODIGO
	AND G.CODIGO_EMPRESA = A.CODIGO_EMPRESA

	LEFT OUTER JOIN viavante.INVOICE_ITEM I ON TM.ID_TITULO_MOVIMENTO = I.ID_TITLE_MOVIMENT
	LEFT OUTER JOIN viavante.INVOICE IFF ON I.PARENT = IFF.ID

	LEFT OUTER JOIN viavante.INSURANCE_REG_SET IR ON IR.ID = IFF.ID_SET
	LEFT JOIN viavante.insurance_registration ireg ON ireg.id = IR.parent
	LEFT OUTER JOIN viavante.VENDEDOR V ON V.CODIGO = IR.ID_CONSULTANT
	LEFT JOIN viavante.representante r ON r.codigo = iff.id_unity
	LEFT JOIN viavante.catalogo cata ON cata.cnpj_cpf = r.cnpj_cpf
	LEFT JOIN viavante.insurance_status ins ON IR.id_status = ins.id
	
	LEFT JOIN viavante.insurance_reg_set_coverage irsc ON irsc.parent = IR.id
	LEFT JOIN viavante.insurance_reg_set_cov_trailer irsct ON irsct.parent = irsc.id
	LEFT JOIN viavante.insurance_vehicle iv ON iv.id = irsc.id_vehicle
	LEFT JOIN viavante.insurance_trailer it ON it.id = irsc.id_trailer
	LEFT JOIN viavante.insurance_trailer itt ON it.id = irsct.id_trailer

where date_diff('day',cast(cast(T.DATA_VENCIMENTO as timestamp) as date),current_date) > 0
and t.CRC_CPG = 'R'
and ins.id = 7

