/*relatorio_comissao_completo*/

--union all
-------------------------------------
select cast(irs.id as varchar) "conjunto", 
irsc.id as "coverage",
irs.parent as "matricula",
c.fantasia "unidade",
v.descricao "vendedor",
a.descricao "aplicacao_fin",
bx.data_baixa "data_baixa",
bx.valor_baixa "valor_baixa",
irs.date_activation "data_ativacao",
tm.data_vencimento "data_vencimento",
tm.referencia "referencia",
irs.id_renovated_set "conjunto_anteriob",
b.description as "beneficio",
irs.monthly_value as "valor_mensalidade",
irs.adhesion_value as "valor_adesao",
ccli.nome as "associado",
(bx.valor_baixa * (a.taxa_comissao / 100)) as "comissao"

from silver.titulo_movimento tm 
	-- inner join silver.titulo_comissao tc on tm.id_titulo_movimento = tc.id_titulo_movimento

	-- left join silver.representante r on r.pessoa = tc.pessoa_representante
	--     and r.cnpj_cpf = tc.cnpj_cpf_representante
	-- left join silver.catalogo c on c.pessoa = tc.pessoa_representante 
	--     and c.cnpj_cpf = tc.cnpj_cpf_representante--unidade

	-- inner join silver.endereco en on en.pessoa = tc.pessoa_representante
	--     and  en.cnpj_cpf = tc.cnpj_cpf_representante
	--     and en.sequencia = tc.seq_end_rep--endereço (não usa)

	-- inner join silver.cliente cl on tm.pessoa = cl.pessoa
	--     and tm.cnpj_cpf = cl.cnpj_cpf
	-- inner join silver.catalogo ccli on tm.pessoa = ccli.pessoa 
	--     and tm.cnpj_cpf = ccli.cnpj_cpf
	-- inner join silver.endereco ecli on tm.pessoa = ecli.pessoa 
	--     and tm.cnpj_cpf = ecli.cnpj_cpf
	--     and tm.seq_endereco = ecli.sequencia --cliente(usa só pra pegar o nome associado)

	left outer join silver.invoice_item ii on ii.id_title_moviment = tm.id_titulo_movimento
	left outer join silver.invoice iff on ii.parent = iff.id
	left outer join silver.insurance_reg_set irs on irs.id = iff.id_set

	left outer join silver.insurance_reg_set_coverage irsc on irsc.parent = irs.id

	-- left outer join silver.price_list_benefits plb on plb.id = irsc.id_price_list
	-- left outer join silver.type_category ty on ty.id = plb.id_type_category
	-- left outer join silver.category c on c.id = ty.id_category
	-- left outer join silver.benefits b on b.id = c.id_benefits

	-- left outer join silver.vendedor v on irs.id_consultant = v.codigo
	--inner join silver.aplicacao_recurso_financeiro a on tm.codigo_aplicacao_recurso_fin = a.codigo and a.codigo_empresa = tm.codigo_empresa
	inner join (
				select max(tb.data_lancamento) data_baixa,
				sum(tb.valor_baixa) as valor_baixa,
				tb.ponteiro
                
				from titulo_movimento tb 
                inner join silver.situacao_documento stb on stb.codigo = tb.codigo_situacao_documento 

				where tb.historico not in(1,5)
				and tb.crc_cpg = 'R'
				and stb.entra_fluxo_caixa = 'S'
				and (tb.ponteiro_consolidado is null or tb.ponteiro_consolidado = 0)
				group by tb.ponteiro
				) bx on tm.ponteiro = bx.ponteiro
				and a.taxa_comissao > 0
				and (tm.ponteiro_consolidado is null or tm.ponteiro_consolidado = 0)
				
--where a.codigo in(166,1)
    and bx.data_baixa >= date('2024-04-01')
    and bx.data_baixa <= date('2024-04-30')




