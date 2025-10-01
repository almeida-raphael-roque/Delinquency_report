import awswrangler as awr
import pandas as pd
import os
import openpyxl


class ETL_relat_inadimp:

    def ETL_inadimp():
        caminho_query = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_inadimplencia\sql\faturas.sql"
        with open(caminho_query,'r') as arquivo_query:
            query = arquivo_query.read()
        df_faturas = awr.athena.read_sql_query(query, database='silver')

        boletos_pagos = df_faturas.loc[df_faturas['historico']!=1,'ponteiro'].tolist()
        df_inadimp = df_faturas[~df_faturas['ponteiro'].isin(boletos_pagos)]
        df_inadimp = df_inadimp[df_inadimp['historico']==1]
        df_inadimp = df_inadimp.drop_duplicates(subset=['ponteiro', 'conjunto', 'matricula', 'empresa'], keep='first')
        # a pedido do setor de COBRANÇAS, retirar associado: APROSSIL - ASSOCIACAO DE PROPRIETARIOS DE CAMINHOES DO SUL D
        # a pedido do setor de COBRANÇAS, retirar associado: TESTE
        df_inadimp_atual = df_inadimp[
            ~(
                (df_inadimp['empresa'].isin(['Viavante', 'Stcoop', 'Segtruck'])) &
                (df_inadimp['associado'] == "APROSSIL - ASSOCIACAO DE PROPRIETARIOS DE CAMINHOES DO SUL D")
            )
        ]
        df_inadimp_atual = df_inadimp_atual[~df_inadimp_atual['associado'].str.contains('TESTE', na=False)]

        caminho_pasta = r'C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Inadimplência'
        caminho_arquivo = os.path.join(caminho_pasta,'relatorio_inadimplencia.xlsx')
        os.makedirs(caminho_pasta,exist_ok=True)
        if os.path.exists(caminho_arquivo):
            os.remove(caminho_arquivo)
            print("Arquivo antigo removido, iniciando carregamento...")
        df_inadimp_atual.to_excel(caminho_arquivo, engine = 'openpyxl', index=False, sheet_name='inadimplentes')
        print(f"Arquivo Excel salvo com sucesso em: {caminho_arquivo}")

if __name__ == '__main__':
    ETL_relat_inadimp.ETL_inadimp()


                            
                