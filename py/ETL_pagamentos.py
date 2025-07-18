import awswrangler as awr
import pandas as pd
import os
import openpyxl

class ETL_relat_pagam:

    def ETL_pagam():

        xlsx_path = r"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Inadimplência\relatorio_pagamentos.xlsx"
        df_inadimp = pd.read_excel(xlsx_path,engine = 'openpyxl')
        ponteiros_inadimp = df_inadimp['ponteiro'].tolist()


        caminho_query = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_inadimplencia\sql\faturas_baixadas.sql"

        #realizando a leitura da query
        with open(caminho_query,'r') as arquivo_query:
            query = arquivo_query.read()

        #transformando em dataframe (pandas) e executando a consulta no athena 
        df_pagamentos = awr.athena.read_sql_query(query, database='silver')

        df_pagamentos = df_pagamentos.drop_duplicates('ponteiro', keep='first')
        #df_pagamentos = df_pagamentos[df_pagamentos['data_baixa']>= pd.to_datetime('2023-01-01').date()]

        #filtrando df_pagamentos para ponteiros inadimp
        df_pagamentos = df_pagamentos[df_pagamentos['ponteiro'].isin(ponteiros_inadimp)]

        caminho_pasta = r'C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Inadimplência'
        caminho_arquivo = os.path.join(caminho_pasta,'relatorio_pagamentos.xlsx')

        #verificando a existência da pasta e removendo a versão antiga
        os.makedirs(caminho_pasta,exist_ok=True)
        if os.path.exists(caminho_arquivo):
            os.remove(caminho_arquivo)
            print("Arquivo antigo removido, iniciando carregamento...")

        #convertendo o dataframe em excel e associando ao caminho da pasta
        df_pagamentos.to_excel(caminho_arquivo, engine = 'openpyxl', index=False, sheet_name='faturas_baixadas')

        print(f"Arquivo Excel salvo com sucesso em: {caminho_arquivo}")

if __name__ == '__main__':
    ETL_relat_pagam.ETL_pagam()

                            
                