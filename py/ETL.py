
import awswrangler as awr
import pandas as pd
import os
import openpyxl

class ETL_relat_inadimp:

    def ETL_inadimp():

        caminho_query = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_inadimplencia\sql\query_ANSI.sql"

        #realizando a leitura da query
        with open(caminho_query,'r') as arquivo_query:
            query = arquivo_query.read()

        #transformando em dataframe (pandas) e executando a consulta no athena 
        df_inadimplentes = awr.athena.read_sql_query(query, database='silver')


        caminho_pasta = r'C:\Users\raphael.almeida\Grupo Unus\analise de dados - Arquivos em excel'
        caminho_arquivo = os.path.join(caminho_pasta,'relatorio_inadimplencia.xlsx')

        #verificando a existência da pasta e removendo a versão antiga
        os.makedirs(caminho_pasta,exist_ok=True)
        if os.path.exists(caminho_arquivo):
            os.remove(caminho_arquivo)
            print("Arquivo antigo removido, iniciando carregamento...")

        #convertendo o dataframe em excel e associando ao caminho da pasta
        with pd.ExcelWriter(caminho_arquivo, engine='openpyxl') as writer:
            df_inadimplentes.to_excel(writer, index=False, sheet_name='Inadimplentes')

        print(f"Arquivo Excel salvo com sucesso em: {caminho_arquivo}")

if __name__ == '__main__':
    ETL_relat_inadimp.ETL_inadimp()



                    