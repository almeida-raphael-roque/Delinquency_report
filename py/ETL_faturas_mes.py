import awswrangler as awr
import pandas as pd
import os
import openpyxl


class ETL_relat_mes:

    def ETL_mes():
        caminho_query = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_inadimplencia\sql\faturas_mes.sql"
        with open(caminho_query,'r') as arquivo_query:
            query = arquivo_query.read()
        df_mes = awr.athena.read_sql_query(query, database='silver')

        caminho_pasta = r'C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Inadimplência'
        caminho_arquivo = os.path.join(caminho_pasta,'relatorio_faturas_mes.xlsx')
        os.makedirs(caminho_pasta,exist_ok=True)
        if os.path.exists(caminho_arquivo):
            os.remove(caminho_arquivo)
            print("Arquivo antigo removido, iniciando carregamento...")
        df_mes.to_excel(caminho_arquivo, engine = 'openpyxl', index=False, sheet_name='faturas_mes')
        print(f"Arquivo Excel salvo com sucesso em: {caminho_arquivo}")

if __name__ == '__main__':
    ETL_relat_mes.ETL_mes()


                            
                