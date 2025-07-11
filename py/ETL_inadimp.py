import awswrangler as awr
import pandas as pd
import os
import openpyxl


class ETL_relat_inadimp:

    def ETL_inadimp():
        
        caminho_query = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_inadimplencia\sql\faturas_inadimplentes.sql"

        #realizando a leitura da query
        with open(caminho_query,'r') as arquivo_query:
            query = arquivo_query.read()

        #transformando em dataframe (pandas) e executando a consulta no athena 
        df_inadimp = awr.athena.read_sql_query(query, database='silver')

        #ponteiros cujo historico é diferente de 1
        lista_boletos_pagos = df_inadimp.loc[df_inadimp['historico'] != 1, ['ponteiro','conjunto','matricula']]

        # Gerando um index para comparação de séries
        df_inadimp = df_inadimp.set_index(['ponteiro', 'conjunto', 'matricula'])
        lista_boletos_pagos = lista_boletos_pagos.set_index(['ponteiro', 'conjunto', 'matricula'])

        # Comparando os index e filtrando
        df_inadimp = df_inadimp[~df_inadimp.index.isin(lista_boletos_pagos.index)]

        # Resetando o index para manter o padrão do restante do código
        df_inadimp = df_inadimp.reset_index()

        #filtrando com status de a pagar
        df_inadimp = df_inadimp[df_inadimp['historico']==1]
        
        
        df_inadimp = df_inadimp.drop_duplicates(subset=['ponteiro', 'conjunto', 'matricula', 'empresa'], keep='first')
        

        #fazendo validação com placas (chassis) canceladas
        # cancel_path = r"C:\Users\raphael.almeida\Grupo Unus\analise de dados - Arquivos em excel\CAMPANHA_RANKING_ATIVACOES.xlsx"
        # df_cancel = pd.read_excel(cancel_path, engine='openpyxl', sheet_name='CANCELAMENTOS')

        # chassis_cancel = df_cancel['chassi'].unique()

        # df_validacao = df_inadimp[~df_inadimp['chassi'].isin(chassis_cancel)]

        
        caminho_pasta = r'C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Inadimplência'
        caminho_arquivo = os.path.join(caminho_pasta,'relatorio_inadimplencia.xlsx')

        #verificando a existência da pasta e removendo a versão antiga
        os.makedirs(caminho_pasta,exist_ok=True)
        if os.path.exists(caminho_arquivo):
            os.remove(caminho_arquivo)
            print("Arquivo antigo removido, iniciando carregamento...")

        #convertendo o dataframe em excel e associando ao caminho da pasta
        df_inadimp.to_excel(caminho_arquivo, engine = 'openpyxl', index=False, sheet_name='inadimplentes')

        print(f"Arquivo Excel salvo com sucesso em: {caminho_arquivo}")

if __name__ == '__main__':
    ETL_relat_inadimp.ETL_inadimp()


                            
                