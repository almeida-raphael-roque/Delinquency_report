import pandas as pd
import openpyxl
import awswrangler as awr

class ETL_hist_fat:
    def __init__(self):
        self.colunas_comuns = [
            "matricula",
            "conjunto",
            "cooperativa",
            "ponteiro",
            "numero_boleto",
            "nosso_numero",
            "unidade",
            "associado",
            "data_vencimento",
            "data_baixa",
            "valor_titulo",
            "valor_baixa",
            "data_emissao",
            "data_atualizacao",
            "situacao"
        ]
        self.excel_path = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_inadimplencia\historico_faturas.xlsx"
        self.excel_save_path = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_inadimplencia\historico_faturas.xlsx"
        self.query_path_inadimplentes = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_inadimplencia\sql\faturas_inadimplentes.sql"
        self.query_path_baixadas = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_inadimplencia\sql\faturas_baixadas.sql"
        self.onedrive_save_path = r"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Inadimplência\historico_faturas.xlsx"

    def carregar_base_excel(self):
        df_base = pd.read_excel(self.excel_path, engine='openpyxl')
        df_base = df_base[self.colunas_comuns]
        df_base.drop_duplicates(subset=['matricula', 'conjunto', 'cooperativa','ponteiro', 'situacao', 'data_atualizacao'], inplace=True)
        df_base.drop_duplicates(subset=['matricula', 'conjunto', 'cooperativa','ponteiro', 'data_atualizacao'], inplace=True)
        return df_base

    def tratar_datas_base(self, df):
        for coluna in ['data_vencimento', 'data_baixa', 'data_emissao']:
            df[coluna] = pd.to_datetime(df[coluna])
            df[coluna] = df[coluna].dt.date
        return df

    def tratar_colunas_nulas(self, df):
        df['data_baixa'] = df['data_baixa'].fillna(pd.Timestamp('1900-01-01'))
        df['data_emissao'] = df['data_emissao'].fillna(pd.Timestamp('1900-01-01'))
        df['data_vencimento'] = df['data_vencimento'].fillna(pd.Timestamp('1900-01-01'))
        df['valor_titulo'] = df['valor_titulo'].fillna(0)
        df['valor_baixa'] = df['valor_baixa'].fillna(0)
        df['nosso_numero'] = df['nosso_numero'].fillna('NULL')
        df['numero_boleto'] = df['numero_boleto'].fillna('NULL')
        return df

    def carregar_inadimplentes(self):
        with open(self.query_path_inadimplentes, 'r') as file:
            query = file.read()
        
        df_inadimplentes = awr.athena.read_sql_query(query, database='silver')
        df_inadimplentes = df_inadimplentes.drop_duplicates('ponteiro', keep='first')
        
        df_inadimplentes.loc[:, 'data_atualizacao'] = df_inadimplentes['data_vencimento']
        df_inadimplentes.loc[:, 'situacao'] = 'INADIMPLENTE'
        
        df_inadimplentes['data_vencimento'] = pd.to_datetime(df_inadimplentes['data_vencimento'])
        df_inadimplentes = df_inadimplentes[df_inadimplentes['data_vencimento'] > pd.to_datetime('2025-05-31')]
        
        for col in self.colunas_comuns:
            if col not in df_inadimplentes.columns:
                df_inadimplentes.loc[:, col] = pd.NA
        
        df_inadimplentes = df_inadimplentes[self.colunas_comuns]
        return df_inadimplentes

    def carregar_pagamentos(self, df_inadimplentes_lista):
        with open(self.query_path_baixadas, 'r') as file:
            query = file.read()
        
        df_pagamentos = awr.athena.read_sql_query(query, database='silver')
        
        for col in self.colunas_comuns:
            if col not in df_pagamentos.columns:
                df_pagamentos.loc[:, col] = pd.NA
        
        df_pagamentos = df_pagamentos[self.colunas_comuns]
        df_pagamentos.loc[:, 'data_atualizacao'] = df_pagamentos['data_baixa']
        df_pagamentos.loc[:, 'situacao'] = 'PAGO'
        
        df_pagamentos['data_baixa'] = pd.to_datetime(df_pagamentos['data_baixa'])
        df_pagamentos['data_vencimento'] = pd.to_datetime(df_pagamentos['data_vencimento'])
        
        df_pagamentos = df_pagamentos.drop_duplicates('ponteiro', keep='first')
        df_pagamentos = df_pagamentos[
            (df_pagamentos['data_baixa'] > pd.to_datetime('2025-05-31')) &
            (df_pagamentos['data_vencimento'] > pd.to_datetime('2025-05-31'))
        ]
        
        return df_pagamentos[df_pagamentos['ponteiro'].isin(df_inadimplentes_lista)]

    def processar_dados(self):
        # Carregar e tratar base inicial
        df_base = self.carregar_base_excel()
        df_base = self.tratar_datas_base(df_base)
        df_base = self.tratar_colunas_nulas(df_base)
        
        # Carregar e tratar inadimplentes
        df_inadimplentes = self.carregar_inadimplentes()
        
        # Combinar bases
        df_composto_inadimplentes = pd.concat([df_base, df_inadimplentes])
        df_composto_inadimplentes = df_composto_inadimplentes.drop_duplicates(keep='first')
        
        # Criar lista de inadimplentes
        df_inadimplentes_lista = df_composto_inadimplentes.loc[
            df_composto_inadimplentes['data_vencimento'].notna(), 'ponteiro'
        ].to_list()
        
        # Carregar pagamentos
        df_pagamentos_inadimplencia = self.carregar_pagamentos(df_inadimplentes_lista)
        
        # Combinar todas as bases
        df_atualizado = pd.concat([df_composto_inadimplentes, df_pagamentos_inadimplencia])
        df_atualizado.drop_duplicates(
            subset=['matricula', 'conjunto', 'cooperativa', 'ponteiro', 'situacao', 'data_atualizacao'],
            inplace=True
        )
        df_atualizado.drop_duplicates(
            subset=['matricula', 'conjunto', 'cooperativa', 'ponteiro', 'data_atualizacao'],
            inplace=True
        )
        
        # Tratar datas e colunas nulas do resultado final
        df_atualizado = self.tratar_datas_base(df_atualizado)
        df_atualizado['data_atualizacao'] = pd.to_datetime(df_atualizado['data_atualizacao']).dt.date
        df_atualizado = df_atualizado[self.colunas_comuns]
        df_atualizado = self.tratar_colunas_nulas(df_atualizado)
        
        # Tratar data de emissão
        df_atualizado['data_emissao'] = pd.to_datetime(df_atualizado['data_emissao'], errors='coerce')
        df_atualizado['data_emissao'] = df_atualizado['data_emissao'].dt.date
        
        # Remover duplicatas finais
        df_atualizado.drop_duplicates(
            subset=['matricula', 'conjunto', 'cooperativa', 'ponteiro', 'situacao', 'data_atualizacao'],
            inplace=True
        )
        df_atualizado.drop_duplicates(
            subset=['matricula', 'conjunto', 'cooperativa', 'ponteiro', 'data_atualizacao'],
            inplace=True
        )
        
        return df_atualizado

    def salvar_resultado(self, df):
        df.to_excel(self.excel_save_path, engine='openpyxl', index=False, sheet_name='historico_faturas')
        df.to_excel(self.onedrive_save_path, engine='openpyxl', index=False, sheet_name='historico_faturas')
        return len(df)

if __name__ == '__main__':
    etl = ETL_hist_fat()
    df_final = etl.processar_dados()
    total_registros = etl.salvar_resultado(df_final)
    print(f"Total de registros processados: {total_registros}")