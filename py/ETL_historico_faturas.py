import pandas as pd
import openpyxl
import awswrangler as awr

#tenho que mudar as colunas
class ETL_hist_fat:
    def __init__(self):
        self.colunas_comuns = [
            "codigo_cadastro",
            "ponteiro",
            "numero_documento",
            "numero_boleto",
            "nosso_numero",
            "sequencia_documento",
            "aplicacao_financeira",
            "valor_titulo",
            "valor_baixa",
            "data_emissao",
            "data_baixa",
            "data_vencimento",
            "data_atualizacao",
            "conjunto",
            "matricula",
            "unidade",
            "cooperativa",
            "empresa",
            "associado",
            "vendedor",
            "status_conjunto",
            "grupo",
            "situacao"
        ]
        self.excel_path = r"C:\Users\raphael.almeida\Documents\Processos\relatorio_inadimplencia\historico_faturas.xlsx"
        self.inadimplentes_excel = r"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Inadimplência\relatorio_inadimplencia.xlsx"
        self.pagamentos_excel = r"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Inadimplência\relatorio_pagamentos.xlsx"
        self.onedrive_save_path = r"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Inadimplência\historico_faturas.xlsx"

    def carregar_base_excel(self):
        df_base = pd.read_excel(self.excel_path, engine='openpyxl')

        # Garantir todas as colunas necessárias; preenche ausentes com NA sem erro em DF vazio
        df_base = df_base.reindex(columns=self.colunas_comuns, fill_value=pd.NA)
        df_base.drop_duplicates(subset=['matricula', 'conjunto', 'cooperativa', 'ponteiro', 'situacao', 'data_atualizacao'], inplace=True)
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
        
        df_inadimplentes = pd.read_excel(self.inadimplentes_excel, engine='openpyxl')
        df_inadimplentes.loc[:, 'data_atualizacao'] = df_inadimplentes['data_vencimento']
        df_inadimplentes.loc[:, 'situacao'] = 'INADIMPLENTE'
        
        df_inadimplentes['data_vencimento'] = pd.to_datetime(df_inadimplentes['data_vencimento'])
        df_inadimplentes = df_inadimplentes[df_inadimplentes['data_vencimento'] > pd.to_datetime('2025-08-31')]
       
        # Garantir todas as colunas necessárias; preenche ausentes com NA sem erro em DF vazio
        df_inadimplentes = df_inadimplentes.reindex(columns=self.colunas_comuns, fill_value=pd.NA)
        return df_inadimplentes

    def carregar_pagamentos(self, df_inadimplentes_mascara):

        df_pagamentos = pd.read_excel(self.pagamentos_excel, engine='openpyxl')
        df_pagamentos.loc[:, 'data_atualizacao'] = df_pagamentos['data_baixa']
        df_pagamentos.loc[:, 'situacao'] = 'PAGO'
        
        # Garantir todas as colunas necessárias; preenche ausentes com NA sem erro em DF vazio
        df_pagamentos = df_pagamentos.reindex(columns=self.colunas_comuns, fill_value=pd.NA)
        
        df_pagamentos['data_baixa'] = pd.to_datetime(df_pagamentos['data_baixa'])
        df_pagamentos['data_vencimento'] = pd.to_datetime(df_pagamentos['data_vencimento'])
        
        df_pagamentos = df_pagamentos[
            (df_pagamentos['data_baixa'] > pd.to_datetime('2025-08-31')) &
            (df_pagamentos['data_vencimento'] > pd.to_datetime('2025-08-31'))
        ]
        
        return df_pagamentos[df_pagamentos[['ponteiro', 'conjunto', 'matricula', 'empresa']].apply(tuple, axis=1).isin(df_inadimplentes_mascara)]
        

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
        
        # Criar conjunto máscara com ponteiro, conjunto, matricula e empresa
        df_inadimplentes_mascara = [
            tuple(row) for row in df_composto_inadimplentes[['ponteiro', 'conjunto', 'matricula', 'empresa']].values
        ]

        
        # Carregar pagamentos
        df_pagamentos_inadimplencia = self.carregar_pagamentos(df_inadimplentes_mascara)
        
        # Combinar todas as bases
        df_atualizado = pd.concat([df_composto_inadimplentes, df_pagamentos_inadimplencia])
        df_atualizado.drop_duplicates(
            subset=['matricula', 'conjunto', 'cooperativa', 'ponteiro', 'situacao', 'data_atualizacao'],
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
        
        return df_atualizado

    def salvar_resultado(self, df):
        df.to_excel(self.excel_path, engine='openpyxl', index=False, sheet_name='historico_faturas')
        df.to_excel(self.onedrive_save_path, engine='openpyxl', index=False, sheet_name='historico_faturas')
        return len(df)

    @classmethod
    def ETL_hist_fat(cls):
        etl = cls()
        df_final = etl.processar_dados()
        total_registros = etl.salvar_resultado(df_final)
        print(f"Total de registros processados: {total_registros}")

if __name__ == '__main__':
    ETL_hist_fat.ETL_hist_fat()