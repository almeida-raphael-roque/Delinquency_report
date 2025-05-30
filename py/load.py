import pandas as pd 
import openpyxl
import awswrangler as awr

class Load:
    def loader(self,sql,name):
        sql_path = fr"C:\Users\raphael.almeida\Documents\Processos\relatorio_inadimplencia\sql\{sql}.sql"

        with open(sql_path, 'r') as r:
            query = r.read()

        df = awr.athena.read_sql_query(query, database='silver')

        save_path = fr"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Inadimplência\{name}.xlsx"

        df.to_excel(save_path, engine='openpyxl', index=False, sheet_name = name)

if __name__ == '__main__':
    instance = Load()
    load_ativos = instance.loader('conjuntos_clientes_ativos', 'conjuntos_clientes_ativos')
    load_inadimplentes = instance.loader('faturas_inadimplentes', 'relatorio_inadimplencia')
    load_pagamentos = instance.loader('faturas_baixadas', 'relatorio_pagamentos')