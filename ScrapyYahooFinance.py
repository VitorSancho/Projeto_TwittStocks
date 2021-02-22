import requests
import pandas as pd
import xlsxwriter
from datetime import datetime
import os
import sqlalchemy

diretorio_projeto = os.path.dirname(os.path.abspath(__file__))
caminho_banco_dados = os.path.join(diretorio_projeto, 'ProjetoAcoesTwitter.db')
conexao = sqlalchemy.create_engine(f"sqlite:///{caminho_banco_dados}")

dados = pd.DataFrame()
STOCKS = {"USA": ['GOOGL', 'AMZN', 'MSFT', 'TSLA', 'AAPL'],
          "BR": ['ITSA4', 'PETR4', 'VALE3', 'WEGE3']}
# lista_de_acoes_do_ibov = pd.read_excel("IBOV.xlsx")
# total=len(lista_de_acoes_do_ibov)
amostra = "GOOGL"
# contador=1
# for codigo_acao in lista_de_acoes_do_ibov["Código"]:

url = f"https://finance.yahoo.com/quote/{amostra}/history?p={amostra}"


header = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
          "X-Requested-With": "XMLHttpRequest"}

r = requests.get(url, headers=header)

lista_de_tabelas = pd.read_html(r.text)

tabela_completa = pd.DataFrame(lista_de_tabelas[0])
tabela_dados_importante = tabela_completa.copy(deep=True)
tabela_dados_importante = tabela_completa[['Date', 'Close*', 'Volume']]
tabela_dados_importante.rename(columns={"Close*": 'Close'}, inplace=True)
tabela_dados_importante.drop(tabela_dados_importante.index[-1], inplace=True)
tabela_dados_importante['stock'] = 'Googl'


def corrige_data(str_data):
    return datetime.strptime(str_data, '%b %d, %Y')


tabela_dados_importante["Date"] = pd.to_datetime(
    tabela_dados_importante["Date"], format='%b %d, %Y')

# tabela_dados_importante['data_id'] = tabela_dados_importante["Date"].split(
#     "-")[0]+tabela_dados_importante["Date"].split("-")[1]+tabela_dados_importante["Date"].split("-")[2]


def gera_data_id(x):
    corte = str(x).split("-")
    if len(corte[2][:1]) == 1:
        return (corte[0]+corte[1]+corte[2]+"0")[0:8]
    return (corte[0]+corte[1]+corte[2])[0:7]


tabela_dados_importante['data_id'] = tabela_dados_importante['Date'].apply(
    lambda x: gera_data_id(x))

indice = '20210210'

db_query = "SELECT MAX(data_id) as ultima_data_importada from tb_negociacao_googl"

try:
    ultima_data_importada = pd.read_sql_query(
        db_query, conexao)
except:
    ultima_data_importada = 0

print(ultima_data_importada)

for i in range(99):
    if tabela_dados_importante.loc[i]['data_id'] == indice:
        indice_corte = i
        break
# print("#####"+str(indice_corte))

tabela_novos_dados = tabela_dados_importante[:indice_corte]

# talvez não precise disso
pd.to_numeric(tabela_novos_dados['Close'],
              downcast='float', errors='ignore')
pd.to_numeric(tabela_novos_dados['Volume'],
              downcast='integer', errors='ignore')


tabela_dados_importante.to_sql(f"tb_negociacao_googl", conexao,
                               if_exists="append", index=False)


# print(tabela_dados_importante.head())

# Limitar data frame de acordo com o max id
