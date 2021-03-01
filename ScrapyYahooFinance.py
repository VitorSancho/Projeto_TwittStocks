import requests
import pandas as pd
import os
import sqlalchemy


def gera_data_id(x):
    corte = str(x).split("-")
    if len(corte[2][:1]) == 1:
        return int((corte[0]+corte[1]+corte[2]+"0")[0:8])
    return int((corte[0]+corte[1]+corte[2])[0:7])


def gera_header_request(pais):
    if pais == 'USA':
        url = f"https://finance.yahoo.com/quote/{ticker_acao}/history?p={ticker_acao}"
    elif pais == 'BR':
        url = f"https://finance.yahoo.com/quote/{ticker_acao}.SA/history?p={ticker_acao}.SA"

    header = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
              "X-Requested-With": "XMLHttpRequest"}
    r = requests.get(url, headers=header)
    return r


def gera_indice_corte_dataframe(ultima_data_importada, default=None):
    for linha in range(99):
        if tabela_dados_importante.loc[linha]['data_id'] == ultima_data_importada:
            return linha
            break

    return default


diretorio_projeto = os.path.dirname(os.path.abspath(__file__))
caminho_banco_dados = os.path.join(diretorio_projeto, 'ProjetoAcoesTwitter.db')
conexao = sqlalchemy.create_engine(f"sqlite:///{caminho_banco_dados}")

STOCKS = {"USA": ['GOOGL', 'AMZN', 'MSFT', 'TSLA', 'AAPL'],
          "BR": ['ITSA4', 'PETR4', 'VALE3', 'WEGE3']}
# STOCKS = {"USA": ['GOOGL']}

for pais in STOCKS.items():
    lista_de_acoes_do_pais = pais[1]
    for ticker_acao in lista_de_acoes_do_pais:
        print(f'Buscando dados de negociação da {ticker_acao}')

        req = gera_header_request(pais[0])
        lista_de_tabelas = pd.read_html(req.text)
        tabela_completa = pd.DataFrame(lista_de_tabelas[0])

        tabela_dados_importante = tabela_completa.copy(deep=True)

        tabela_dados_importante = tabela_completa.iloc[:, [0, 4, 6]].values
        tabela_dados_importante = pd.DataFrame(
            tabela_dados_importante, columns=['Date', 'Close', 'Volume'])

        tabela_dados_importante.drop(
            tabela_dados_importante.index[-1], inplace=True)
        tabela_dados_importante["Date"] = pd.to_datetime(
            tabela_dados_importante["Date"], format='%b %d, %Y')

        tabela_dados_importante['stock'] = f'{ticker_acao}'
        tabela_dados_importante['data_id'] = tabela_dados_importante['Date'].apply(
            lambda x: gera_data_id(x))

        db_query = f"SELECT MAX(data_id) as ultima_data_importada from tb_negociacoes where stock= '{ticker_acao}'"

        ultima_data_importada = pd.read_sql_query(
            db_query, conexao).values[0][0]
        # print(ultima_data_importada)

        indice_corte = gera_indice_corte_dataframe(ultima_data_importada, 99)

        # print("##########"+str(indice_corte))
        novos_dados_negociacao = tabela_dados_importante[:indice_corte]

        novos_dados_negociacao.to_sql(f"tb_negociacoes", conexao,
                                      if_exists="append", index=False)

        print(
            f'Os dados de negociação da {ticker_acao} foram salvos com sucesso! \n Um total de {indice_corte} registros foram adicionados')
