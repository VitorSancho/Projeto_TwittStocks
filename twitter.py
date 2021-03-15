import tweepy
import os
import dotenv
import pandas as pd
import sqlalchemy
import time
import datetime
import numpy as np
import sqlite3


def data_from_tweet(tweet, stock):
    return pd.DataFrame({
        "stock": [stock],
        "id_tweets": [tweet.id_str],
        "horario_tweet": [tweet.created_at.strftime("%Y-%m-%d %H:%M:%S")]})
    # "conteudo": [tweet.text]})


def deletar_dados_de_hoje(hoje, stock):
    db_query = f"delete tb_tweet where horario_tweet = '{hoje} and stock={stock}'"
    pass


def registrar_log_execucao(conexao, Cursor, max_tweets, quantidade_de_tweets, stock, hoje, primeira_coleta_de_tweets):
    if not primeira_coleta_de_tweets:
        db_query = f"SELECT MAX(horario_tweet) as horario,right(execucao_diaria,1) as execucao_diaria from tb_log where stock = '{stock}'"
        ultima_execucao = pd.read_sql_query(db_query, conexao)[
            'execucao_diaria'].values[0]
        execucao_atual = ultima_execucao+1
        if quantidade_de_tweets == max_tweets:
            obs = 'coleta de hoje foi cancelada pois a quantidade de tweetss foi inconsistente com a janela de tempo'
    else:
        execucao_atual = 1
        obs = ""

    execucao_atual = f'{execucao_atual}ª execucão do dia'
    horario_execucao = datetime.datetime.now()
    log_execucao = np.array([stock, horario_execucao,
                             execucao_atual, quantidade_de_tweets, obs])

    print(stock)
    Cursor.execute(
        f'INSERT INTO tb_log_execucao (stock, horario_coleta, execucao_diaria, quantidade, obs) values ({stock, horario_execucao, execucao_atual, quantidade_de_tweets, obs})')
    conexao.commit()

    pass


def coletar_tweets(stocks, max_tweets, hoje, Cursor):
    # diretorio_projeto = os.path.dirname(os.path.abspath(__file__))
    # caminho_banco_dados = os.path.join(diretorio_projeto, 'ProjetoAcoesTwitter.db')
    # conexao = sqlalchemy.create_engine(f"sqlite:///{caminho_banco_dados}")

    # dotenv.load_dotenv(dotenv.find_dotenv())

    # auth = tweepy.OAuthHandler(
    #     os.getenv("CUSTOMER_KEY"),
    #     os.getenv("CUSTOMER_SECRET"))

    # auth.set_access_token(
    #     os.getenv("ACCESSTOKEN"),
    #     os.getenv("ACCESSTOKEN_SECRET"))

    # api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    # # result = api.search(q="itsa4")
    # STOCKS = {"USA": ['GOOGL', 'AMZN', 'MSFT', 'TSLA', 'AAPL'],
    #         "BR": ['ITSA4', 'PETR4', 'VALE3', 'WEGE3']}
    # STOCKS = {"USA": ['teomewhy']}
    # max_tweets = 1000

    for pais in stocks.items():
        primeira_coleta_de_tweets = False
        lista_de_stocks = pais[1]
        for stock in lista_de_stocks:

            if stock == 'GOOGL':
                query = stock+' -GOOGLE'

            db_query = f"SELECT MAX(cast(id_tweets as bigint)) as max_id, horario_tweet as horario from tb_tweet where stock = '{stock}'"

            max_id = pd.read_sql_query(db_query, conexao)['max_id'].values[0]
            momento_ultimo_tweet = pd.read_sql_query(db_query, conexao)[
                'horario'].values[0]

            print(f"Iniciando coleta de tweets da ação {stock}")

            if max_id == None:  # caso em que a tabela não está populada
                max_id = 0
                dia_ultimo_tweet = hoje
                print("entrei aqui2")
                primeira_coleta_de_tweets = True
            else:
                momento_ultimo_tweet = datetime.datetime.strptime(
                    momento_ultimo_tweet, "%Y-%m-%d %H:%M:%S")
                dia_ultimo_tweet = momento_ultimo_tweet.date()

                if dia_ultimo_tweet != hoje:
                    primeira_coleta_de_tweets = True

            query = stock
            try:
                searched_tweets = [status for status in tweepy.Cursor(
                    api.search, q=query, since_id=max_id+1).items(max_tweets)]

            except tweepy.TweepError as e:
                print(e.reason)
                time.sleep(120)

            quantidade_de_tweets = 0
            try:
                data = pd.concat([data_from_tweet(i, stock)
                                  for i in searched_tweets])

                quantidade_de_tweets = data.shape[0]

                if not primeira_coleta_de_tweets and quantidade_de_tweets == max_tweets:
                    print("hey hey")
                    deletar_dados_de_hoje(hoje, stock)
                    continue

                data.to_sql(f"tb_tweet", conexao,
                            if_exists="append", index=False)

                print(
                    f"Foram adicionados {quantidade_de_tweets} tweets sobre {stock} a tabela tb_tweet")

            except ValueError:
                print(f"Não há novos tweets sobre {stock}")
                quantidade_de_tweets = 0

            registrar_log_execucao(
                conexao, Cursor, max_tweets, quantidade_de_tweets, stock, hoje, primeira_coleta_de_tweets)


if __name__ == "__main__":
    resposta = input("Deseja iniciar coleta de tweets, Y/N?")

    if resposta == "Y":
        diretorio_projeto = os.path.dirname(os.path.abspath(__file__))
        caminho_banco_dados = os.path.join(
            diretorio_projeto, 'ProjetoAcoesTwitter.db')
        conexao = sqlalchemy.create_engine(f"sqlite:///{caminho_banco_dados}")
        connection = sqlite3.connect('ProjetoAcoesTwitter.db')

        Cursor = connection.cursor()

        dotenv.load_dotenv(dotenv.find_dotenv())

        auth = tweepy.OAuthHandler(
            os.getenv("CUSTOMER_KEY"),
            os.getenv("CUSTOMER_SECRET"))

        auth.set_access_token(
            os.getenv("ACCESSTOKEN"),
            os.getenv("ACCESSTOKEN_SECRET"))

        api = tweepy.API(auth, wait_on_rate_limit=True,
                         wait_on_rate_limit_notify=True)
        # result = api.search(q="itsa4")
        STOCKS = {"USA": ['GOOGL', 'AMZN', 'MSFT', 'TSLA', 'AAPL'],
                  "BR": ['ITSA4', 'PETR4', 'VALE3', 'WEGE3']}
        max_tweets = int(input("Quantos tweets deseja coletar no máximo?"))
        hoje = datetime.date.today()
        # fAZER LOG PARA INDICAR SE O NÚMEOR DE TWEETS COLETADOS SÃO MENORES QUE O REQUERIDO

        coletar_tweets(STOCKS, max_tweets, hoje, Cursor)
