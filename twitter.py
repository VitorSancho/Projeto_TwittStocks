import tweepy
import os
import dotenv
import pandas as pd
import sqlalchemy
import time


def data_from_tweet(tweet, stock):
    return pd.DataFrame({
        "stock": [stock],
        "id_tweets": [tweet.id_str],
        "horario_tweet": [tweet.created_at.strftime("%Y-%m-%d %H:%M:%S")],
        "conteudo": [tweet.text]})


diretorio_projeto = os.path.dirname(os.path.abspath(__file__))
caminho_banco_dados = os.path.join(diretorio_projeto, 'ProjetoAcoesTwitter.db')
conexao = sqlalchemy.create_engine(f"sqlite:///{caminho_banco_dados}")

dotenv.load_dotenv(dotenv.find_dotenv())

auth = tweepy.OAuthHandler(
    os.getenv("CUSTOMER_KEY"),
    os.getenv("CUSTOMER_SECRET"))

auth.set_access_token(
    os.getenv("ACCESSTOKEN"),
    os.getenv("ACCESSTOKEN_SECRET"))

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
# result = api.search(q="itsa4")
STOCKS = {"USA": ['GOOGL', 'AMZN', 'MSFT', 'TSLA', 'AAPL'],
          "BR": ['ITSA4', 'PETR4', 'VALE3', 'WEGE3']}
# STOCKS = {"USA": ['teomewhy']}
max_tweets = 500

for pais in STOCKS.items():
    lista_de_stocks = pais[1]
    for stock in lista_de_stocks:

        if stock == 'GOOGL':
            query = stock+' -GOOGLE'

        db_query = f"SELECT MAX(cast(id_tweets as bigint)) as max_id from tb_tweet where stock = '{stock}'"

        max_id = pd.read_sql_query(db_query, conexao)['max_id'].values[0]
        print("entrei aqui")

        if max_id == None:
            max_id = 0
            print("entrei aqui2")

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

            data.to_sql(f"tb_tweet", conexao,
                        if_exists="append", index=False)

            quantidade_de_tweets = data.shape[0]

            print(
                f"Foram adicionados {quantidade_de_tweets} tweets sobre {stock} a tabela tb_tweet")

        except ValueError:
            print(f"Não há novos tweets sobre {stock}")
