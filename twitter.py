import tweepy
import os
import dotenv
import pandas as pd
import sqlalchemy


def data_from_tweet(tweet, stock):
    return pd.DataFrame({
        "stock": [stock],
        "id_tweet": [tweet.id_str],
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

api = tweepy.API(auth)
# result = api.search(q="itsa4")
STOCKS = {"USA": ['GOOGL', 'AMZN', 'MSFT', 'TSLA', 'AAPL'],
          "BR": ['ITSA4', 'PETR4', 'VALE3', 'WEGE3']}
max_tweets = 5

for pais in STOCKS.items():
    lista_de_acoes = pais[1]
    for ticker_acao in lista_de_acoes:

        if ticker_acao == 'GOOGL':
            query = ticker_acao+' -GOOGLE'

        db_query = f"SELECT MAX(cast(id_tweet as bigint)) as max_id from tb_{ticker_acao}"
        try:
            max_id = pd.read_sql_query(db_query, conexao)['max_id'].values[0]
        except:
            max_id = 0

        query = ticker_acao
        searched_tweets = [status for status in tweepy.Cursor(
            api.search, q=query, since_id=max_id+1).items(max_tweets)]

        quantidade_de_tweets = 0
        try:
            data = pd.concat([data_from_tweet(i, ticker_acao)
                              for i in searched_tweets])
            data.to_sql(f"tb_{ticker_acao}", conexao,
                        if_exists="append", index=False)
            quantidade_de_tweets = data.shape[0]
            print(
                f"Foram adicionados {quantidade_de_tweets} dados a tabela tb_{ticker_acao}")

        except ValueError:
            print(f"Não há novos tweets sobre {ticker_acao}")
