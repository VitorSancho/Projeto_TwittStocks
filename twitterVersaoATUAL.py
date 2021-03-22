import tweepy
import os
import dotenv
import pandas as pd
import sqlalchemy
import time  # talvez não precise
import datetime


def verifica_se_eh_primeira_execucao(conexao):
    db_query = f"SELECT MAX(horario_coleta) as ultima_coleta,substr(execucao_diaria,1,1) as execucao_diaria from tb_log_execucao limit 1"
    # alterando aqui
    try:
        ultima_coleta = datetime.datetime.strptime((pd.read_sql_query(db_query, conexao)[
        'ultima_coleta'].values[0]), '%Y-%m-%d %H:%M:%S').date()
        execucao_diaria = pd.read_sql_query(db_query, conexao)[
        'execucao_diaria'].values[0]

    primeira_execucao=ultima_coleta == datetime.datetime.today().date()    

    if primeira_execucao:
        execucao_diaria=0        

    return primeira_execucao, execucao_diaria
    # se data da ultima coleta igual a hoje
    # função = False

    except:
        return True, 0


def data_from_tweet(tweet, stock):
    return pd.DataFrame({
        "stock": [stock],
        "id_tweets": [tweet.id_str],
        "horario_tweet": [tweet.created_at.strftime("%Y-%m-%d %H:%M:%S")]})
    # "conteudo": [tweet.text]})


def coletar_tweets(stocks, quantidade_tweets_a_coletar):
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
        lista_de_stocks = pais[1]
        for stock in lista_de_stocks:

            if stock == 'GOOGL':
                query = stock+' -GOOGLE'

            db_query = f"SELECT MAX(cast(id_tweets as bigint)) as max_id from tb_tweet where stock = '{stock}'"

            max_id = pd.read_sql_query(db_query, conexao)['max_id'].values[0]
            print("eintrei aqui")

            if max_id == None:
                max_id = 0
                print("entrei aqui2")

            # ainda preciso dessa variável?
            query = stock
            try:
                searched_tweets = [status for status in tweepy.Cursor(
                    api.search, q=query, since_id=max_id+1).items(quantidade_tweets_a_coletar)]

            except tweepy.TweepError as e:
                print(e.reason)
                time.sleep(120)

            quantidade_de_tweets_coletados = 0
            try:
                data = pd.concat([data_from_tweet(i, stock)
                                  for i in searched_tweets])

                quantidade_de_tweets_coletados = data.shape[0]

                # testar condição para primeira execução do dia
                #CONTINUAR AQUI TESTANDO SE A ULTIMA EXECUCAO É DO DIA ATUAL
                if quantidade_de_tweets_coletados != quantidade_tweets_a_coletar:

                    data.to_sql(f"tb_tweet", conexao,
                                if_exists="append", index=False)

                    print(
                        f"Foram adicionados {quantidade_de_tweets_coletados} tweets sobre {stock} a tabela tb_tweet")

                else:
                    print(
                        f"Não foi adicionado nenhum tweet sobre {stock} pois a coleta não está confiável com a janela de tempo.")

            except ValueError:
                print(f"Não há novos tweets sobre {stock}")


if __name__ == "__main__":
    resposta = input("Deseja iniciar coleta de tweets, Y/N?")
    if resposta == "Y":

        quantidade_tweets_a_coletar = int(
            input("Quantos tweets deseja coletar?"))
        diretorio_projeto = os.path.dirname(os.path.abspath(__file__))
        caminho_banco_dados = os.path.join(
            diretorio_projeto, 'ProjetoAcoesTwitter.db')
        conexao = sqlalchemy.create_engine(f"sqlite:///{caminho_banco_dados}")

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

        # puxar qual é a execução do dia
        primeira_execucao_do_dia = verifica_se_eh_primeira_execucao(
            conexao)
        # print(primeira_execucao_do_dia, execucao_diaria)
        # passar qual é a execução do dia como parâmetro da função

        # coletar_tweets(STOCKS, quantidade_tweets_a_coletar,primeira_execucao_do_dia)
