import sqlite3

connection = sqlite3.connect('ProjetoAcoesTwitter.db')

# cursor permite execução de comandos SQL
Cursor = connection.cursor()

campos_negociacoes = "id integer primary key autoincrement,stock varchar(20),data_id int, Date DATETIME, Close numeric, Volume bigint, FOREIGN KEY (stock) REFERENCES tb_stocks (stock)"
campos_tweet = "id integer primary key autoincrement,stock varchar(20), id_tweets int, horario_tweet timestamp, FOREIGN KEY (stock) REFERENCES tb_stocks (stock)"
campos_stock = "stock varchar(20), empresa varchar(20), país varchar(20), PRIMARY KEY (stock)"
campos_log = "id integer primary key autoincrement,stock varchar(20),horario_coleta timestamp,execucao_diaria varchar(20),quantidade int,obs varchar(200)"
# conteudo varchar,


def cria_tabela(nome_tabela, cursor_database, campos):
    cursor_database.execute(
        f'CREATE TABLE if not exists tb_{nome_tabela} ({campos})')
    # cursor_database.execute(
    #     f'CREATE TABLE if not exists tb_{nome_tabela} ({campos})')
    print(f'Tabela tb_{nome_tabela} criada!')


def deleta_tabela(nome_tabela, cursor_database):
    cursor_database.execute(f'DROP TABLE if exists tb_{nome_tabela}')
    connection.commit()


def cria_conjunto_de_tabelas(iteravel, cursor_database):
    if type(iteravel) == dict:
        tipo = "dicionario"
        for categoria in iteravel.items():
            lista_de_tabelas = categoria[1]
            [cria_tabela(tabela, cursor_database)
             for tabela in lista_de_tabelas]
    elif type(iteravel) == list:
        tipo = "lista"
        for tabela in iteravel:
            cria_tabela(tabela, cursor_database)
    print(f'Tabelas foram criadas de acordo com {tipo}!')


def apaga_conjunto_de_tabelas(iteravel, cursor_database):
    if type(iteravel) == dict:
        tipo = "dicionario"
        for categoria in iteravel.items():
            lista_de_tabelas = categoria[1]
            [deleta_tabela(tabela, cursor_database)
             for tabela in lista_de_tabelas]
    elif type(iteravel) == list:
        tipo = "lista"
        for tabela in iteravel:
            deleta_tabela(tabela, cursor_database)
    print(f'Tabelas foram deletadas de acordo com {tipo}!')


def insere_conjunto_de_dados(conjunto):
    Cursor.execute(
        f'INSERT INTO tb_stocks (stock,empresa,país) values {conjunto}')
    connection.commit()

    Cursor.execute(
        'CREATE TABLE if not exists tb_stocks (stock varchar(6) primary key,empresa varchar(15), país varchar(10))')

# STOCKS = {"USA": ['tweet_GOOGL', 'tweet_AMZN', 'tweet_MSFT', 'tweet_TSLA', 'tweet_AAPL'],
#           "BR": ['tweet_ITSA4', 'tweet_PETR4', 'tweet_VALE3', 'tweet_WEGE3']}
#
# STOCKS = {"USA": ['GOOGL', 'AMZN', 'MSFT', 'TSLA', 'AAPL'],
#           "BR": ['ITSA4', 'PETR4', 'VALE3', 'WEGE3']}
#
# negociacao_STOCKS = {"USA": ['negociacao_GOOGL', 'negociacao_AMZN', 'negociacao_MSFT', 'negociacao_TSLA', 'negociacao_AAPL'],
#                      "BR": ['negociacao_ITSA4', 'negociacao_PETR4', 'negociacao_VALE3', 'negociacao_WEGE3']}


STOCKS = (("GOOGL", "Google", "USA"), ("AMZN", "Amazon", "USA"), ("MSFT", "Microsoft", "USA"),
          ("TSLA", "Tesla", "USA"), ("AAPL", "Apple",
                                     "USA"), ("ITSA4", "Itaúsa", "Brasil"),
          ("PETR4", "Petrobras", "Brasil"), ("VALE3",
                                             "Vale", "Brasil"), ("WEGE3", "Weg", "Brasil"))

# [insere_conjunto_de_dados(stock) for stock in STOCKS]

# cursor_database.execute('CREATE INDEX indice_stock ON table_name (stock)')
lista = ['stocks', 'negociacoes', 'tweet']
# apaga_conjunto_de_tabelas(lista, Cursor)


# # Cursor.execute('delete from tb_tweet')
# # connection.commit()

# cria_tabela('stocks', Cursor, campos_stock)
# cria_tabela("tweet", Cursor, campos_tweet)
# cria_tabela("negociacoes", Cursor, campos_negociacoes)
# cria_tabela("log_execucao", Cursor, campos_log)


Cursor.execute(
    'INSERT INTO tb_log_execucao (stock, horario_coleta, execucao_diaria, quantidade, obs) values ("stock", 2020-20-21, "1ª execucao", 20, "testando")')
connection.commit()
