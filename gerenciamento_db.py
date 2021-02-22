import sqlite3

connection = sqlite3.connect('ProjetoAcoesTwitter.db')

# cursor permite execução de comandos SQL
Cursor = connection.cursor()


def cria_tabela(nome_tabela, cursor_database):
    cursor_database.execute(
        f'CREATE TABLE if not exists tb_{nome_tabela} (stock text,data_id text, Date DATETIME, Close numeric, Volume bigint)')


def deleta_tabela(nome_tabela, cursor_database):
    cursor_database.execute(f'DROP TABLE tb_{nome_tabela}')
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


STOCKS = {"USA": ['GOOGL', 'AMZN', 'MSFT', 'TSLA', 'AAPL'],
          "BR": ['ITSA4', 'PETR4', 'VALE3', 'WEGE3']}

negociacao_STOCKS = {"USA": ['negociacao_GOOGL', 'negociacao_AMZN', 'negociacao_MSFT', 'negociacao_TSLA', 'negociacao_AAPL'],
                     "BR": ['negociacao_ITSA4', 'negociacao_PETR4', 'negociacao_VALE3', 'negociacao_WEGE3']}

vale = ['negociacao_googl']
cria_conjunto_de_tabelas(vale, Cursor)
# apaga_conjunto_de_tabelas(vale, Cursor)

# Cursor.execute(
#     'insert into tb_negociacao_GOOGL values ("vvs","cccss","2020-02-02","456.23","3458325082")')
# connection.commit()
