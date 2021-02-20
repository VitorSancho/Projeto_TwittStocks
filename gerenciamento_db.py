import sqlite3

connection = sqlite3.connect('ProjetoAcoesTwitter.db')

# cursor permite execução de comandos SQL
Cursor = connection.cursor()


def cria_tabela(nome_tabela, cursor_database):
    cursor_database.execute(
        f'CREATE TABLE if not exists tb_{nome_tabela} (stock text,id_tweet integer, horario_tweet DATETIME, conteudo text)')


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

vale = ['vale4']
cria_conjunto_de_tabelas(STOCKS, Cursor)
# apaga_conjunto_de_tabelas(vale, Cursor)
