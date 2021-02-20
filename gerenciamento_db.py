import sqlite3

connection = sqlite3.connect('Tweets.db')

# cursor permite execução de comandos SQL
Cursor = connection.cursor()


def create_table():
    Cursor.execute(
        'CREATE TABLE tb_ (id_tweet integer, horario_tweet text, conteudo text)')


create_table()


def drop_table():
    Cursor.execute('DROP TABLE tb_testes')
    connection.commit()


# drop_table()


def insert():
    Cursor.execute("INSERT INTO tb_testes values (1, 'vsvv'), (5, 'verv')")
    # connection.commit()


# insert()
