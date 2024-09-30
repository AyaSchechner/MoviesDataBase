import mysql.connector as mc


def create_table(cursor, query):
    try:
        table_name = query.split('(')[0].split()[-1]
        print(f'Creating table {table_name}')
        cursor.execute(query)
    except mc.Error as e:
        print(e.msg)


def create_index(cursor, index):
    try:
        index_name = index.split()[2]
        print(f'creating index {index_name}')
        cursor.execute(index)
    except mc.Error as e:
        print(e.msg)


if __name__ == "__main__":
    # Connect to mysql server
    connection = mc.connect(host='localhost',
                                 user='ayaschechner',
                                 password='ayasc77989',
                                 db='ayaschechner',
                                 port=3305)
    cursor = connection.cursor()

    # Table 1
    movies = ("CREATE TABLE movies ("
              "movie_id INT(6),"
              "title VARCHAR(255)," 
              "genre VARCHAR(25)," 
              "PRIMARY KEY(movie_id),"
              "FULLTEXT(genre)"
              ")")
    create_table(cursor, movies)

    # Table 2
    actors = ("CREATE TABLE actors ( "
              "name VARCHAR(50),"
              "movie_id INT(6),"
              "FOREIGN KEY(movie_id) REFERENCES movies(movie_id)"
              ")")
    create_table(cursor, actors)

    # Table 3
    movies_rating = ("CREATE TABLE movies_rating (" 
                     "movie_id INT(6),"
                     "popularity INT(10),"
                     "vote_count INT(10),"
                     "vote_average INT(10),"
                     "FOREIGN KEY(movie_id) REFERENCES movies(movie_id)"
                     ")")
    create_table(cursor, movies_rating)

    # Table 4
    movie_technical = ("CREATE TABLE movie_technical ("
                       "movie_id INT(6),"
                       "release_date VARCHAR(10),"
                       "run_time INT(4),"
                       "FOREIGN KEY(movie_id) REFERENCES movies(movie_id)"
                       ")")
    create_table(cursor, movie_technical)

    # Table 5
    movie_production = ("CREATE TABLE movie_production ("
                        "movie_id INT(6),"
                        "original_language VARCHAR(2),"
                        "production VARCHAR(225),"
                        "country VARCHAR(50),"
                        "director VARCHAR(50),"
                        "FOREIGN KEY(movie_id) REFERENCES movies(movie_id),"
                        "FULLTEXT(country)"
                        ")")
    create_table(cursor, movie_production)

    # Index 1
    idx_popularity = "CREATE INDEX idx_popularity ON movies_rating(popularity)"
    create_index(cursor, idx_popularity)

    # Index 2
    idx_vote_average = "CREATE INDEX idx_vote_average ON movies_rating(vote_average)"
    create_index(cursor, idx_vote_average)

    # Index 3
    idx_director = "CREATE INDEX idx_director ON movie_production(director)"
    create_index(cursor, idx_director)

    # Index 4
    idx_production = "CREATE INDEX idx_production ON movie_production(production)"
    create_index(cursor, idx_production)

    # Index 5
    idx_language = "CREATE INDEX idx_language ON movie_production(original_language)"
    create_index(cursor, idx_language)

    connection.close()
