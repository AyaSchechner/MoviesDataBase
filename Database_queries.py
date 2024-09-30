import mysql.connector as mc

connection = mc.connect(host='localhost',
                        user='ayaschechner',
                        password='ayasc77989',
                        db='ayaschechner',
                        port=3305)
cursor = connection.cursor()


def exe_q(query):
    cursor.execute(query)
    result = cursor.fetchall()
    return result


# Query 1
def query_1(genre=None, country=None):
    if genre is None or country is None:
        print("must provide genre and country")
        return
    q01 = f"SELECT movies.title, movie_production.country \
            FROM movies, movie_production \
            WHERE movies.movie_id = movie_production.movie_id \
            AND MATCH(movies.genre) AGAINST('{genre}' IN NATURAL LANGUAGE MODE) \
            AND MATCH(movie_production.country) AGAINST('{country}' IN NATURAL LANGUAGE MODE)\
            LIMIT 50"
    return exe_q(q01)


# Query 2
def query_2():
    q02 = f"SELECT movies.title, movie_production.original_language, movies_rating.popularity \
            FROM movies, movies_rating, movie_production \
            WHERE movies.movie_id = movies_rating.movie_id \
            AND movies.movie_id = movie_production.movie_id \
            AND movies_rating.popularity > 10 \
            AND NOT MATCH(movie_production.country) AGAINST('United States of America' IN BOOLEAN MODE) \
            AND movie_production.original_language != 'en'\
            ORDER BY movies_rating.popularity DESC \
            LIMIT 50"
    return exe_q(q02)


# Query 3
def query_3(production=None):
    if production is None:
        print("must provide production company")
        return
    q03 = f"SELECT movies.title, movies_rating.popularity \
            FROM movies \
            INNER JOIN movies_rating ON movies.movie_id = movies_rating.movie_id \
            INNER JOIN movie_production ON movies.movie_id = movie_production.movie_id \
            WHERE movie_production.production = '{production}' \
            AND movies_rating.vote_average > 7 \
            ORDER BY movies_rating.popularity DESC \
            LIMIT 50"
    return exe_q(q03)


# Query 4
def query_4():
    q04 = "SELECT DISTINCT movies.title \
            FROM  movies, actors, (SELECT actors.name, SUM(movie_technical.run_time) AS sum \
                    FROM actors \
                    INNER JOIN movie_technical ON actors.movie_id = movie_technical.movie_id \
                    GROUP BY actors.name \
                    ORDER BY SUM(movie_technical.run_time) DESC \
                    LIMIT 10 \
                    ) AS best_actors \
            WHERE best_actors.name = actors.name AND actors.movie_id = movies.movie_id \
            LIMIT 50"
    return exe_q(q04)


# Query 5
def query_5():
    q05 = "SELECT DISTINCT popular_directors.director \
            FROM movies \
            INNER JOIN movie_production ON movies.movie_id = movie_production.movie_id \
            INNER JOIN( \
                SELECT movie_production.director AS director, AVG(movies_rating.popularity) as avg_popular_director \
                FROM movie_production \
                INNER JOIN movies_rating ON movie_production.movie_id = movies_rating.movie_id \
                GROUP BY movie_production.director \
                ORDER BY avg_popular_director DESC \
                LIMIT 30 \
            )AS popular_directors \
            ON movie_production.director = popular_directors.director"

    return exe_q(q05)


# Query 6
def query_6():
    q06 = "SELECT movie_production.production AS production_company, CAST(AVG(movies_rating.popularity) AS FLOAT) AS avg_popularity \
            FROM movie_production \
            INNER JOIN movies_rating ON movie_production.movie_id = movies_rating.movie_id \
            GROUP BY movie_production.production \
            ORDER BY avg_popularity DESC \
            LIMIT 30"

    return exe_q(q06)
