import pandas as pd
import mysql.connector as mc
import ast
import math
import warnings

# Path to the CSV file on your computer
file_path1 = 'movies_metadata.csv'
file_path2 = 'credits.csv'

# Ignore DtypeWarning
warnings.filterwarnings("ignore", category=FutureWarning)

# Read the CSV file into a DataFrame
movies_df = pd.read_csv(file_path1)
credit_df = pd.read_csv(file_path2)

# Set display options to show all columns
pd.set_option('display.max_columns', None)

add_movie = ("INSERT INTO movies "
             "(movie_id, title, genre) "
             "VALUES (%(movie_id)s, %(title)s, %(genre)s)")

add_actor = ("INSERT INTO actors "
             "(name,movie_id) "
             "VALUES (%(name)s, %(movie_id)s)")

add_rating = ("INSERT INTO movies_rating "
              "(movie_id, popularity, vote_count, vote_average) "
              "VALUES (%(movie_id)s, %(popularity)s, %(vote_count)s, %(vote_average)s)")

add_technical = ("INSERT INTO movie_technical "
                 "(movie_id, release_date, run_time) "
                 "VALUES (%(movie_id)s, STR_TO_DATE(%(release_date)s, '%Y-%m-%d'), %(run_time)s)")

add_production = ("INSERT INTO movie_production "
                  "(movie_id, original_language, production, country, director) "
                  "VALUES (%(movie_id)s, %(original_language)s, %(production)s, %(country)s, %(director)s)")


def make_movies_table():
    rows = []
    count = 0
    seen = set()
    for index, row in movies_df.iterrows():
        if count < 5000:
            id = row['id']
            if id not in seen:
                seen.add(id)
                title = row['title']
                genre = row['genres']
                genre = ast.literal_eval(genre)
                if genre:
                    genre = genre[0]['name']
                if genre == []:
                    genre = ''
                if id is not None and title is not None and genre is not None:
                    rows.append([int(id), title, genre])
                    count += 1
    answer = pd.DataFrame(rows, columns=['id', 'title', 'genre'])
    return answer


def make_actors_table(movies):
    rows = []
    for index, row in credit_df.iterrows():
        movie_id = row['id']
        if str(movie_id) in movies['id'].astype(str).values:
            actors_list = row['cast']
            actors_list = ast.literal_eval(actors_list)
            actors_num = 0
            for actor_dict in actors_list:
                if actors_num < 3:
                    actor_name = actor_dict['name']
                    if actor_name is not None and movie_id is not None:
                        rows.append({'name': str(actor_name), 'movie_id': int(movie_id)})
                        actors_num += 1

    answer = pd.DataFrame(rows)
    return answer


def make_ratings_table(movies):
    rows = []
    seen = set()
    for index, row in movies_df.iterrows():
        movie_id = row['id']
        if movie_id not in seen:
            if str(movie_id) in movies['id'].astype(str).values:
                popularity = row['popularity']
                vote_count = row['vote_count']
                vote_average = row['vote_average']
                if movie_id is not None and popularity is not None and vote_count is not None and vote_average is not None:
                    rows.append({'id': int(movie_id), 'popularity': float(popularity), 'vote_count': float(vote_count),
                                 'vote_average': float(vote_average)})
                    seen.add(movie_id)
    answer = pd.DataFrame(rows)
    return answer


def make_technical_table(movies):
    rows = []
    seen = set()
    for index, row in movies_df.iterrows():
        movie_id = row['id']
        if movie_id not in seen:
            if str(movie_id) in movies['id'].astype(str).values:
                release_date = row['release_date']
                if type(release_date) == float:
                    release_date = '1111-11-11'
                runtime = row['runtime']
                if movie_id is not None and release_date is not None and runtime is not None and not math.isnan(
                        runtime):
                    rows.append({'id': int(movie_id), 'release_date': release_date, 'run_time': int(runtime)})
                    seen.add(movie_id)
    answer = pd.DataFrame(rows)
    return answer


def make_production_table(movies):
    rows1 = []
    seen1 = set()
    for index, row in movies_df.iterrows():
        movie_id = row['id']
        if movie_id not in seen1:
            if str(movie_id) in movies['id'].astype(str).values:
                original_language = row['original_language']
                countries_dict = row["production_countries"]
                production_dict = row["production_companies"]
                if not pd.isnull(production_dict):
                    production_dict = ast.literal_eval(production_dict)
                if not pd.isnull(countries_dict):
                    countries_dict = ast.literal_eval(countries_dict)
                if type(countries_dict) == float:
                    country = 'nan'
                else:
                    if countries_dict:
                        country = countries_dict[0]['name']
                    elif type(countries_dict) == float:
                        country = 'nan'
                if type(production_dict) == float:
                    production = 'nan'
                else:
                    if production_dict:
                        production = production_dict[0]['name']
                    elif type(production_dict) == float:
                        production = 'nan'
                if movie_id is not None and original_language is not None:
                    rows1.append({'id': int(movie_id), 'original_language': original_language, 'production': production,
                                  'country': country})
                    seen1.add(movie_id)
    answer = pd.DataFrame(rows1)
    answer['director'] = ''

    seen2 = set()
    for index, row in credit_df.iterrows():
        movie_id = row['id']
        if movie_id not in seen2:
            if str(movie_id) in answer['id'].astype(str).values:
                crew = row['crew']
                crew = ast.literal_eval(crew)
                director = 0
                for dict in crew:
                    if dict['job'] == 'Director':
                        director = dict['name']
                if director == 0:
                    director = 'nan'
                answer.loc[answer['id'] == int(movie_id), 'director'] = director
                seen2.add(movie_id)
    return answer


def make_all_tables_df():
    movies_df2 = make_movies_table()
    actors_df2 = make_actors_table(movies_df2)
    ratings_df2 = make_ratings_table(movies_df2)
    technical_df2 = make_technical_table(movies_df2)
    production_df2 = make_production_table(movies_df2)

    return movies_df2, actors_df2, ratings_df2, technical_df2, production_df2


def insert_movies(movies):
    for i in range(movies.shape[0]):
        movie_id = int(movies['id'][i])
        title = movies['title'][i]
        genre = movies['genre'][i]

        data_movie = {
            'movie_id': movie_id,
            'title': title,
            'genre': genre,
        }

        cursor.execute(add_movie, data_movie)


def insert_actors(actors):
    for i in range(actors.shape[0]):
        name = actors['name'][i]
        movie_id = int(actors['movie_id'][i])

        data_actor = {
            'name': name,
            'movie_id': movie_id,
        }

        cursor.execute(add_actor, data_actor)


def insert_ratings(ratings):
    for i in range(ratings.shape[0]):
        movie_id = int(ratings['id'][i])
        popularity = float(ratings['popularity'][i])
        vote_count = float(ratings['vote_count'][i])
        vote_average = float(ratings['vote_average'][i])

        data_rating = {
            'movie_id': movie_id,
            'popularity': popularity,
            'vote_count': vote_count,
            'vote_average': vote_average,
        }

        cursor.execute(add_rating, data_rating)


def insert_technicals(technicals):
    for i in range(technicals.shape[0]):
        movie_id = int(technicals['id'][i])
        release_date = technicals['release_date'][i]
        run_time = float(technicals['run_time'][i])

        data_technical = {
            'movie_id': movie_id,
            'release_date': release_date,
            'run_time': run_time,
        }

        cursor.execute(add_technical, data_technical)


def insert_productions(productions):
    for i in range(productions.shape[0]):
        movie_id = int(productions['id'][i])
        original_language = productions['original_language'][i]
        production = productions['production'][i]
        country = productions['country'][i]
        director = productions['director'][i]

        data_production = {
            'movie_id': movie_id,
            'original_language': original_language,
            'production': production,
            'country': country,
            'director': director,
        }
        cursor.execute(add_production, data_production)


if __name__ == "__main__":
    connection = mc.connect(host='localhost',
                            user='ayaschechner',
                            password='ayasc77989',
                            db='ayaschechner',
                            port=3305)
    cursor = connection.cursor()

    movies_df1, actors_df1, ratings_df1, technical_df1, production_df1 = make_all_tables_df()
    insert_movies(movies_df1)
    insert_actors(actors_df1)
    insert_ratings(ratings_df1)
    insert_technicals(technical_df1)
    insert_productions(production_df1)

    connection.commit()
    cursor.close()
    connection.close()
