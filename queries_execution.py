import queries_db_script as queries
import mysql.connector as mc

# import api_data_retrieve as api
# import create_db_script as create

if __name__ == "__main__":
    connection = mc.connect(host='localhost',
                            user='ayaschechner',
                            password='ayasc77989',
                            db='ayaschechner',
                            port=3305)
    cursor = connection.cursor()

    # no need to run this, DB is already loaded
    # create.main()
    # api.main()

    q1_result = queries.query_1('Drama', 'Italy, Germany')
    print("query 1-", q1_result)
    q2_result = queries.query_2()
    print("query 2-", q2_result)
    q3_result = queries.query_3('Pixar Animation Studios')
    print("query 3-", q3_result)
    q4_result = queries.query_4()
    print("query 4-", q4_result)
    q5_result = queries.query_5()
    print("query 5-", q5_result)
    q6_result = queries.query_6()
    print("query 6-", q6_result)
