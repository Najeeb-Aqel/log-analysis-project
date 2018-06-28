#!/usr/bin/env python3
import psycopg2

# query for the first question
query1 = "select articles.title, count(*) as views from articles join log on log.path like concat('%', articles.slug, '%') where log.status like '%OK%' group by articles.title order by views desc limit 3"

# query for the second question
query2 = "select authors.name , count(*) as views from articles join log on log.path like concat('%', articles.slug, '%') join authors on articles.author = authors.id where log.status like '%OK%' group by authors.name order by views desc"

#query for the third question
query3 = (
    "select day, perc from ("
    "select day, round((sum(badlogs)/"
    "(select count(*) from log where substring(cast(log.time as text), 1, 10) = day)"
    "* 100), 2) as perc from"
    "(select substring(cast(log.time as text), 1, 10) as day, count(*) as badlogs from log where status like '%404%' group by day)"
    "as log_percentage group by day order by perc desc)"
    "as final_query where perc >= 1")

# fetching results of the query 
def execute_query(query):
    db = psycopg2.connect("dbname=news")
    cursor = db.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    db.close()
    return result


def print_query_1(results):
    print ("What are the most popular three articles of all time?")
    for i, entry in enumerate(results):
        print (str(i + 1) + ")" + entry[0] + "\t" + "--" + str(entry[1]) + " views")


def print_query_2(results):
    print ("Who are the most popular article authors of all time?")
    for i, entry in enumerate(results):
        print (str(i + 1) + ")" + entry[0] + "\t" + "--" + str(entry[1]) + " views")


def print_query_3(results):
    print ("On which days did more than 1% of requests lead to errors?")
    for entry in enumerate(results):
    print ( str(entry[0]) + "\t" + "--" + str(entry[1]) + "% errors")

# getting results for all the three queries 
query1_results = execute_query(query1)
query2_results = execute_query(query2)
query3_results = execute_query(query3)

# printing results for all the three queries 
print_query_1(query1_results)
print_query_2(query2_results)
print_query_3(query3_results)








    


