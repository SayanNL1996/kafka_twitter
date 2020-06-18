import mysql.connector


connection = mysql.connector.connect(host='localhost',
                                     database='kafka_twitter',
                                     user='root',
                                     password='Qwerty@123')
conn = connection.cursor()