import mysql.connector
from RangeShardManger import RangeShardManger

connections = [
    mysql.connector.connect(
        host="localhost",
        user="root",
        password="Admin1234",
        port=6603,
        database="con1"

    ),
    mysql.connector.connect(
        host="localhost",
        user="root",
        password="Admin1234",
        port=6603,
        database="con2"


    ),
   mysql.connector.connect(
        host="localhost",
        user="root",
        password="Admin1234",
        port=6603,
        database="con3"
    ),
    mysql.connector.connect(
        host="localhost",
        user="root",
        password="Admin1234",
        port=6603,
        database="con4"
    ),
]

# table query CREATE TABLE user (
#       id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
#       name VARCHAR(30) NOT NULL,
#       hash_id VARCHAR(30) NOT NULL
# )

rangeShardManger = RangeShardManger(connections)

def insertUser(name) : 
    connection , hash_value = rangeShardManger.getNode(name)

    mycursor = connection.cursor()

    sql = "INSERT INTO user (name, hash_id) VALUES (%s, %s)"
    val = (name, hash_value )
    mycursor.execute(sql, val)

    connection.commit()
    print(mycursor.rowcount, "record inserted.")


def getUser(name) : 
    connection , hash_value = rangeShardManger.getNode(name)

    mycursor = connection.cursor()

    sql = "Select * from user where name = %s"
    val = (name,)
    mycursor.execute(sql, val)

    myresult = mycursor.fetchall()

    for x in myresult:
        print(x)




insertUser('name')
getUser('name')
