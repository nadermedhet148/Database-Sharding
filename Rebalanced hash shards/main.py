import mysql.connector
from PartionsManger import PartionsManger
import math

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


    )
]

# table query CREATE TABLE user (
#       id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
#       name VARCHAR(30) NOT NULL,
#       hash_id VARCHAR(30) NOT NULL
# )


class DBManger:

    def __init__(self,connections):
        self.partionsManger = PartionsManger(connections,1000)

    def insertUser(self,name) :
        connection , hashed_val = self.partionsManger.get_node(name)

        mycursor = connection.cursor()

        sql = "INSERT INTO user (name, hash_id) VALUES (%s, %s)"
        val = (name, hashed_val )
        mycursor.execute(sql, val)

        connection.commit()
        print(mycursor.rowcount, "record inserted.")


    def getUser(self,name) :
        connection , hashed_val = self.partionsManger.get_node(name)


        mycursor = connection.cursor()

        sql = "Select * from user where name = %s"
        val = (name,)
        mycursor.execute(sql, val)

        myresult = mycursor.fetchall()

        for x in myresult:
            print(x[0] , x[1] , x[2])

    def add_new_node(self , node):
        moved_partions = self.partionsManger.add_node(node)
        for node_moved_partions in moved_partions:
            node = moved_partions.get(node_moved_partions)
            for partion in node.get('partions'):
                self.move_data_between_node(node.get('connection') , partion.get('new_node') , partion.get('hashed_value'))

    def move_data_between_node(self,old_node , new_node , partion_id):
        # get data from old_node
        mycursor = old_node.cursor()

        sql = "Select * from user where hash_id = %s"
        val = (partion_id,)
        mycursor.execute(sql, val)
        result = mycursor.fetchall()
        # move data to new node
        for x in result:   
            mycursor = new_node.cursor()
            sql = "INSERT INTO user (name, hash_id) VALUES (%s, %s)"
            val = (x[1], x[2] )
            mycursor.execute(sql, val)

        # delete data from the old new
        delete_cursor = old_node.cursor()
        sql = "DELETE FROM user WHERE hash_id = " + str(partion_id)
        delete_cursor.execute(sql)
        old_node.commit()
        print(delete_cursor.rowcount, "record(s) deleted")



dbManger = DBManger(connections)


dbManger.insertUser('TEST')
dbManger.getUser('TEST')

dbManger.add_new_node(   
     mysql.connector.connect(
        host="localhost",
        user="root",
        password="Admin1234",
        port=6603,
        database="con4"
    ))