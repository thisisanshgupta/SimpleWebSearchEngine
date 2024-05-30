import sqlite3

class dataAccess:

    #parameter database is database file path
    def __init__(self, database):
        self.database = database

    #creates connection to database and returns connection object
    def create_connection(self):
        try:
            conn = sqlite3.connect(self.database)
            return conn
        except Error as e:
            print(e)

        return None

    #select data
    def selectCommand(self, query):
        con = self.create_connection()
        cur = con.cursor()
        try:
            cur.execute(query)
            rows = cur.fetchall()
        except Exception as e:
            print(e)
            return None
        finally:
            cur.close()
            con.close()
        return rows

    #insert/update data
    def executeCommand(self, query, row):
        con = self.create_connection()
        cur = con.cursor()
        try:
            cur.execute(query, row)
            con.commit()
            return cur.lastrowid
        except Exception as e:
            print(e)
            return None
        finally:
            cur.close()
            con.close()