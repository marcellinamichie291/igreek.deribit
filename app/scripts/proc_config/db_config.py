import mysql.connector


def get_db_connection():
    try:
        _conn = mysql.connector.connect(
            host="192.168.122.57",
            user="user",
            password="123456",
            database="igreek",
        )
        if _conn.is_connected():
            return _conn
        else:
            print("MySQL Not connected")
            return None
    except Exception as e:
        print("An exception occurred: " + str(e))
        return None
