# ANCHOR: connect
import taosrest

def create_connection():
    conn = None
    try:
        conn = taosrest.connect(url="http://localhost:6041",
                                user="root",
                                password="taosdata",
                                timeout=30)
    except Exception as err:
        print(err)
    finally:
        if conn:
            conn.close() 
# ANCHOR_END: connect

if __name__ == "__main__":
    create_connection()

