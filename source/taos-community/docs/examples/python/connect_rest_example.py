# ANCHOR: connect
import taosrest

def create_connection():
    conn = None
    url="http://localhost:6041"
    try:
        conn = taosrest.connect(url=url,
                                user="root",
                                password="taosdata",
                                timeout=30)
        
        print(f"Connected to {url} successfully.");
    except Exception as err:
        print(f"Failed to connect to {url} , ErrMessage:{err}")
    finally:
        if conn:
            conn.close() 
# ANCHOR_END: connect

if __name__ == "__main__":
    create_connection()

