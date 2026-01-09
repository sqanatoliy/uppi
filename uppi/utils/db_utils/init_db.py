import psycopg
from decouple import config

def execute_sql_file(filename, db_config):
    """
    Виконує SQL-запити з вказаного файлу для ініціалізації схеми бази даних.
    """
    conn = None
    cursor = None
    
    try:
        print(f"Спроба підключення до {db_config['host']}...")
        conn = psycopg.connect(**db_config)
        cursor = conn.cursor()
        
        print(f"Читання файлу {filename}...")
        with open(filename, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        print("Виконання SQL-запитів...")
        cursor.execute(sql_script)
        
        conn.commit()
        print("Схему успішно створено!")
        
    except Exception as e:
        print(f"❌ Помилка при виконанні: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn: 
            conn.close()
        print("З'єднання закрите.")

#  НАЛАШТУВАННЯ БАЗИ ДАНИХ З .ENV ФАЙЛУ
config_db = {
    "dbname": config("DB_NAME", default="uppi_db"),
    "user": config("DB_USER", default="uppi_user"),     
    "password": config("DB_PASSWORD", default="uppi_password"), 
    "host": config("DB_HOST", default="localhost"),
    "port": config("DB_PORT", default="5432"),
    "sslmode": "require" 
}

if __name__ == "__main__":
    execute_sql_file('uppi/utils/db_utils/uppi_schema.sql', config_db)