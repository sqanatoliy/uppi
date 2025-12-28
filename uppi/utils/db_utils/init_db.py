import psycopg
from decouple import config

def execute_sql_file(filename, db_config):
    try:
        # 1. Підключаємося до бази даних
        conn = psycopg.connect(**db_config)
        cursor = conn.cursor()
        
        print(f"Читання файлу {filename}...")
        
        # 2. Відкриваємо та зчитуємо SQL-файл
        with open(filename, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # 3. Виконуємо скрипт
        print("Виконання SQL-запитів...")
        cursor.execute(sql_script)
        
        # 4. Фіксуємо зміни (Commit)
        conn.commit()
        print("Схему успішно створено!")
        
    except Exception as e:
        print(f"Помилка при виконанні: {e}")
        if conn:
            conn.rollback()
    finally:
        # 5. Закриваємо з'єднання
        if cursor:
            cursor.close()
        if conn: 
            conn.close()

# Налаштування підключення (заміни на свої дані)
config_db = {
    "dbname": config("DB_NAME", default="uppi_db"),
    "user": config("DB_USER", default="uppi_user"),
    "password": config("DB_NAME", default="uppi_password"),
    "host": config("DB_HOST", default="localhost"),
    "port": config("DB_PORT", default="5432"),
}

if __name__ == "__main__":
    execute_sql_file('uppi/utils/db_utils/uppi_schema.sql', config_db)