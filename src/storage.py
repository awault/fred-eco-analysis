# src/storage.py

from sqlalchemy import text

def does_table_exist(connection, table_name):
    """
    Checks if the specified table exists in the current database.

    Args:
        connection: The database connection object
        table_name (str): The name of the table to check.

    Returns:
        bool: Returns True if the table exists, otherwise False.

    """
    query = text("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = :table_name)
                 
                """)
    result = connection.execute(query, {'table_name': table_name})
    return result.scalar()


def create_table(connection, table_name, table_definition):
    """
    Creates a table in the database if it does not already exist.

    Args:
        connection: The database connection object.
        table_name (str): The name of the table to create.
        table_definition(str): The SQL query to create the table.

    Returns:
        None
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {table_definition}
        );
        """
    
    # Execute the query to create the table if it does not exist
    connection.execute(text(create_table_query))
    print(f"\nTable '{table_name}' checked/created successfully.")