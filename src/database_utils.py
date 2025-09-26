# database_utils.py

from sqlalchemy import Engine, text
import pandas as pd

def upsert_to_mysql(df: pd.DataFrame, table_name: str, engine: Engine):
    """
    Idempotently loads data from a DataFrame into MySQL using the "Staging Table" pattern.
    """
    if df.empty:
            print("DataFrame is empty, skipping database load.")
            return
    staging_table_name = f'{table_name}_staging'
    target_table_name = f'{table_name}'

    # Create a connection that will persist for the entire operation.
    with engine.connect() as connection:
        try:
            # Step 1: Bulk-load all data into the temporary staging table.
            df.to_sql(staging_table_name, con=connection, if_exists='replace', index=False)
            print(f"Data loaded into the staging table '{staging_table_name}'.")

            # Step 2: Build and execute the UPSERT query.
            cols = ", ".join([f"`{c}`" for c in df.columns])
            update_clause = ", ".join([f"T.`{c}` = S.`{c}`" for c in df.columns if c not in ['symbol', 'open_time']])

            upsert_query = f"""
            INSERT INTO {target_table_name} ({cols})
            SELECT {cols} FROM {staging_table_name} AS S
            ON DUPLICATE KEY UPDATE {update_clause.replace("T.", f"{target_table_name}.")}
            """

            print("Executing UPSERT from staging table to main table...")
            # Executing the query within a transaction.
            trans = connection.begin()
            connection.execute(text(upsert_query))
            trans.commit()

            print("UPSERT operation completed successfully.")

        except Exception as e:
            print(f"An error occurred during database load: {e}")
            # If an error occurred, rollback the transaction.
            if 'trans' in locals():
                trans.rollback()
        finally:
            # Step 3: Ensure the temporary table is always dropped.
            try:
                connection.execute(text(f"DROP TABLE IF EXISTS {staging_table_name};"))
                print(f"Temporary table '{staging_table_name}' dropped.")
            except Exception as e:
                print(f"Error while dropping the temporary table: {e}")
