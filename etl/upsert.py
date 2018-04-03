import psycopg2


class Upsert:
    PRIMARY_KEY_FIELD = 'id'

    def __init__(self, staging_table, target_table):
        self.staging_table = staging_table
        self.target_table = target_table

    def process(self, redshift_conn):
        redshift_cursor = redshift_conn.cursor()
        qry = self.get_upsert_queries()
        try:
            redshift_cursor.execute(qry)
            redshift_conn.commit()
        except psycopg2.DatabaseError as e:
            if redshift_conn:
                redshift_conn.rollback()
            print(e)
            pass
        finally:
            if redshift_conn:
                redshift_conn.close()

    def get_upsert_queries(self):
        qry = """
        BEGIN TRANSACTION;
        DELETE FROM {target_table} WHERE {primary_key_field} IN (SELECT DISTINCT {primary_key_field} FROM {staging_table});
        INSERT INTO {target_table} (select * from {staging_table});
        TRUNCATE TABLE {staging_table};
        END TRANSACTION;
        """.format(target_table=self.target_table,
                   primary_key_field=self.PRIMARY_KEY_FIELD,
                   staging_table=self.staging_table)
        return qry
