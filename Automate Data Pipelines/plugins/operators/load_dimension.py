from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 query_sql,
                 truncate_table_flag,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.query_sql = query_sql
        self.truncate_table_flag = truncate_table_flag

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')

        # Connect to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Connect to {self.redshift_conn_id}")

        if self.truncate_table_flag == 'Y':
            self.log.info("Truncate table")
            query = f"""
                TRUNCATE TABLE 
                {self.table_name} 
            """
            self.log.info("Query completed")

        query = f"""
            INSERT INTO 
            {self.table_name} 
            {self.query_sql }
        """

        self.log.info(f"Query: {query}")
        redshift_hook.run(query)
        self.log.info(f"Query completed")
