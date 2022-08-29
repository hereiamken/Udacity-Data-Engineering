from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 s3_bucket,
                 s3_path,
                 aws_key,
                 aws_secret,
                 region,
                 copy_json_option,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.region = region
        self.copy_json_option = copy_json_option

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')

        # Connect to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Connected to {self.redshift_conn_id}")

        query = f"""
            copy {self.table_name} 
            from 's3://{self.s3_bucket}/{self.s3_path}' 
            access_key_id '{self.aws_key}'
            secret_access_key '{self.aws_secret}' 
            region {self.region} 
            format as json {self.copy_json_option} 
            timeformat as 'epochmillisecs'
        """

        self.log.info(f"Query: {query}")
        redshift_hook.run(query)
        self.log.info("Query completed")
