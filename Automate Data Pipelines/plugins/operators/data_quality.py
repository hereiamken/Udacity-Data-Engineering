from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 data_quality_check,
                 redshift_conn_id,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.data_quality_check = data_quality_check
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')

        # Connect to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Connect to {self.redshift_conn_id}")

        if len(self.data_quality_check) == 0:
            self.log.info(f"No quality check provided")
            return

        error_count = 0
        failed_logs = []

        for item in self.data_quality_check:
            query = item.get('check_sql')
            expected = item.get('expected')

            try:
                self.log.info(f"Query: {query}")
                record = redshift_hook.get_records(query)[0]
            except Exception as e:
                self.log.info(f"Query failed: {e}")

            if expected != record[0]:
                error_count += 1
                failed_logs.append(query)

        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failed_logs)
            raise ValueError('Data quality check failed')
        else:
            self.log.info('Tests completed')
