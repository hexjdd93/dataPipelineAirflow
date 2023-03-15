from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_sql_command = '''
    INSERT INTO {table_name}
    {sql_statement}
    '''

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(LoadFactOperator.insert_sql_command.format(table_name=self.table, sql_statement=SqlQueries.songplay_table_insert))
        redshift.run(LoadFactOperator.insert_sql_command.format(table_name=self.table, sql_statement=SqlQueries.songplay_table_insert))
        
        
