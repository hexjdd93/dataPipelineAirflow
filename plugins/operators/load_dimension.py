from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    command =""
    as_command =''
    sql_statement=''

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 new_docs=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.new_docs = new_docs

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if (self.table == "users"):
            self.sql_statement=SqlQueries.user_table_insert
        elif (self.table == "songs"):
            self.sql_statement=SqlQueries.song_table_insert
        elif (self.table == "artists"):
            self.sql_statement=SqlQueries.artist_table_insert
        elif (self.table == "time"):
            self.sql_statement = SqlQueries.time_table_insert
        else:
            raise ValueError("table {} is not valid".format(self.table))
        
        

        if (self.new_docs):
            self.log.info('DROP {}'.format(self.table))
            redshift.run('DROP TABLE IF EXISTS {};'.format(self.table))
            self.command="CREATE TABLE"
            self.as_command="AS"
            self.log.info("{} {} {} {}".format(self.command, self.table, self.as_command, self.sql_statement))
            redshift.run("{} {} {} {}".format(self.command, self.table, self.as_command, self.sql_statement))
        else:
            self.command="INSERT INTO"
            self.log.info("{} {} {}".format(self.command, self.table, self.sql_statement))
            redshift.run("{} {} {}".format(self.command, self.table, self.sql_statement))
