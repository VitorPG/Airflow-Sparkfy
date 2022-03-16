from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    insert_sql=" INSERT INTO {} {}"
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn='',
                 table='',
                 overwrite='', 
                 sql='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn=redshift_conn
        self.table=table
        self.overwrite=overwrite
        self.sql=sql
        

    def execute(self, context):
        self.log.info('Establishing connection to redshift')
        redshift_hook=PostgresHook(self.redshift_conn)
        
        if self.overwrite:
            self.log.info(f'Deleting previous data from {self.table} table')
            redshift_hook.run(f'DELETE FROM {self.table}')
            
        self.log.info(f'Loading {self.table} data to dimension table')
        
        formated_sql=LoadDimensionOperator.insert_sql.format(self.table,self.sql)
        redshift_hook.run(formated_sql)
        self.log.info('Load Complete')