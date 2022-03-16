from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn='',
                 quality_check=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn=redshift_conn
        self.quality_check=quality_check

    def execute(self, context):
        redshift_hook=PostgresHook(self.redshift_conn)
        
        #comparison operator code was based on the following code
        #https://knowledge.udacity.com/questions/552650
        
        for check in self.quality_check:
            records=redshift_hook.get_records(check['quality_check'])[0]
            if check['comparison']=='!=':
                if records[0] != check['expected_value']:
                    raise ValueError(f"{check['table']} table FAILED {check['check_type']} test")
            elif check['comparison']=='<':
                if records[0] < check['expected_value']:
                    raise ValueError(f"{check['table']} table FAILED {check['check_type']} test")
            elif check['comparison']=='=':
                if records[0] == check['expected_value']:
                    raise ValueError(f"{check['table']} table FAILED {check['check_type']} test")
            elif check['comparison']=='>':
                if records[0] > check['expected_value']:
                    raise ValueError(f"{check['table']} table FAILED {check['check_type']} test") 
            self.log.info(f"{check['table']} table PASSED {check['check_type']} test")    
            
            
    
    
   
            
                       