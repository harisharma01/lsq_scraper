#need to introduce roll back to all the queries 
import sys, os
class SequelQueries:

    def __init__(self, connection, cursor, debug_logger, info_logger):

        self.conn = connection
        self.crs = cursor

        self.debug_logger = debug_logger
        self.info_logger = info_logger

    def execute_commit(self, query):

        self.crs.execute(query)
        self.conn.commit()

    def fetch_commit(self, query):

        self.crs.execute(query)
        data = self.crs.fetchall()
        self.conn.commit()
        return data
                
    def create_table(self,table_name):

        try:
            query = f"""CREATE TABLE {table_name}(
                                College_id varchar(255) default NULL
            )
                                    """
            self.execute_commit(query= query)
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.info_logger.critical(f'An Exception Occured for Establishing Database Connection \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    def get_tables(self):

        try:
            query = 'show tables'
            tables_name_tuple = self.fetch_commit(query=query)
            return tables_name_tuple
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.info_logger.critical(f'An Exception Occured for Establishing Database Connection \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    def truncate_table(self, table_name):
        try:
            query = f"""truncate table {table_name}"""
            self.execute_commit(query=query)
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.info_logger.critical(f'An Exception Occured for Establishing Database Connection \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    def get_columns(self, database_name,table):

        try:
            query = f"""
                                SELECT 
                                COLUMN_NAME 
                                FROM INFORMATION_SCHEMA.COLUMNS 
                                WHERE TABLE_NAME = '{table}'
                                AND
                                TABLE_SCHEMA = '{database_name}'
                                """
            
            columns = self.fetch_commit(query=query)
            return columns
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.info_logger.critical(f'An Exception Occured for Establishing Database Connection \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')
        
    def add_columns(self, table, extra_columns):

        try:
            # columns_string = ', '.join([f'`{cols}` VARCHAR(256) default NULL' for cols in extra_columns ])
            # query = f'Alter table {table} add ' + f'({columns_string})'
            # self.execute_commit(query)
            columns = []
            for col in extra_columns:
                if col == 'Remarks':
                    columns.append(f'`{col}` VARCHAR(1000) default NULL')
                else:
                    columns.append(f'`{col}` VARCHAR(256) default NULL')

            columns_string = ', '.join(columns)
            query = f'ALTER TABLE {table} ADD ({columns_string})'
            self.execute_commit(query)
            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.info_logger.critical(f'An Exception Occured for Establishing Database Connection \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    def fill_data(self, table,columns_tuple,values_tuple):
        
        try:
            query = f"""Insert INTO {table} {columns_tuple}
            VALUES {values_tuple}"""
            self.execute_commit(query=query)
        
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.info_logger.critical(f'An Exception Occured for Establishing Database Connection \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')