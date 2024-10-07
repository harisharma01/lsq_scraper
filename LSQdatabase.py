import mysql.connector
import pandas as pd
import sys, os
from sqlalchemy import create_engine
import urllib
from prettytable import PrettyTable
import time
import numpy as np
from mysql_queries import SequelQueries
from datetime import date

class lsqDB:
    def __init__(self, debug_logger, info_logger):

        self.debug_logger = debug_logger
        self.info_logger = info_logger
        self.lsq_database_logs = PrettyTable()
        self.lsq_database_logs.field_names = []

        self.database_host  = os.getenv('DB_HOST')
        self.database_user  =  os.getenv('DB_USER')
        self.database_password =  os.getenv('DB_PASSWORD')
        self.database_name = os.getenv('DB_NAME')

    def connect_sql(self):
        try:
            self.conn = mysql.connector.connect(host = self.database_host,
                                                user = self.database_user,
                                                password = self.database_password,
                                                database = self.database_name
            )

            self.crs = self.conn.cursor()
            password= urllib.parse.quote_plus(self.database_password)
            conn_str = f'mysql+mysqlconnector://{self.database_user}:{password}@{self.database_host}/{self.database_name}'
            self.alchemy_engine = create_engine(conn_str)
            self.current_table_headers_lsq = []
            self.current_table_headers_summary = []

            self.folder_name = 'LSQ'
            self.LSQ_leads_table_name = 'LSQ_Leads'
            self.college_id_map_table = 'CollegeId_Name'
            self.summary_table ='SummaryData'
            self.todays_date = date.today().strftime('%Y-%m-%d')
            self.info_logger.info(f"TODAYSSSS DATEEE = >{self.todays_date}")

            self.sql_queries = SequelQueries(self.conn, self.crs, self.debug_logger, self.info_logger)
            self.check_n_clear_table() #if the table is not find then the create table function is being called in this funtion itself
            time.sleep(1)
            self.add_cols_college_names()
            return self.conn #to close the connection after the script has run executing
            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for Establishing Database Connection \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    def db_req_data(self, college_id, college_name, leads_data, summary_data):
        try:
            self.college_id = college_id
            self.college_name = college_name
            self.leads_data = leads_data
            if leads_data is not None:
                self.leads_headers_data = leads_data.columns
            self.summary_data = summary_data
            if summary_data is not None:
                self.summary_headers = summary_data.columns

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for {self.college_id}:{self.college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')


    def check_n_clear_table(self):
        try:
            table_name_tup = self.sql_queries.get_tables()  #getting tables name
            table_name_li = [i[0] for i in table_name_tup]            
            tables = [self.LSQ_leads_table_name, self.college_id_map_table, self.summary_table]

            for tab_ in tables:
                if str(tab_) not in table_name_li:
                    self.sql_queries.create_table(tab_)
                else:
                    self.sql_queries.truncate_table(tab_)
            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for {self.college_id}:{self.college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')
        
    def add_cols_college_names(self):
        
        table_to_check = self.college_id_map_table
        extra_columns = []
        try:
            column_names_tuple = self.sql_queries.get_columns(self.database_name, self.college_id_map_table)
            self.current_table_headers = [col[0] for col in column_names_tuple] #converting to list
            columns_to_add = ['College_id','College_Name','Panel','ScrapingDate']
            extra_columns = list(filter(lambda x: x not in self.current_table_headers, columns_to_add))

            if len(extra_columns ) != 0:
                self.sql_queries.add_columns(table_to_check, extra_columns)  #calling query to add the columns here

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for {self.college_id}:{self.college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    def headers_check_lsq(self): #define the table name here, wherever this function is being called
        
        table_to_check = self.LSQ_leads_table_name
        extra_columns = [] #making extra columns null here
        try:
            column_names_tuple = self.sql_queries.get_columns(self.database_name, table_to_check)
            self.current_table_headers = [col[0] for col in column_names_tuple] #converting to list
            extra_columns = list(filter(lambda x: x not in (self.current_table_headers), self.leads_headers_data))
            if len(extra_columns ) != 0:
                self.sql_queries.add_columns(table_to_check, extra_columns)  #calling query to add the columns here

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for {self.college_id}:{self.college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    def headers_check_summary(self): #define the table name here, wherever this function is being called
        
        table_to_check = self.summary_table
        extra_columns = [] #making extra columns null here
        try:
            column_names_tuple = self.sql_queries.get_columns(self.database_name, table_to_check)
            self.current_table_headers = [col[0] for col in column_names_tuple] #converting to list

            extra_columns = list(filter(lambda x: x not in (self.current_table_headers), self.summary_headers))
            if len(extra_columns ) != 0:
                self.sql_queries.add_columns(table_to_check, extra_columns)  #calling query to add the columns here

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for {self.college_id}:{self.college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')

    def fill_leadsdata_db(self):
        start_time = time.time()

        for retry in range(3):
            try:
                self.dataframe_ = pd.DataFrame(self.leads_data, columns = self.leads_headers_data)
                self.dataframe_['College_id'] = self.college_id  #adding college id along with the database
                # self.dataframe_.to_csv(f'Dump_{self.college_name}.csv')
                self.dataframe_ = self.dataframe_.astype(str)
                self.dataframe_.to_sql(self.LSQ_leads_table_name, self.alchemy_engine, if_exists='append', index = False, chunksize=10000)
                break

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                self.debug_logger.critical(f'An Exception Occured for {self.college_id}:{self.college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')
                if retry == 2:
                    raise
                time.sleep(3)

        cur_time = time.time()
        self.time_taken_fld = cur_time - start_time
        return np.round(self.time_taken_fld,3)

    def fill_summary_data(self):

        for retry in range(3):
            try:
                self.dataframe_ = pd.DataFrame(self.summary_data)
                self.dataframe_['College_id'] = self.college_id  #adding college id along with the summary data
                self.dataframe_ = self.dataframe_.astype(str)
                # self.dataframe_.to_csv(f'Summary_{self.college_name}.csv')
                self.dataframe_.to_sql(self.summary_table, self.alchemy_engine, if_exists='append', index = False)
                break

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                self.debug_logger.critical(f'An Exception Occured for {self.college_id}:{self.college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')
                if retry == 2:
                    raise
                time.sleep(3)

    def handle_no_leads_colleges(self):

        start_time = time.time()
        for retry in range(3):
            try:
                self.leads_dataframe = pd.DataFrame()  #pushing empty dataframe to keep the record
                self.leads_dataframe['College_id'] = [self.college_id] #adding college id along with the database
                self.leads_dataframe = self.leads_dataframe.astype(str)
                self.leads_dataframe.to_sql(self.LSQ_leads_table_name, self.alchemy_engine, if_exists='append', index = False, chunksize=5000)
                break

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                self.debug_logger.critical(f'An Exception Occured for {self.college_id}:{self.college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')
                if retry == 2:
                    raise
                time.sleep(3)
        cur_time = time.time()
        self.time_taken_fld = cur_time - start_time
        return np.round(self.time_taken_fld,3)

    def handle_no_summary_colleges(self):

        for retry in range(3):

            try:
                self.summary = pd.DataFrame()  #pushing empty dataframe to keep the record
                self.summary['College_id'] = [self.college_id] #adding college id along with the database
                self.summary = self.summary.astype(str)
                self.summary.to_sql(self.summary_table, self.alchemy_engine, if_exists='append', index = False)
                break
            
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                self.debug_logger.critical(f'An Exception Occured for {self.college_id}:{self.college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')
                if retry == 2:
                    raise
                time.sleep(3)

    def fill_college_id_name(self):

        try:
            self.sql_queries.fill_data(table=self.college_id_map_table, 
                                       columns_tuple='(College_id, College_Name, Panel, ScrapingDate)',
                                    #    values_tuple=f"('{self.college_id}','{self.college_name}')")
                                       values_tuple=(self.college_id,self.college_name, self.folder_name, self.todays_date))
            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            self.debug_logger.critical(f'An Exception Occured for {self.college_id}:{self.college_name}: \n{e} \n More Info: Type :{exc_type} Function :{fname} Line Number: {exc_tb.tb_lineno}')