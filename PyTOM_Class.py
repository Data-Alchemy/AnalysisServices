import sys
import ssas_api
import pandas as pd
import datetime
import re
import json

## required for connect ##
ssas_api._load_assemblies()

###########################################################################
########################## Pandas Settings ################################
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
###########################################################################


def aas_server_list() -> dict:
     return{
        'dev' : '',
        'qa'  : '',
        'prod': ''
    }

class PyTOM():

    def __init__(self,db_name,usr,pwd,process_type,server,query='Normal',days_back:int =1):
        self.db_name        = db_name
        self.usr            = usr
        self.pwd            = pwd
        self.process_type   = process_type
        self.server         = server
        self.aas_server_list= aas_server_list()
        self.query          = query
        self.days_back      = days_back



    def validate_parms(self):
         return {'db_name'          : self.db_name,
                 'usr'              : self.usr,
                 'pwd'              : self.pwd,
                 'process_type'     : self.process_type,
                 'server'           : self.server
                 }

    def Connection_String(self):

        aas_server_list = self.aas_server_list
        powerbi_server_list = {
        }
        try:
            self.conn = ssas_api.set_conn_string(
                server=aas_server_list[f'{self.server}'],
                db_name=self.db_name,
                username=self.usr,
                password=self.pwd
            )

            return self.conn
        except Exception as e:
            return f"connection to model failed \n Error received {e}:"


    def Query_Metadata(self)->dict:
        import System
        from System.Data import DataTable
        import Microsoft.AnalysisServices.Tabular as TOM
        import Microsoft.AnalysisServices.AdomdClient as ADOMD
        TOMServer = TOM.Server()
        TOMServer.Connect(self.Connection_String())
        table_list = []
        table_columns = []
        measure_list = []
        partition_list = []
        refresh_list = []

        for item in TOMServer.Databases:
            AASDB = TOMServer.Databases[item.ID]
            for table in AASDB.Model.Tables:
                current_table = AASDB.Model.Tables.Find(table.Name)
                for column in current_table.Columns:
                    table_columns.append({'table_name': table.Name, 'column_name': column.Name})
                for partition in current_table.Partitions:
                    partition_list.append(
                        {'database name': item.ID, 'table name': table.Name, 'partition name': partition.Name,
                         'Partition refresh date': partition.RefreshedTime})
                for measure in current_table.Measures:
                    measure_list.append({'database': item.ID, 'table': table.Name, 'measure name': measure.Name,
                                         'measureexpression': measure.Expression})
            #################################################################################################
        p = pd.DataFrame(partition_list)
        m = pd.DataFrame(measure_list)
        c = pd.DataFrame(table_columns)

        return {'partitions':p,'measures':m,'columns':c}

    ############################################################################
    ##custom functions based on naming convention i implemented for partitions##
    ##       \/ remove if your partitions don't have date in the name \/      ##
    ############################################################################

    def partition_date(self,table, name):
        table = str(table).lower()
        if 'fact' in table and len(re.findall(r'\d', name)) == 8:
            return datetime.datetime.strptime(name[-10:], '%Y-%m-%d')
        else:
            return None

    def Refresh_Query(self) -> json:


        self.Partitions                             = self.Query_Metadata()['partitions']
        self.Partitions['partition_date']           = self.Partitions.apply(lambda x: self.partition_date(x['table name'],x['partition name']),axis = 1)
            #self.vector(self.Partitions['table name'], self.Partitions['partition name'])
        self.Partitions['Partition refresh date']   = pd.to_datetime(self.Partitions['Partition refresh date'], format='%Y-%m-%d %H:%M:%S %p')
        self.Partitions.sort_values(
            by=['database name',
                'partition_date',
                'table name',
                'Partition refresh date']
            ,ascending                              =[True, True, True, True], inplace=True)
        #print(self.Partitions.dtypes)

        ##################################################
        ### filter partition query to spec for refresh ###
        filter1 = self.Partitions['database name']                                              == self.db_name                                                                          # if several models exist filter to specific model
        filter2 = self.Partitions['Partition refresh date']                                     == datetime.datetime.strptime('1699-12-31 12:00:00 AM','%Y-%m-%d %H:%M:%S %p')           # never been refreshed
        filter3 = self.Partitions['partition_date'].isnull()                                                                                                                             # if partition has no date in the name (aka dim type tables)
        filter4 = self.Partitions['partition_date']                                             >= datetime.datetime.strftime(
                datetime.datetime.now() - datetime.timedelta(days=(self.days_back), hours=int(
                datetime.datetime.strftime(datetime.datetime.now(), '%H')), minutes=int(
                datetime.datetime.strftime(datetime.datetime.now(), '%M')), seconds=int(
                datetime.datetime.strftime(datetime.datetime.now(), '%S'))), '%Y/%m/%d %H:%M:%S %p')                                        # partitions after given date
        filter5 = self.Partitions['partition_date']                                             <= datetime.datetime.strftime(
                datetime.datetime.now() - datetime.timedelta(days=(self.days_back)     , hours  =int(
                datetime.datetime.strftime(datetime.datetime.now(), '%H')) , minutes=int(
                datetime.datetime.strftime(datetime.datetime.now(), '%M')) , seconds=int(
                datetime.datetime.strftime(datetime.datetime.now(), '%S'))), '%Y/%m/%d %H:%M:%S %p')                                        # partitions before today
        recovery_filter = self.Partitions['table name'].astype(str).str.contains(self.query)                                                # for selecting specific partitions when data needs to be recovered
        if self.query == 'Normal' or len(self.query)==0:
            self.refresh_query = self.Partitions[filter1 & ((( filter2 & filter5) ) | filter3)]
        else:
            self.refresh_query = self.Partitions[filter1 &recovery_filter & ((filter4 & filter5)|filter3)]                                                                  # recovery query for failures or issues


        ######################################################
        ########### build refresh query ######################
        refresh_list = []

        for index, row in self.refresh_query.iterrows():
            refresh_list.append({"table": row['table name'], "partition": row['partition name']})
        final_output = json.dumps(refresh_list, indent=4)
        return final_output
##################################################################################################################
