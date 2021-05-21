import ssas_api
import pandas as pd,datetime,numpy,re,json
###########################################################################
########################## Pandas Settings ################################
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
###########################################################################

#place holder for element in tom engine without this you can't switch between single table and all tables setting#
class iter():
    id = ''
item = iter()
##################################################################################################################
#CrawlPhaseSemanticModel
dbname = ''#enter model name
process_options = {'all':'item.id','single':dbname} #options for processing can be all models or single model
usr = "'+open('C:/Python_Automation/usrID.txt').readline()+'" # requires single quote for ssas_api class
pwd = "'"+open('C:/Python_Automation/usrKys.txt').readline()+"'" # requires single quote for ssas_api class
process_type = process_options['single']
aas_server_list = {
                    'dev':'',
                   'qa':'',
                   'prod':''
                  }
powerbi_server_list = {
}

###########################################################################
##################### Capture Groups for output ###########################
table_list = []
measure_list = []
partition_list = []
refresh_list = []

conn = ssas_api.set_conn_string(
server=aas_server_list['dev'],
db_name= dbname,
username= usr,
password=pwd
)

dax_string = '''
//any valid DAX query
EVALUATE
CALCULATETABLE('table_name')
'''
#print(process_type)
#df = ssas_api.get_DAX(connection_string=conn, dax_string=dax_string)
#print(df)


import System
from System.Data import DataTable
import Microsoft.AnalysisServices.Tabular as TOM
import Microsoft.AnalysisServices.AdomdClient as ADOMD

TOMServer = TOM.Server()
TOMServer.Connect(conn)
#print(TOMServer)
for item in TOMServer.Databases:
    AASDB = TOMServer.Databases[item.ID]
    for table in AASDB.Model.Tables:
        current_table = AASDB.Model.Tables.Find(table.Name)
        for partition in current_table.Partitions:
            '''print('Database Size:', item.EstimatedSize)
            print('Model :', item.Model)
            print('ID:', item.ID)
            print("Created: ", item.CreatedTimestamp)
            print("Compatibility Level: ", item.CompatibilityLevel)'''
            partition_list.append({'database name': item.ID, 'table name': table.Name, 'partition name': partition.Name,'Partition refresh date': partition.RefreshedTime})
#################################################################################################
########### this is the measures engine for extracting measures on the aas model ################
        for measure in current_table.Measures:
            measure_list.append({'database': item.ID, 'table': table.Name, 'measure name': measure.Name,'measureexpression': measure.Expression})
#################################################################################################


#### find the next partition that has never been run and build object #######
p =pd.DataFrame(partition_list)
m = pd.DataFrame(measure_list)

def partition_date(table, name):
    table = str(table).lower()
    if 'fact' in table and len(re.findall(r'\d',name))==8:
        return datetime.datetime.strptime(name[-10:],'%Y-%m-%d')
    else:
        return  None

vect = numpy.vectorize(partition_date)
p['partition_date'] = vect(p['table name'],p['partition name'])
p['Partition refresh date'] = pd.to_datetime(p['Partition refresh date'], format='%m/%d/%Y %H:%M:%S %p')
p.sort_values(by=['database name','partition_date','table name','Partition refresh date'], ascending=[True,True,True,True],inplace=True)

filter1 = p['database name']==dbname
filter2 = p['Partition refresh date'] == datetime.datetime.strptime('12/31/1699 12:00:00 AM','%m/%d/%Y %H:%M:%S %p')
filter3 = p['partition_date'].isnull()

final_df =p[filter1 & (filter2 | filter3)]
refresh_input = final_df.groupby('table name').head(1)


######################################################
########### build refresh query ######################


for index, row in refresh_input.iterrows():
    refresh_list.append({"table": row['table name'], "partition": row['partition name']})
final_output =json.dumps(refresh_list,indent=4)


