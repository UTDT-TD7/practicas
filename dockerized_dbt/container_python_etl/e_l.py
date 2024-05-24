#Objective of this script: perform both, e_l and etl jobs
# In[] Libraries and env. variables
import pandas as pd
import psycopg2
import random


#env_variables
host_1='postgres_e';host_2='postgres_l';port='5432';database='postgres'
#NUNCA hagan esto (hardcodear claves)
user='codestack';password='systems'

#Extract query
limit_1=int(random.random()*100)

column_names_e=['sessionid','iteminsession','userid','ts','auth','level','trackid',
                'song','artist','zip','city','state','useragent','lon','lat','lastname',
                'firstname','gender','registration', 'duration']

extract_query="SELECT * FROM listen_events OFFSET "+str(limit_1)+" ROWS FETCH NEXT "+str(limit_1+10)+" ROWS ONLY"



#Insert query
names=str(column_names_e).replace('[','').replace(']','').replace("'","")
symbol=str(['%s,']*20).replace('[','').replace(']','').replace("'","").replace(",,",",")
symbol=symbol[0:len(symbol)-1]
insert_query="INSERT INTO listen_events (" +names+") VALUES " +"("+symbol+")"

#Para el etl
insert_query_etl="INSERT INTO etl_dump (sessionid, iteminsession, userid, ts, trackid, zip,len,ban,muestreo ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

# In[] Functions

#Function that connects to a postgres-db
def connectionSQL(host,database,user,password,port):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port)
        print('Connection to database was succesful'+ '\n')
    except:
        print('ERROR, CANNOT CONNECT TO DATABASE'+ '\n')
    return conn

#Function to perform a query
def SQL_query(host,database,user,password,port,query,column_names):
    conn=connectionSQL(host,database,user,password,port)
    #Create a cursor element
    cur = conn.cursor()
    # Perform a query
    cur.execute(query)
    # Retrieve query results
    records = cur.fetchall()
    
    #Retrieve something?
    if len(records)>0:
        #Transform to DataFrame
        df2 = pd.DataFrame(records)
        #Transformations: the right headers+ everything as a string+ no nans
        df2.columns=column_names
        df2=df2.fillna('')#names=list(df2.columns)
        df2=df2.convert_dtypes()
        #for nombre in names:
        #    df2[nombre]=df2[nombre].astype(str)
        print('Query completed')
    else:
        df2=pd.DataFrame(columns=column_names) 
        print('query failed, df empty')
    
    return df2

#Function to batch-insert some registers
def executemany_query_result_set(host,database,user,password,port,sql,data):
    print ("execute_query_result_set()")
    con=connectionSQL(host,database,user,password,port)
    try:
        cursor = con.cursor()
        cursor.executemany(sql, data)
        #results = cursor.fetchall()
        #return results
        con.commit()
    finally:
        pass
        if con != "":
            con.close()
            
#Function to transcribe an entire df to a db table
def transcripcion_completa(host,database,user,password,port,sql,data):
    #We already have the query, now, we have to generate the records
    #The records
    records=[]
    for index,registro in data.iterrows():
        record=[]
        for columna in data.columns:
            record.append(registro[columna])
        records.append(tuple(record))

    #Now, the upload
    try:
        executemany_query_result_set(host,database,user,password,port,sql,records)
        print('Inserts performed wihout fail'+ '\n')
    except:
        print('ERROR, INSERTS COULDN`T BE DONE'+ '\n')



###NUEVOS REQUERIMIENTOS:
    #Arizona dejó de censurar cancones
    #Cambiar el nomre del atributo de 'muestreo' a 'sampleo'
    #Ahora se considera larga a una canción a partir del minuto 400


#Transformations
#3 transformaciones: una lógica de duración de canciones, una lógica de estados
#y la extracción de un año.
def transf(df):
    #Logica de canciones, menos de 200 es corta, hasta 450 es mediana y mas de 450 es larga
    df['len'] = ['short' if x <200  else 'medium' if x<=600 else 'large'  for x in df['duration']]

    #Logica de estados, California y Arizona censuran canciones
    df['ban'] = ['warn_flag' if x =='CA' or x=='AZ' else 'normal'  for x in df['state']]
    
    #Flag_para muestre
    df['muestreo'] = ['sample' if x %10==0 else 'not_sample'  for x in df['userid']]
    
    
    
    #Devolvemos solo lo que nos interesa
    df=df[['sessionid','iteminsession','userid','ts','trackid',
                    'zip','len','ban','muestreo']]
    
    return df


# In[] Main

def main():
    #Perform the extraction
    df=SQL_query(host_1,database,user,password,port,extract_query,column_names_e)
    
    #Perform the first batch_insert
    transcripcion_completa(host_2,database,user,password,port,insert_query,df)
    
    #transform
    df_transf=transf(df)
    
    #Perform the batch inserts
    transcripcion_completa(host_2,database,user,password,port,insert_query_etl,df_transf)
    
    

if __name__ == "__main__":
	main()








