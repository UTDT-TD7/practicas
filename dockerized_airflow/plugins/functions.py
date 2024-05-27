#Functions
import pandas as pd
import psycopg2

#Function that connects to a postgres-db
def connectionSQL(host,database,user,password,port):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port)
    print('The connection to the DB was succesful')
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
        print('Inserts were comitted')
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
    executemany_query_result_set(host,database,user,password,port,sql,records)



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