# In[] Libraries and env variables
#import os   
import pandas as pd
import psycopg2
import json

host='localhost'
user='catedra'
password='S3cret'
port=5432
database='eventsim'
filename='file.txt'

# In[] Functions

#Function to perform a connection to a SQL DB
def connectionSQL(host,database,user,password,port):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port)
    return conn

#Function which disconnects you from the SQL DB
def disconnectSQL(conn):
    conn.close()
    
def executemany_query_result_set(sql,data,host,database,user,password,port):
    print ("execute_query_result_set()")
    con = connectionSQL(host,database,user,password,port)

    try:
        cursor = con.cursor()
        cursor.executemany(sql, data)
        #results = cursor.fetchall()
        #return results
        con.commit()
    finally:
        if con != "":
            con.close()
            
            
#Function to transcribe an entire df to a DB in Postgres
def transcripcion_completa(sql,data,host,database,user,password,port):
    #We already have the query, now, we have to generate the records
    #The records
    records=[]
    for index,registro in data.iterrows():
        record=[]
        for columna in data.columns:
            record.append(registro[columna])
        records.append(tuple(record))

    #Now, the upload
    executemany_query_result_set(sql,records,host,database,user,password,port)

#Function that reads a json-like txt and returns a list of dictionaries
def leer(filename):
    #First read
    m=[]
    with open(filename, 'r') as fh:
        for line in fh:
                m.append(line.strip('\n'))
                
    #2nd read and json of the objects 
    m2=[]
    for line in m:
        if line!='':
            m2.append(json.loads(line))
    return m2

#Function that extracts every dictionary info and stores it in a list
def crear_listas(dics,campo):
    lista=[]
    for dic in dics:
        lista.append(dic[campo])
    return lista
        
#Function that creates a df using lists
def create_df(lista_lista,lista_nombres):
    dc={}
    for i in range(len(lista_lista)):
        dc[lista_nombres[i]]=lista_lista[i]
    return pd.DataFrame(dc)

# In[] Flux

def main():
    #Read info
    info=leer(filename)
    
    #Create lists
    artist=crear_listas(info,'artist');song=crear_listas(info,'song');ts=crear_listas(info,'ts')
    duration=crear_listas(info,'duration');sessionId=crear_listas(info,'sessionId')
    auth=crear_listas(info,'auth');level=crear_listas(info,'level')
    ts=crear_listas(info,'ts');sessionId=crear_listas(info,'sessionId')
    level=crear_listas(info,'level');itemInSession=crear_listas(info,'itemInSession')
    city=crear_listas(info,'city');zip2=crear_listas(info,'zip');
    state=crear_listas(info,'state');userAgent=crear_listas(info,'userAgent')
    lon=crear_listas(info,'lon');lat=crear_listas(info,'lat')
    userId=crear_listas(info,'userId');lastName=crear_listas(info,'lastName')
    firstName=crear_listas(info,'firstName');gender=crear_listas(info,'gender')
    registration=crear_listas(info,'registration')
    #song_id=crear_listas(info,'song_id')
    
    #Create Dataframes
    sessions_lista=[sessionId,itemInSession,userId,ts,auth,level,userAgent]#song_id
    sessions_name=['sessionId','itemInSession','userId','ts','auth','level','userAgent']
    sessions=create_df(sessions_lista, sessions_name)
    songs_lista=[song,artist,duration]#song_id
    songs_name=['song','artist','duration']
    songs=create_df(songs_lista, songs_name)
    users_lista=[userId,firstName,lastName,gender,registration]
    users_name=['userId','firstName','lastName','gender','registration']
    users=create_df(users_lista, users_name)
    location_lista=[zip2,state,city,lat,lon]
    location_name=['zip','state','city','lat','lon']
    location=create_df(location_lista, location_name)
    
    #insert to postgres
    query_insert_1="INSERT INTO public.location (zip,state,city,lat,lon) VALUES (%s,%s,%s,%s,%s)"   
    transcripcion_completa(query_insert_1,location,host,database,user,password,port) 
    query_insert_2="INSERT INTO public.users (userId,firstName,lastName,gender,registration) VALUES (%s,%s,%s,%s,%s)"   
    transcripcion_completa(query_insert_2,users,host,database,user,password,port) 

if __name__ == '__main__':
	main()    
    
    
    
    
    
    
    