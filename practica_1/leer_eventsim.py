# In[] Libraries and env variables
#import os   
import pandas as pd
import psycopg2
import json
import numpy as np

host='localhost'
user='catedra'
password='S3cret'
port=5432
database='catedra'
filename='listen_events.txt'
filename_1='auth_events.txt'
filename_2='page_view_events.txt'
filename_3='status_change_events.txt'

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
        try:
            lista.append(dic[campo])
        except:
            lista.append(None)
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
    info_1=leer(filename_1)
    info_2=leer(filename_2)
    info_3=leer(filename_3)
    
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
    trackId=crear_listas(info,'trackId')
    
    #***auth
    ts_1=crear_listas(info_1,'ts');sessionId_1=crear_listas(info_1,'sessionId')
    level_1=crear_listas(info_1,'level');itemInSession_1=crear_listas(info_1,'itemInSession')
    city_1=crear_listas(info_1,'city');zip_1=crear_listas(info_1,'zip')
    state_1=crear_listas(info_1,'state');userAgent_1=crear_listas(info_1,'userAgent')
    lon_1=crear_listas(info_1,'lon');lat_1=crear_listas(info_1,'lat')
    userId_1=crear_listas(info_1,'userId');lastName_1=crear_listas(info_1,'lastName')
    firstName_1=crear_listas(info_1,'firstName');gender_1=crear_listas(info_1,'gender')
    registration_1=crear_listas(info_1,'registration');success_1=crear_listas(info_1,'success')
    
    #***page_view
    ts_2=crear_listas(info_2,'ts');sessionId_2=crear_listas(info_2,'sessionId')
    level_2=crear_listas(info_2,'level');itemInSession_2=crear_listas(info_2,'itemInSession')
    city_2=crear_listas(info_2,'city');zip_2=crear_listas(info_2,'zip')
    state_2=crear_listas(info_2,'state');userAgent_2=crear_listas(info_2,'userAgent')
    lon_2=crear_listas(info_2,'lon');lat_2=crear_listas(info_2,'lat')
    userId_2=crear_listas(info_2,'userId');lastName_2=crear_listas(info_2,'lastName')
    firstName_2=crear_listas(info_2,'firstName');gender_2=crear_listas(info_2,'gender')
    registration_2=crear_listas(info_2,'registration');page_2=crear_listas(info_2,'page')
    auth_2=crear_listas(info_2,'auth');method_2=crear_listas(info_2,'method')
    status_2=crear_listas(info_2,'status');trackId_2=crear_listas(info_2,'trackId')
    artist_2=crear_listas(info_2,'artist');song_2=crear_listas(info_2,'song')
    duration_2=crear_listas(info_2,'duration')
    
    #**status_change_events
    ts_3=crear_listas(info_3,'ts');sessionId_3=crear_listas(info_3,'sessionId')
    level_3=crear_listas(info_3,'level');itemInSession_3=crear_listas(info_3,'itemInSession')
    city_3=crear_listas(info_3,'city');zip_3=crear_listas(info_3,'zip')
    state_3=crear_listas(info_3,'state');userAgent_3=crear_listas(info_3,'userAgent')
    lon_3=crear_listas(info_3,'lon');lat_3=crear_listas(info_3,'lat')
    userId_3=crear_listas(info_3,'userId');lastName_3=crear_listas(info_3,'lastName')
    firstName_3=crear_listas(info_3,'firstName');gender_3=crear_listas(info_3,'gender')
    registration_3=crear_listas(info_3,'registration')
    auth_3=crear_listas(info_3,'auth')
    

    
    #Create Dataframes
    listen_events_lista=[sessionId,itemInSession,userId,ts,auth,level,trackId,
                    song,artist,zip2,city,state,userAgent,lon,lat,lastName,firstName,
                    gender,registration,duration]
    listen_events_name=['sessionId','itemInSession','userId','ts','auth','level','trackId',
                        'song','artist','zip','city','state','userAgent','lon','lat',
                        'lastName','firstName','gender','registration','duration'
                        ]
    listen_events=create_df(listen_events_lista, listen_events_name)
    
    #**auth
    auth_events_lista=[ts_1,sessionId_1,level_1,itemInSession_1,city_1,zip_1,state_1,
                       userAgent_1,lon_1,lat_1,userId_1,lastName_1,firstName_1,gender_1,
                       registration_1,success_1]
    auth_events_name=['ts','sessionId','level','itemInSession','city','zip','state',
                      'userAgent','lon','lat','userId','lastName','firstName','gender',
                      'registration','success']
    auth_events=create_df(auth_events_lista, auth_events_name)

    #**page_view
    
    page_view_events_lista=[ts_2,sessionId_2,level_2,itemInSession_2,city_2,zip_2,state_2,
                            userAgent_2,lon_2,lat_2,userId_2,lastName_2,firstName_2,
                            gender_2,registration_2,page_2,auth_2,method_2,status_2,
                            trackId_2,artist_2,song_2,duration_2]
    page_view_events_name=['ts','sessionId','level','itemInSession','city','zip','state',
                           'userAgent','lon','lat','userId','lastName','firstName','gender',
                           'registration','page','auth','method','status','trackId',
                           'artist','song','duration']
    page_view_events=create_df(page_view_events_lista, page_view_events_name)
    
    #*status_change_events
    status_change_events_lista=[ts_3,sessionId_3,level_3,itemInSession_3,city_3,zip_3,
                                state_3,userAgent_3,lon_3,lat_3,userId_3,lastName_3,
                                firstName_3,gender_3,registration_3,auth_3]
    status_change_events_name=['ts','sessionId','level','itemInSession','city','zip',
                               'state','userAgent','lon','lat','userId','lastName',
                               'firstName','gender','registration','auth']
    status_change_events=create_df(status_change_events_lista, status_change_events_name)
    
    #Converts datatypes to the best possible
    auth_events=auth_events.convert_dtypes()
    listen_events=listen_events.convert_dtypes()
    page_view_events=page_view_events.convert_dtypes()
    status_change_events=status_change_events.convert_dtypes()
    
    
    
    #to .csv
    listen_events.to_csv('listen_events.csv', sep=',', encoding='utf-8',index=False)
    auth_events.to_csv('auth_events.csv', sep=',', encoding='utf-8',index=False)
    page_view_events.to_csv('page_views_events.csv', sep=',', encoding='utf-8',index=False)
    status_change_events.to_csv('status_change_events.csv', sep=',', encoding='utf-8',index=False)
    

    
    
    

if __name__ == '__main__':
	main()    
    
    
    
    
    
    
    