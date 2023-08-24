import mysql.connector as sql
import pandas as pd
import warnings
#import psycopg2.extras as extras

warnings.simplefilter(action='ignore', category=UserWarning)

#only sqlDF and df_to_sql were updated for postgres move

host : str = '######'
database : str = '######'
user : str = '######'
port : int = 3306
password : str  =  '######'


def sqlDF(query):
    try:
        connection = sql.connect(host=host,
                                             database=database,
                                             user=user,
                                             port = port,
                                             password = password,
                                             )
        result=pd.read_sql(query,connection)

        
        #print("Command executed")

    except sql.DatabaseError as error:
        print("Failed to execute: {}".format(error))
    finally:
        if connection is not None:
            connection.close()
            #print("MySQL connection is closed")
    return result


def sqlExecute(query):
    try:
        connection = sql.connect(host=host,
                                             database=database,
                                             user=user,
                                             port = port,
                                             password = password,
                                             )
        cursor = connection.cursor()

        cursor.execute(query)
        
        connection.commit()
        
        #print("Command executed")

    except (Exception, sql.DatabaseError) as error:
        print("Error: %s" % error)
        connection.rollback()
        cursor.close()
        return 1
    
    finally:
        if connection is not None:
            cursor.close()
            #print("MySQL connection is closed")
            
            
def sqlInsert(table_name,columns,values,database='nathanproj'):
    """values is a list of tuples containing values for each column,
    insert statement is for the format "INSERT INTO customers (name, address) VALUES (%s, %s)" """
    try:
        connection = sql.connect(host=host,
                                             database=database,
                                             user=user,
                                             port = port,
                                             password = password,
                                             )
        mycursor = connection.cursor()
        
        cols=str(tuple(columns)).replace("'",'')
        
        query=f'INSERT INTO {table_name} {cols} VALUES (%s, %s)'
        
        mycursor.executemany(query,values)
        
        connection.commit()
        
        #print("Insert Successful")

    except sql.Error as error:
        print("Failed to execute: {}".format(error))
    finally:
        if connection.is_connected():
            connection.close()
            #print("MySQL connection is closed")
            
def df_to_sql(table_name,df):
    
    try:
        connection = sql.connect(host=host,
                                              database=database,
                                              user=user,
                                              port = port,
                                              password = password,
                                              )
        mycursor = connection.cursor()
        
        cols = "`,`".join([str(i) for i in df.columns.tolist()])

        for i,row in df.iterrows():
            query = f"INSERT INTO {table_name} (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            mycursor.execute(query, tuple(row))
        
        connection.commit()
        
        #print("Insert Successful")

    except sql.Error as error:
        print("Failed to execute: {}".format(error))
    finally:
        if connection is not None:
            connection.close()
           # print("MySQL connection is closed")

# def df_to_sql(table_name,df):
    
#     connection = sql.connect(host=host,
#                                          database=database,
#                                          user=user,
#                                          port = port,
#                                          password = password,
#                                          )
    
#     tuples = [tuple(x) for x in df.to_numpy()]
  
#     cols = ','.join(list(df.columns))
#     # SQL query to execute
#     query = "INSERT INTO %s(%s) VALUES %%s" % (table_name, cols)
#     cursor = connection.cursor()
#     try:
#         extras.execute_values(cursor, query, tuples)
#         connection.commit()
#         print("the dataframe is inserted")
#     except (Exception, sql.DatabaseError) as error:
#         print("Error: %s" % error)
#         connection.rollback()
#         cursor.close()
#         return 1
    
#     finally:
#         if connection is not None:
#             cursor.close()
#             print("MySQL connection is closed")
            
            
            
            
            
            