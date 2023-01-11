import pyodbc
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus

def to_ms_sql(data, table_name,DATABASE,SERVER,PWD,UID):
    #conn = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=12345Muh@"
    conn = "DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={0};DATABASE={1};UID={2};PWD={3}".format(
        SERVER, DATABASE, UID, PWD)

    connection = pyodbc.connect(conn)
    cursor = connection.cursor()
    quoted = quote_plus(conn) #conn
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = create_engine(new_con)


    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        print("FUNC call")
        if executemany:
            cursor.fast_executemany = True


    data.to_sql(table_name, engine, if_exists = 'replace', chunksize = None)
    print("data was uploaded to db")

# """
#
# def write_df_to_sql(df, **kwargs):
#     chunks = np.split(df, df.shape()[0] / 10**6)
#     for chunk in chunks:
#         chunk.to_sql(**kwargs)
#     return True
# """
