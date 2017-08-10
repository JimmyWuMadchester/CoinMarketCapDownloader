import pyodbc
import pandas as pd

def get_db_cursor():
    server = 'tcp:coin.database.windows.net,1433'
    database = 'MyCoinDb'
    username = 'jimmy_ic@coin'
    password = '123qweASDF'
    driver= '{ODBC Driver 13 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    return cursor

def get_allcoinsymbols():
    sql = """\
        SELECT  Id,
                symbol,
                ticker,
                history,
                last14Days FROM dbo.CoinMktCapSymbol
    """
    cursor = get_db_cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    columnnames = [column[0] for column in cursor.description]
    df = pd.DataFrame.from_records(rows, columns=columnnames)
    
    return df


def add_coin_history(history):
    sql = """\
        EXEC dbo.Stp_HistoryDaily_Add	@date = ?,          
										@position = ?,         
										@name = ?,           
										@symbol = ?,         
										@category = ?,       
										@marketCap = ?,      
										@price = ?,          
										@availableSupply = ?,
										@volume24 = ?,       
										@change1h = ?,       
										@change24h = ?,      
										@change7d = ?        
    """
    cursor = get_db_cursor()
    cursor.execute(sql, history)
    cursor.commit()

def get_coindownloader_log(date):
    sql = """
        SELECT  Id,
                Timestamp,
                CoinSymbol,
                Action
        FROM    dbo.CoinDownloaderLog CDL
        WHERE   CONVERT(date, CDL.Timestamp) = ?
    """
    cursor = get_db_cursor()
    cursor.execute(sql, date)
    rows = cursor.fetchall()
    columnnames = [column[0] for column in cursor.description]
    df = pd.DataFrame.from_records(rows, columns=columnnames)
    return df