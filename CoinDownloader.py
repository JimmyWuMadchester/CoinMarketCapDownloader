import coinmarketcapclient
import repo

def main():
    '''entry point'''
    
    finished_list = repo.get_coindownloader_log('2017-08-10')['CoinSymbol'].values

    df_coinsymbols = repo.get_allcoinsymbols()
    for symbol in df_coinsymbols['symbol']:

        if (symbol in finished_list):
            continue

        print symbol
        df_history = coinmarketcapclient.get_df_full_history_usd(symbol)
        if df_history is None:
            continue
        tuples = [tuple(x) for x in df_history.values]
        for row in tuples:
           repo.add_coin_history(row)
    
    #df_history = coinmarketcapclient.get_df_full_history_usd('420G')
    #if not df_history:
    #    return

    #tuples = [tuple(x) for x in df_history.values]
    #for row in tuples:
    #    repo.add_coin_history(row)
    
    
if __name__ == "__main__":
    main()