########################################################################################################################
                                                #Imports#
########################################################################################################################
from helper_database import *
from helper_config import *
from helper_dataholder import *
from datetime import datetime
import pandas as pd
import holidays
import os
########################################################################################################################
                                             #Error Handling#
########################################################################################################################
def validate_config(cfg: Config, quarter: str = None):
    if 'Table' not in cfg.hold or cfg.hold['Table'] is None:
        raise ValueError('La section des tables est vide ou non enregistrée')

    if quarter is not None:
        if 'QuarterlyExpo' not in cfg.hold:
            raise ValueError("Le fichier config n'a pas de section 'QuarterlyExpo'")

        if quarter not in cfg.hold['QuarterlyExpo'] or cfg.hold['QuarterlyExpo'][quarter] is None:
            raise ValueError(f"Le fichier config n'a pas les valeurs trimestrielles enregistrées pour {quarter}")
    pass
def validate_portfolio(ptf_id: pd.DataFrame, ptf_name: str):
    if 'portfolio_code' not in ptf_id.columns:
        raise IndexError(f"Colonnes disponibles: {ptf_id.columns}")
    if ptf_name not in ptf_id['portfolio_code'].values:
        raise ValueError(f"Nom de fond inexistant, vérifiez la syntaxe de {ptf_name}")
    pass
########################################################################################################################
                                          #Repository Functions#
########################################################################################################################
def get_view(cfg:Config,db_name:str = 'OpenData',s_query:str = None) -> pd.DataFrame:
    result = pd.DataFrame()
    with Database(config = cfg.hold['DataBase'], name = db_name) as SQLserver:
        SQLserver.query = s_query
        for chunk in SQLserver.execute_query():
            chunk_df = pd.DataFrame(chunk)
            chunk_df = chunk_df.drop_duplicates()
            result = pd.concat([result,chunk_df],ignore_index = True)
    return result

def get_cache_file(dth:DataHolder,ptf_name:str,quarter:str):
    files_names = f"{ptf_name.upper()}_METRICS_{quarter}_*.pkl"
    cache_files = list(dth.cache_dir.glob(files_names))

    if not cache_files:
        return None

    try:
        cache_files.sort(key=lambda x: datetime.strptime('_'.join(x.stem.split('_')[-2:]), "%Y%m%d_%H-%M-%S"),
                         reverse=True)
        recent_file_name = cache_files[0].stem

        return dth.load(name=recent_file_name)

    except ValueError as ve:
        raise DataHolderError(f"Erreur lors du tri des fichiers, vérifiez encodage de date et heure: {ve}")
    except Exception as e:
        raise DataHolderError(f"Erreur lors du chargement du fichier: {e}")

def valide_date(date: datetime):
    year = int(date.year)
    fr_holidays = holidays.France(years=year)
    index_date = date.weekday()

    # Vérification si la date est un week-end ou un jour férié
    while index_date >= 5 or date in fr_holidays:
        if index_date == 6:
            day_spread = 2
        else:
            day_spread = 1
        # Ajustement de la date
        date = date - pd.Timedelta(days=day_spread)
        index_date = date.weekday()

    return date

def price_EoQ(cfg:Config,date:str,db_name:str,mode:str,sec_ids:list):
    validate_config(cfg)
    table = cfg.hold['Table']
    columns = cfg.hold['Columns']
    date = valide_date(datetime.strptime(date, '%Y-%m-%d')).strftime('%Y-%m-%d')

    s_query = (f"SELECT {columns['SQL_Query']['Price_EoQ']}"
               f"FROM (SELECT {columns['SQL_Query']['Price_EoQ']}, "
               f"ROW_NUMBER() OVER (PARTITION BY [oxygen_security_id] ORDER BY [value_date] DESC) as rn "
               f"FROM {table['Price_Security']} "
               f"WHERE [value_date] <= '{date}' "
               f"AND [oxygen_security_id] IN ({','.join(map(str,sec_ids))}) "
               f"AND [price_type_code] = 'LAST' "
               f"AND [fixing_code] = 'CLOSE' "
               f"AND [quotation_mode] = '{mode}') subquery "
               f"WHERE rn = 1 "
               f"ORDER BY [value_date] DESC;")

    df_priced = get_view(cfg, db_name, s_query)

    return df_priced

def get_aum (cfg:Config,ptf_id:str,dates:list,db_name:str):
    validate_config(cfg)
    table = cfg.hold['Table']
    columns = cfg.hold['Columns']
    start_date,end_date = [valide_date(datetime.strptime(dt,'%Y-%m-%d')).strftime('%Y-%m-%d') for dt in dates]
    s_query  = (f"SELECT {columns['SQL_Query']['Get_aum']} "
                f"FROM {table['Share_Quote']} "
                f"WHERE [oxygen_portfolio_id] = '{ptf_id}' "
                f"AND [quote_date] BETWEEN '{start_date}' AND '{end_date}'")
    aum_record = get_view(cfg,db_name,s_query)
    return aum_record

def get_ptf_isr(cfg:Config,ptf_id:str,dates:list,db_name:str):
    validate_config(cfg)
    table = cfg.hold['Table']
    columns = cfg.hold['Columns']
    start_date,end_date = [valide_date(datetime.strptime(dt,'%Y-%m-%d')).strftime('%Y-%m-%d') for dt in dates]

    s_query = (f"SELECT {columns['SQL_Query']['Get_ptf_isr']} "
               f"FROM {table['Ptf_score']} "
               f"WHERE [oxygen_portfolio_id] = '{ptf_id}' "
               f"AND [value_date] BETWEEN '{start_date}' AND '{end_date}'")
    isr_record = get_view(cfg,db_name,s_query)

    isr_record['value_date'] = pd.to_datetime(isr_record['value_date'], format='%Y-%m-%d')
    isr_record['value_date'] = isr_record['value_date'].dt.strftime('%Y-%m-%d')

    isr_record.rename(columns = {'value_date':'position_date','great_score':'ptf_score'}, inplace = True)
    return isr_record

def get_ptf_pos(cfg:Config,ptf_name:str,quarter:str ,db_name:str = 'OpenData'):

    validate_config(cfg, quarter)
    table = cfg.hold['Table']
    columns = cfg.hold['Columns']
    dates = pd.to_datetime(cfg.hold['QuarterlyExpo'][quarter])
    start_date, end_date = dates.strftime('%Y-%m-%d')
    historical_date = (dates[0] - pd.Timedelta(days=1))
    historical_date = valide_date(historical_date).strftime('%Y-%m-%d')

    # On recupère les données de OPENDATA, puis converti en dataframe
    s_query = (f"SELECT {columns['SQL_Query']['Portfolio_Position']} "
               f"FROM {table['Portfolio_Position']} "
               f"WHERE [portfolio_code] = '{ptf_name.upper()}' "
               f"AND [position_date] BETWEEN '{historical_date}' AND '{end_date}' "
               f"AND [stock_type] = 'TIT' "
               f"ORDER BY [position_date] ASC, [pocket_code] ASC,[oxygen_security_id] ASC;")

    raw_data = get_view(cfg,db_name,s_query)

    # On regroupe les données par plusieurs dimensions et on calcule les métriques agrégées
    raw_data = raw_data.groupby(columns['Dataframes']['POS']).agg(
                        {"quantity":'sum',"clean_price":"first","weight":'sum','weight_exposure':'sum'}).reset_index()
    raw_data['position_date'] = pd.to_datetime(raw_data['position_date']).dt.strftime('%Y-%m-%d')

    # On regroupe par date et par identifiant de titre pour calculer les totaux selon leur type de position
    df_agg = raw_data.groupby(["position_date", "oxygen_security_id"]).agg({"quantity": "sum",
                                                                            "weight": "sum"}).reset_index()

    # Différence des quantités et poids par titre (par rapport à la date précédente)
    df_agg["delta_quantity"] = df_agg.groupby("oxygen_security_id")["quantity"].diff()
    df_agg["delta_weight"] = df_agg.groupby("oxygen_security_id")["weight"].diff()

    # L'active management est définie par des variations significatives dans les quantités et les poids (selon Config)
    df_agg["active_management"] = (((df_agg["delta_quantity"].abs() > 0) &
                                   (df_agg["delta_weight"].abs() > float(cfg.hold['Conditions']['Weight_diff']))) |
                                   (df_agg["delta_weight"].isna() & df_agg["delta_quantity"].isna()))

    # Attribuer à l'active management un statut en fonction des conditions
    df_agg.loc[(pd.isna(df_agg['delta_quantity'])) & (df_agg['quantity'] > 0) & (df_agg["active_management"]), 'statut'] = 'Buy First Appearance'
    df_agg.loc[(pd.isna(df_agg['delta_quantity'])) & (df_agg['quantity'] < 0) & (df_agg["active_management"]), 'statut'] = 'Sell First Appearance'
    df_agg.loc[(df_agg['delta_quantity'] > 0) & (df_agg["active_management"]), 'statut'] = 'Buy'
    df_agg.loc[(df_agg['delta_quantity'] < 0) & (df_agg["active_management"]), 'statut'] = 'Sell'
    df_agg['statut'] = df_agg['statut'].fillna('Held')

    # Remplacer les valeurs manquantes car nous sommes limités sur la temporalité
    df_agg['delta_quantity'] = df_agg['delta_quantity'].fillna('No Historical Value')
    df_agg['delta_weight'] = df_agg['delta_weight'].fillna('No Historical Value')

    # On ajoute les nouvelles colonnes agréegées
    merged_on = ["position_date", "oxygen_security_id","active_management","statut","delta_quantity","delta_weight"]
    data = raw_data.merge(df_agg[merged_on],on=["position_date", "oxygen_security_id"], how="left")

    # On se débarasse de l'avant dernier date du quarter précédent
    data = data[data['position_date'] != historical_date]

    # Creation d'un dictionnaire des données active management
    date_keys = data[data['active_management'] == True]['position_date'].unique().tolist()
    dict_data = {}
    col_dict = {'security_id':'oxygen_security_id','portfolio_id':'oxygen_portfolio_id'}

    for date_key in date_keys:
        dict_data[date_key] = {}
        status_keys = data[(data['statut'] != 'Held') & (data['position_date'] == date_key)]['statut'].unique().tolist()
        for status_key in status_keys:
            dict_data[date_key][status_key] = {}
            for col_key,col_item in col_dict.items():
                item = data[(data['statut'] == status_key) & (data['position_date'] == date_key)][col_item].unique().tolist()
                dict_data[date_key][status_key][col_key] = item

    return {"POS":data,"DATE":[start_date,end_date],"DATA":dict_data}

def get_analytics(cfg:Config,ptf:dict,db_name:str ='OpenData'):
    validate_config(cfg)
    columns = cfg.hold['Columns']
    table = cfg.hold['Table']
    ptf_pos = ptf['POS'][(ptf['POS']['active_management'] == True) &
                         (ptf['POS']['position_type_name'].isin(['Normal / Position en propre','Sous-jacent d une mise en pension']))]
    end_quarter = ptf['DATE'][-1]
    priced_securities = pd.DataFrame()

    sec_ids = ptf_pos['oxygen_security_id'].unique().tolist()
    s_query =(f"SELECT {columns['SQL_Query']['Security_Data']} "
              f"FROM {table['Security_Data']} "
              f"WHERE [oxygen_security_id] IN ({','.join(map(str, sec_ids))}) ")

    securities_info = get_view(cfg,db_name,s_query)
    referential = securities_info.groupby('price_expression')[['price_expression', 'oxygen_security_id']]

    for price_mode,df_grouped in referential:
        securities_id = df_grouped['oxygen_security_id'].unique().tolist()
        df_inter = price_EoQ(cfg,end_quarter,db_name,str(price_mode),securities_id)
        priced_securities = pd.concat([priced_securities, df_inter], ignore_index = True)

    analytics = ptf_pos.merge(priced_securities, on='oxygen_security_id', how='left') \
                       .merge(securities_info, on='oxygen_security_id', how='left')
    analytics = analytics[columns['Dataframes']['Analytics']]

    if analytics['maturity_date'].isnull().all():
        analytics.drop(columns=['maturity_date'], inplace=True)
    else:
        analytics['maturity_date'] = analytics['maturity_date'].fillna('No Maturity')
        analytics['maturity_date'] = analytics['maturity_date'].apply(
            lambda x: x if x == 'No Maturity' else x.strftime('%Y-%m-%d')
        )

    analytics = analytics.rename(columns={'price': 'EoQ_price',
                                          'value_date': 'EoQ_date',
                                          'price_ccy':'currency',
                                          'fx_rate_ptf_vs_price':'fx_rate'}
    )
    #Delta Price
    analytics['delta_price'] = analytics.apply(
        lambda row: row['EoQ_price'] - row['clean_price'] if row['statut'] in ['Buy', 'Buy First Appearance']
        else (row['EoQ_price'] - row['clean_price']) * -1 if row['statut'] in ['Sell', 'Sell First Appearance']
        else None, axis=1
    )
    #Delta P&L
    analytics['delta_pnl'] = analytics.apply(
        lambda row: (abs(row['quantity']) * row['delta_price']) / row['fx_rate'] if row['delta_quantity'] == 'No Historical Value'
        else (abs(row['delta_quantity']) * row['delta_price'] ) / row['fx_rate'], axis=1
    )
    #Position P&L
    position_pnl = analytics.groupby('oxygen_security_id')['delta_pnl'].sum().reset_index()
    position_pnl.columns = ['oxygen_security_id', 'position_pnl']
    analytics = analytics.merge(position_pnl, on = 'oxygen_security_id', how = 'left')

    #Recupère AUM
    ptf_id = analytics['oxygen_portfolio_id'].unique()[0]
    dates = [analytics['position_date'].iloc[0],analytics['position_date'].iloc[-1]]

    aum = get_aum(cfg,ptf_id,dates,db_name)[['quote_date','portfolio_total_aum']]
    aum = aum.rename(columns = {'quote_date':'position_date','portfolio_total_aum':'AUM'})

    # Les dates dans le tableau Share_Quote sont intrepretés par Python en class datetime.date et non datetime.timestamp
    aum['position_date'] = pd.to_datetime(aum['position_date'])
    aum['position_date'] = aum['position_date'].dt.strftime('%Y-%m-%d')
    analytics = analytics.merge(aum, on = 'position_date', how='left')

    # P&L %
    analytics['pnl_pct'] = analytics['delta_pnl'] / analytics['AUM']

    return {"POS":ptf['POS'],"ANALYTICS":analytics,"DATE":ptf['DATE']}

def get_analytics_isr(cfg:Config(),ptf:dict,db_name:str = 'OpenData'):
    validate_config(cfg)

    columns = cfg.hold['Columns']
    table = cfg.hold['Table']

    analytics = ptf['ANALYTICS']
    ptf_id = analytics['oxygen_portfolio_id'].iloc[0]

    dates = ptf['DATE']
    isr_record = get_ptf_isr(cfg, ptf_id, dates, db_name)

    start_date,end_date = [valide_date(datetime.strptime(dt,'%Y-%m-%d')).strftime('%Y-%m-%d') for dt in dates]

    #Formatage des listes ID et Dates
    selected_dates = analytics['position_date'].unique().tolist()
    selected_dates = "','".join(map(str, selected_dates))
    selected_dates = f"'{selected_dates}'"

    sec_id = analytics['oxygen_security_id'].unique().tolist()
    sec_id = "','".join(map(str, sec_id))
    sec_id = f"'{sec_id}'"

    s_query = (f"WITH Security_Score AS ("
               f"SELECT {columns['SQL_Query']['Security_score']} "
               f"FROM {table['Security_score']} "
               f"WHERE [oxygen_security_id] IN ({sec_id}) "
               f"AND [asof_date] BETWEEN '{start_date}' AND '{end_date}') "
               f"SELECT * "
               f"FROM Security_Score "
               f"WHERE [asof_date] IN ({selected_dates})"
               f"GROUP BY [asof_date],[oxygen_security_id],[great_score] "
               f"ORDER BY [asof_date] ASC;")

    isr_security = get_view(cfg, db_name, s_query)

    isr_security['asof_date'] = pd.to_datetime(isr_security['asof_date'], format = '%Y-%m-%d')
    isr_security['asof_date'] = isr_security['asof_date'].dt.strftime('%Y-%m-%d')


    analytics_isr = isr_security.merge(isr_record, left_on = 'asof_date', right_on = 'position_date', how = 'left')
    analytics_isr = analytics_isr[['position_date','oxygen_security_id','great_score','ptf_score']]
    analytics_isr.rename(columns = {'great_score':'security_score'}, inplace = True)
    analytics_isr.fillna('No Value', inplace = True)

    analytics_isr['isr_score_gap'] = analytics_isr.apply(lambda row: row['security_score'] - row['ptf_score']
                                             if row['security_score'] != 'No Value' else 'No Value', axis = 1
    )


    analytics = analytics.merge(analytics_isr, on = ['position_date','oxygen_security_id'], how = 'left')
    df_isr = analytics[columns['Dataframes']['ISR']]
    return {"ISR":df_isr,"ANALYTICS":analytics,"DATE":dates}

def get_analytics_expo(cfg:Config(),ptf:dict,db_name:str = 'OpenData'):
    validate_config(cfg)

    columns = cfg.hold['Columns']
    table = cfg.hold['Table']

    analytics = ptf['ANALYTICS']
    start_date, end_date = ptf['DATE']
    ptf_code = analytics['portfolio_code'].unique().tolist()[0]

    s_query = (f"SELECT {columns['SQL_Query']['Get_Benchmark_Ref']} "
               f"FROM {table['Portfolio_Benchmark']} "
               f"WHERE [portfolio_code] = '{ptf_code}' "
               f"AND [benchmark_type] = 'reporting' ")

    benchmark_ref = get_view(cfg,db_name,s_query)
    bench_id = benchmark_ref['rm_benchmark_id'].tolist()[0]

    s_query = (f"SELECT {columns['SQL_Query']['Security_Benchmark']} "
               f"FROM {table['Security_Benchmark']} "
               f"WHERE [compo_date] BETWEEN '{start_date}' AND '{end_date}' "
               f"AND [benchmark_id] = '{bench_id}' "
               f"ORDER BY [compo_date] ASC;")

    securities_weight = get_view(cfg,db_name,s_query).rename(columns ={'compo_date':'position_date',
                                                                       'benchmark_official_weight':'benchmark_weight'})

    securities_weight['position_date'] = pd.to_datetime(securities_weight['position_date'], format='%Y-%m-%d')
    securities_weight['position_date'] = securities_weight['position_date'].dt.strftime('%Y-%m-%d')

    analytics_expo = analytics.merge(securities_weight, on = ['position_date','oxygen_security_id'], how = 'left')

    analytics_expo['exposition_statut'] = analytics_expo.apply(lambda row: row['weight_exposure'] - row['benchmark_weight']
                                                                      if pd.notna(row['benchmark_weight'])
                                                                      else 'Out of Benchmark', axis=1)

    analytics_expo['benchmark_weight'] = analytics_expo['benchmark_weight'].fillna('Out of Benchmark')

    df_expo = analytics_expo[columns['Dataframes']['Expo']]

    return {'EXPO':df_expo,'ANALYTICS':analytics_expo,'DATE':ptf['DATE']}

def get_analytics_SR(cfg:Config(),ptf:dict,db_name:str = 'OpenData'):
    validate_config(cfg)

    columns = cfg.hold['Columns']
    table = cfg.hold['Table']
    analytics = ptf['ANALYTICS']
    star_date, end_date = ptf['DATE']
    ptf_code = analytics['portfolio_code'].unique().tolist()[0]

    s_query = (f"SELECT {columns['SQL_Query']['Get_SR']} "
               f"FROM {table['SR_Data']} "
               f"WHERE [portfolio_code] = '{ptf_code}' "
               f"AND [quote_date_official] BETWEEN '{star_date}' AND '{end_date}' "
               f"AND ([subscription_amount] IS NOT NULL OR "
               f"[redemption_amount] IS NOT NULL OR "
               f"[total_subscription_redemption_amount] IS NOT NULL) "
               f"ORDER BY [quote_date_official] ASC;")

    SR_df = get_view(cfg,db_name,s_query)

    SR_df.rename(columns={'quote_date_official': 'position_date',
                          'total_subscription_redemption_amount':'total_SR'}, inplace=True)

    SR_df = SR_df.groupby('position_date').agg({'portfolio_code':'first',
                                                'subscription_amount':'sum',
                                                'redemption_amount':'sum',
                                                'total_SR':'sum'}).reset_index()

    SR_df['SR_statut'] = SR_df.apply(lambda row: 'Souscription' if row['total_SR'] > 0 else 'Rachat', axis=1)

    SR_df['position_date'] = pd.to_datetime(SR_df['position_date'], format='%Y-%m-%d')
    SR_df['position_date'] = SR_df['position_date'].dt.strftime('%Y-%m-%d')

    df_merge = SR_df.drop(columns=['portfolio_code'])
    analytics = analytics.merge(df_merge, on = 'position_date', how = 'left')

    return {'SR':SR_df,'ANALYTICS':analytics,'DATE':ptf['DATE']}

def get_metrics(cfg:Config(),dth:DataHolder(),ptf_name:str,quarter:str,db_name:str = 'OpenData'):
    validate_config(cfg,quarter)
    ref_fund = dth.load('referentiel_fond')

    ptf_benchmarked = ref_fund[ref_fund['code_ptf'] == ptf_name.upper()]['Benchmark'].values[0] == 'Benchmarked'

    positions = get_ptf_pos(cfg, ptf_name, quarter, db_name)
    analytics = get_analytics(cfg, positions, db_name)
    isr_scores = get_analytics_isr(cfg, analytics, db_name)

    if ptf_benchmarked:
        expositions = get_analytics_expo(cfg, isr_scores, db_name)
        sr_statut = get_analytics_SR(cfg, expositions, db_name)
        expo = expositions['EXPO']
    else:
        sr_statut = get_analytics_SR(cfg, isr_scores, db_name)
        expo = None

    metrics = sr_statut['ANALYTICS']
    positions = positions['POS']
    analytics = analytics['ANALYTICS']
    isr_scores = isr_scores['ISR']
    SR = sr_statut['SR']
    date = sr_statut['DATE']
    creation_date = datetime.now().strftime("%Y%m%d_%H-%M-%S")

    data_to_save = {
        f'{ptf_name.upper()}_METRICS_{quarter}_{creation_date}': metrics,
        f'{ptf_name.upper()}_POS_{quarter}_{creation_date}': positions,
        f'{ptf_name.upper()}_ANALYTICS_{quarter}_{creation_date}': analytics,
        f'{ptf_name.upper()}_ISR_{quarter}_{creation_date}': isr_scores,
        f'{ptf_name.upper()}_EXPO_{quarter}_{creation_date}': expo,
        f'{ptf_name.upper()}_SR_{quarter}_{creation_date}': SR
    }

    for key, data in data_to_save.items():
        if not dth.exists(key):
            dth.save(key, data)

    return {'METRICS':metrics,'POS':positions,'ANALYTICS':analytics,'ISR':isr_scores,'EXPO':expo,'SR':SR,'DATE':date}

def export_to_excel(dataframe:pd.DataFrame, name:str, quarter:str = None):
    script_dir = os.path.dirname(__file__)
    exit_path = os.path.join(script_dir, 'Resultat')

    if quarter is None:
        file_name = f'{name}.xlsx'
    else:
        file_name = f'{quarter}_{name}.xlsx'

    file_path = os.path.join(exit_path, file_name)

    if not os.path.exists(exit_path):
        os.makedirs(exit_path)

    with pd.ExcelWriter(file_path, engine = 'xlsxwriter') as writer:
        dataframe.to_excel(writer, index = False, sheet_name = name)
        worksheet = writer.sheets[name]

        #Format pour centrer les titres et justifier à gauche
        center_format = writer.book.add_format({'align': 'center', 'valign': 'vcenter'})
        left_format = writer.book.add_format({'align': 'left', 'valign': 'vcenter'})

        for i, col in enumerate(dataframe.columns):
            max_len = max(dataframe[col].astype(str).map(len).max(), len(col)) + 2

            worksheet.write(0, i, col, center_format) # Configuration des titres
            worksheet.set_column(i, i, max_len, left_format) # Ajustement de la largeur de la colonne

    return print(f'Fichier {file_name} exporté avec succès à {file_path}')

########################################################################################################################
                                             #Test Unitaire#
########################################################################################################################

if __name__ == '__main__':
    BIO_metrics = get_metrics(Config(),DataHolder(),'BIO','Q3_2024')

    print(BIO_metrics['METRICS'].dtypes)