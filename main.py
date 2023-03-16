#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import pg8000
import awswrangler as wr


def create_date_table(start='1990-01-01', end='2050-12-31'):
    start_ts = pd.to_datetime(start).date()

    end_ts = pd.to_datetime(end).date()

    # record timetsamp is empty for now
    dates =  pd.DataFrame(columns=['Record_timestamp'],
        index=pd.date_range(start_ts, end_ts))
    dates.index.name = 'd_date'

    days_names = {
        i: name
        for i, name
        in enumerate(['Monday', 'Tuesday', 'Wednesday',
                      'Thursday', 'Friday', 'Saturday', 
                      'Sunday'])
    }
    month_names = {
        i: name
        for i, name
        in enumerate(['Monday', 'Tuesday', 'Wednesday',
                      'Thursday', 'Friday', 'Saturday', 
                      'Sunday'])
    }

    dates['n_day'] = dates.index.dayofweek#.map(days_names.get)
    dates['v_dayname'] = dates.index.day_name()
    dates['n_week'] = dates.index.isocalendar().week
    dates['n_month'] = dates.index.month
    dates['v_monthname'] = dates.index.month_name()
    dates['n_quarter'] = dates.index.quarter
    #dates['Year_half'] = dates.index.month.map(lambda mth: 1 if mth <7 else 2)
    dates['n_year'] = dates.index.year
    dates.reset_index(inplace=True)
    dates.index.name = 'n_time_id'
    dates['d_date_id'] = np.arange(0, len(df))
    #dates['Timestamp'] = dates['Date'].dt.timestamp
    dates = dates.drop(columns=['Record_timestamp'])
    
    df2 = df[['n_year']].drop_duplicates()
    df2['n_year_id'] = np.arange(0, len(df2))
    
    df3 = df[['n_month','v_monthname']].drop_duplicates()
    df3['n_month_id']= np.arange(0, len(df3))
    
    df4 = df[['n_day','v_dayname']].drop_duplicates()
    df4['n_day_id'] = np.arange(0, len(df4))
    
    df5 = df[['n_quarter']].drop_duplicates()
    df5['n_quarter_id'] = np.arange(0, len(df5))
    
    df6 = df[['n_week']].drop_duplicates()
    df6['n_week_id'] = np.arange(0, len(df6))
    
    
    
    
    cols_= ['d_date_id','d_date', 'n_day', 'n_week', 'n_month','n_quarter', 'n_year']
    df7 = dates.copy()[cols_]
    
    dict_dim_date_map = {
    'dim_year' : df2,
    'dim_month' : df3,
    'dim_day': df4,
    'dim_quarter': df5,
    'dim_week':df6
    }
    
    
    # arrumar order das coluns
    #for df_tmp in [df2,df3,df4,df5,df6]:
    df2 = df2[df2.columns[::-1]]
    df3 = df3[df3.columns[::-1]]
    df4 = df4[df4.columns[::-1]]
    df5 = df5[df5.columns[::-1]]
    df6 = df6[df6.columns[::-1]]
    
    for col in cols_[2:]:
        col_name = col
        df_tmp = dict_dim_date[col.replace('n_','dim_')]
        d_swap = pd.Series(df_tmp[f'{col}_id'].values,index=df_tmp[col]).to_dict()
        df7[col + '_id'] = df7[col].map(d_swap)
        
    
    
    return dates, df2, df3, df4, df5, df6, df7.drop(columns=cols_[2:])


def executa_comando_sql(comando):
    host='host'
    database='dw'
    user='data'
    password='data'
    con_post = pg8000.connect(user=user,host=host,database=database,password=password,timeout=120)

    with con_post as connection:
        connection.run(comando)
        connection.commit()


def send_data_sql(df, table):
    #mode = 'append'
    mode ="overwrite"
    host='host'
    database='dw'
    user='data'
    password='data'
    con_post = pg8000.connect(user=user,host=host,database=database,password=password,timeout=120)

    with con_post as connection:
        wr.postgresql.to_sql(df, 
                         con=connection, 
                         schema="toro_data", 
                         table=table, 
                         mode=mode)

def main():

    df, df2, df3, df4, df5, df6, df7 = create_date_table()



    dict_dim_date = {
        'dim_date' : df7,
        'dim_year' : df2,
        'dim_month' : df3,
        'dim_day': df4,
        'dim_quarter': df5,
        'dim_week':df6
    }




    from tqdm import tqdm
    for k, df_tmp in tqdm(dict_dim_date.items()):
        send_data_sql(df_tmp, k)


    dict_dim_date_sql_comands = {
        #'dim_date' : df7,
        'dim_year' : 'n_year_id',
        'dim_month' : 'n_month_id',
        'dim_day': 'n_day_id',
        'dim_quarter': 'n_quarter_id',
        'dim_week': 'n_week_id'
    }
    b = base_table = 'database_schema.'
    comandos = ["ALTER TABLE database_schema.dim_date ADD PRIMARY KEY (d_date_id);"]
    for k, v in tqdm(dict_dim_date_sql_comands.items()):
        tmp_cmd_1 = f"ALTER TABLE {b+k} ADD PRIMARY KEY ({v});"
        tmp_cmd_2 =f"ALTER TABLE {b}dim_date ADD CONSTRAINT fk_dim_date_{k} FOREIGN KEY ({v}) REFERENCES {b+k} ({v});"
        comandos.append(tmp_cmd_1)
        comandos.append(tmp_cmd_2)



    for comando in tqdm(comandos):
        #print(comando)
        executa_comando_sql(comando)

if __name__ == '__main__':
    main()
