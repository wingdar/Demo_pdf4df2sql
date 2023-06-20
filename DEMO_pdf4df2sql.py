# -*- coding: utf-8 -*-
"""
Created on 2023.06.20

程式目的：直接讀取 TWSE 臺灣證券交易所，每日個股資料，轉換並傳入 mariadb 資料，以華航為例

@author: DAR
"""
import os
import pymssql
import pandas as pd
import time

import mariadb
import tabula
#==============================================================================  
if __name__ == "__main__":
#==============================================================================      
    Start_time = time.perf_counter() # 計時啟動
    # 強制變更工作目錄
    # 獲取當前執行檔案的目錄
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 切換到當前執行檔案的目錄
    os.chdir(current_dir)

    print("Current working directory: {0}".format(os.getcwd()))
#===================================================================    
    # 連接 MySQL 資料庫
    conn = mariadb.connect(host = "HOST",  # 以自身設定為主
                           port = 3306,
                           user = "user",
                           password = "pw",
                           db = "dbname"
                           ) #charset='utf8_unicode_ci'
    # 設定自動提交
    conn.autocommit = True

    cursor = conn.cursor()

    # 指定網頁上的 PDF 檔案的 URL
    pdf_url = "https://www.twse.com.tw/pdf/ch/2610_ch.pdf"

    # 使用 tabula-py 套件讀取網頁上的 PDF，並轉換為 DataFrame
    df = tabula.read_pdf(pdf_url, pages="all")
    # 讀取 PDF 文件中的第一個表格
    # df = tabula.read_pdf("2610_ch.pdf", pages='all')

    # 計算 PDF 文件中的表格數量
    num_tables = len(df)

    print(f"The PDF file contains {num_tables} tables.")

    # 輸出結果
    # print(df[0])

    # 列出每個表格的資訊 除錯用
    # for i, dfl in enumerate(df):
    #     print(f"Table {i+1}:\n")
    #     print(dfl)
    #     print("\n\n")

    # 假設 df 是第一個 DataFrame
    df0 = df[0] #收盤價、成交股數
    df1 = df[1] #三大法人
    df2 = df[2] #融資、融券
    df3 = df[6] #本益比

    #======================================================================
    # 分割 DataFrame
    dfs = [df0.iloc[:, i:i+3] for i in range(0, len(df0.columns), 3)]

    # 每個新的 DataFrame 只有一個日期欄位和其後的兩個欄位
    for i, df in enumerate(dfs):
        # df.columns = ['Date', 'Closing Price', 'Trading Volume']
        df.columns = ['日期', '收盤價', '成交股數']
        dfs[i] = df.drop(df.index[0])  # 將更新後的 DataFrame 保存回 dfs 中
        # print(f"DataFrame {i+1}:\n")
        # print(df)
        # print("\n\n")

    # 使用 pandas 的 concat 函數來將這些 DataFrame 結合為一個
    merged_df = pd.concat(dfs, ignore_index=True)

    print("merged_df\n")
    print(merged_df)
    #======================================================================
    # 假設 df 是原始 DataFrame
    # df1 = df[1]

    # 分割 DataFrame
    dfs1 = [df1.iloc[:, i:i+4] for i in range(0, len(df1.columns), 5)]

    # 每個新的 DataFrame 只有一個日期欄位和其後的兩個欄位
    for i, df in enumerate(dfs1):
        # df.columns = ['Date', 'Closing Price', 'Trading Volume']
        df.columns = ['日期', '投信', '自營商', '外資']
        # dfs1[i] = df.drop(df.index[0])  # 將更新後的 DataFrame 保存回 dfs 中
        # print(f"DataFrame {i+1}:\n")
        # print(df)
        # print("\n\n")


    # 重新合併兩個 DataFrame
    merged_df1 = pd.concat(dfs1, ignore_index=True)

    print("merged_df1\n")
    print(merged_df1)
    #======================================================================
    # 分割 DataFrame
    dfs2 = [df2.iloc[:, i:i+3] for i in range(0, len(df2.columns), 3)]

    # 每個新的 DataFrame 只有一個日期欄位和其後的兩個欄位
    for i, df in enumerate(dfs2):
        # df.columns = ['Date', 'Closing Price', 'Trading Volume']
        df.columns = ['日期', '融資張數', '融券張數']
        # dfs2[i] = df.drop(df.index[0])  # 將更新後的 DataFrame 保存回 dfs 中
        # print(f"DataFrame {i+1}:\n")
        # print(df)
        # print("\n\n")

    # 使用 pandas 的 concat 函數來將這些 DataFrame 結合為一個
    merged_df2 = pd.concat(dfs2, ignore_index=True)

    print("merged_df2\n")
    print(merged_df2)

    #======================================================================
    # 分割 DataFrame
    dfs3 = [df3.iloc[:, i:i+2] for i in range(0, len(df3.columns), 2)]

    # 每個新的 DataFrame 只有一個日期欄位和其後的兩個欄位
    for i, df in enumerate(dfs3):
        # df.columns = ['Date', 'Closing Price', 'Trading Volume']
        df.columns = ['日期', '本益比']
        # dfs3[i] = df.drop(df.index[0])  # 將更新後的 DataFrame 保存回 dfs 中
        # print(f"DataFrame {i+1}:\n")
        # print(df)
        # print("\n\n")

    # 使用 pandas 的 concat 函數來將這些 DataFrame 結合為一個
    merged_df3 = pd.concat(dfs3, ignore_index=True)

    print("merged_df3\n")
    print(merged_df3)

    #======================================================================
    # 假設你已經將四個 DataFrame 儲存為 merged_df, merged_df1, merged_df2, merged_df3
    # 使用 '日期' 列將所有 DataFrame 進行合併

    final_df = merged_df.merge(merged_df1, on='日期', how='left')
    final_df = final_df.merge(merged_df2, on='日期', how='left')
    final_df = final_df.merge(merged_df3, on='日期', how='left')

    # 將 final_df 的資料類型進行轉換
    # final_df['日期'] = pd.to_datetime(final_df['日期'])
    # 先將 '日期' 欄位轉換為 datetime 格式，並假設年份為 1900（這是 pandas 的預設行為）
    final_df['日期'] = pd.to_datetime(final_df['日期'], format='%m/%d')
    # 然後，我們更改年份為 2023
    final_df['日期'] = final_df['日期'].apply(lambda dt: dt.replace(year=2023))
    # 將 '日期' 欄位轉換為字串
    final_df['日期'] = final_df['日期'].dt.strftime('%Y-%m-%d')        

    final_df['成交股數'] = final_df['成交股數'].str.replace(',', '').astype(int)
    # final_df['投信'] = final_df['投信'].str.replace(',', '').str.replace('+', '').astype(int)
    # final_df['自營商'] = final_df['自營商'].str.replace(',', '').str.replace('+', '').astype(int)
    # final_df['外資'] = final_df['外資'].str.replace(',', '').str.replace('+', '').astype(int)
    final_df['投信'] = final_df['投信'].str.replace(',', '', regex=True).str.replace('+', '', regex=True).astype(int)
    final_df['自營商'] = final_df['自營商'].str.replace(',', '', regex=True).str.replace('+', '', regex=True).astype(int)
    final_df['外資'] = final_df['外資'].str.replace(',', '', regex=True).str.replace('+', '', regex=True).astype(int)
    final_df['融資張數'] = final_df['融資張數'].str.replace(',', '', regex=True).str.replace('+', '', regex=True)
    final_df['融券張數'] = final_df['融券張數'].str.replace(',', '', regex=True).str.replace('+', '', regex=True)
    final_df['融資張數'] = final_df['融資張數'].fillna(0)
    final_df['融券張數'] = final_df['融券張數'].fillna(0)
    final_df['融資張數'] = final_df['融資張數'].astype(int)
    final_df['融券張數'] = final_df['融券張數'].astype(int)

    # 新增 '市場熱度' 和 '備註' 欄位，預設值為 null
    final_df['市場熱度'] = 0
    final_df['備註'] = None

    # 列印出最終的 DataFrame
    print(final_df)
    #======================================================================
    # # 為每筆資料建立 SQL 語句
    # for i,row in final_df.iterrows():
    #     sql = "INSERT INTO 2610stock_data (日期, 收盤價, 成交股數, 投信, 自營商, 外資, 融資張數, 融券張數, 本益比, 市場熱度, 備註) \
    #            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #     cursor.execute(sql, tuple(row))

    # 為每筆資料建立 SQL 語句
    for i, row in final_df.iterrows():
        date = row['日期']

        # 檢查資料庫中是否已存在相同的日期資料
        check_sql = "SELECT 日期 FROM 2610stock_data WHERE 日期 = %s"
        cursor.execute(check_sql, (date,))
        result = cursor.fetchone()

        if result is None:
            sql = "INSERT INTO 2610stock_data (日期, 收盤價, 成交股數, 投信, 自營商, 外資, 融資張數, 融券張數, 本益比, 市場熱度, 備註) \
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("資料已插入：", date)
        else:
            print("資料已存在，跳過：", date)

    # 提交事務 
    # conn.commit() #已改用自動提交

    # 關閉連線
    cursor.close()
    conn.close()
    print("完成寫入資料庫！")

    END_time = time.perf_counter() #計時結束
    print("總花費時間：",round(END_time-Start_time,2),"秒")


    """
    CREATE TABLE 2610stock_data (
    日期 DATE,
    收盤價 FLOAT,
    成交股數 BIGINT,
    投信 INT,
    自營商 INT,
    外資 INT,
    融資張數 INT,
    融券張數 INT,
    本益比 FLOAT,
    市場熱度 FLOAT,
    備註 TEXT
    );

    https://www.twse.com.tw/zh/listed/profile/company.html?2610    
    https://www.twse.com.tw/pdf/ch/2610_ch.pdf
    """