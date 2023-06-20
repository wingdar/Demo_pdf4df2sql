# Demo_pdf4df2sql
用途：直接讀取 TWSE 臺灣證券交易所，每日個股資料，轉換並傳入 mariadb 資料，以華航為例

使用函數

import os
mport pandas as pd
import time

import mariadb
import tabula #pip install tabula-py
