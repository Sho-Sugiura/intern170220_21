import csv
import pandas as pd

def readCSVbyPd(readFileName, writeFileName):
    df = pd.read_csv(readFileName)

    for colName in df:
        df[colName] = df[colName].fillna(0)                         # 「0」による穴埋め
        df[colName] = df[colName].mask(df[colName] == 'n/a', '0')


    # term: 「months」の削除
    df['term'] = df['term'].map(lambda x: x[0:len(x)-7])
    # int_rate: 「%」の削除
    df['int_rate'] = df['int_rate'].map(lambda x: x[0:len(x) - 1])
    # earliest_cr_line: 「日付」の数値化
    monthDist = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    df['earliest_cr_line'] = df['earliest_cr_line'].map(lambda x: monthDist[x[0:3]])
    df['earliest_cr_line'] = df['earliest_cr_line'].map(lambda x: x[3:7] + x[0:2])
    # revol_util: 「%」の削除
#    df['revol_util'] = df['revol_util'].map(lambda x: x[0:len(x) - 1] if x.isnumeric() else x)

    df.to_csv(writeFileName, index=None)

def readCSV(readFileName, writeFileName):
    workList = []

    with open(readFileName, 'r') as fp:     # ファイルの読込
        dataReader = csv.reader(fp)

        # csvファイルの加工用リストへの移動
        for row in dataReader:
            workList.append(row)

    for rowCnt in range(0, len(workList)):
        # term: 「term」の削除
        targetCol = 2
        strLen = len(workList[rowCnt][targetCol])
        workList[rowCnt][targetCol] = workList[rowCnt][targetCol][0:strLen-7]

        # int_rate: 「%」の削除
        targetCol = 3
        strLen = len(workList[rowCnt][targetCol])
        workList[rowCnt][targetCol] = workList[rowCnt][targetCol][0:strLen-1]

        # earliest_cr_line: 「日付」の数値化
        targetCol = 19
        year = workList[rowCnt][targetCol][4:8]
        month = workList[rowCnt][targetCol][0:3]
        if month == "Jan": month = "01"
        elif month == "Feb": month = "02"
        elif month == "Mar": month = "03"
        elif month == "Apr": month = "04"
        elif month == "May": month = "05"
        elif month == "Jun": month = "06"
        elif month == "Jul": month = "07"
        elif month == "Aug": month = "08"
        elif month == "Sep": month = "09"
        elif month == "Oct": month = "10"
        elif month == "Nov": month = "11"
        elif month == "Dec": month = "12"
        workList[rowCnt][targetCol] = str(year) + str(month)

        # mths_since_last_delinq: 空白の「0」埋め
        targetCol = 21
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # mths_since_last_record: 空白の「0」埋め
        targetCol = 22
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # open_acc: 空白の「0」埋め
        targetCol = 23
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # revol_util: 「%」の削除
        targetCol = 26
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = '0%'
        targetCol = 26
        strLen = len(workList[rowCnt][targetCol])
        workList[rowCnt][targetCol] = workList[rowCnt][targetCol][0:strLen-1]

        # mths_since_last_major_derog: 空白の「0」埋め
        targetCol = 29
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # acc_now_delinq: 空白の「0」埋め
        targetCol = 30
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # open_acc_6m: 空白の「0」埋め
        targetCol = 32
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # open_il_6m: 空白の「0」埋め
        targetCol = 33
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # open_il_12m: 空白の「0」埋め
        targetCol = 34
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # open_il_24m: 空白の「0」埋め
        targetCol = 35
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # mths_since_rcnt_il: 空白の「0」埋め
        targetCol = 36
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # total_bal_il: 空白の「0」埋め
        targetCol = 37
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # il_util: 空白の「0」埋め
        targetCol = 38
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # open_rv_12m: 空白の「0」埋め
        targetCol = 39
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # open_rv_24m: 空白の「0」埋め
        targetCol = 40
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # max_bal_bc: 空白の「0」埋め
        targetCol = 41
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # all_util: 空白の「0」埋め
        targetCol = 42
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # inq_fi: 空白の「0」埋め
        targetCol = 44
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # total_cu_tl: 空白の「0」埋め
        targetCol = 45
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # inq_last_12m: 空白の「0」埋め
        targetCol = 46
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # bc_open_to_buy: 空白の「0」埋め
        targetCol = 49
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # bc_util: 空白の「0」埋め
        targetCol = 50
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # mo_sin_old_il_acct: 空白の「0」埋め
        targetCol = 53
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # mths_since_recent_bc_dlq: 空白の「0」埋め
        targetCol = 59
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # mths_since_recent_inq: 空白の「0」埋め
        targetCol = 60
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # mths_since_recent_revol_delinq: 空白の「0」埋め
        targetCol = 61
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # num_accts_ever_120_pd: 空白の「0」埋め
        targetCol = 62
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # num_tl_120dpd_2m: 空白の「0」埋め
        targetCol = 72
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

        # percent_bc_gt_75: 空白の「0」埋め
        targetCol = 77
        if workList[rowCnt][targetCol] == '':
            workList[rowCnt][targetCol] = 0

    with open(writeFileName, 'w') as fp:
        csvWriter = csv.writer(fp, lineterminator='¥n')
        csvWriter.writerow(workList)

    return 0

readCSVbyPd('training_data.csv', 'training_dataOut.csv')