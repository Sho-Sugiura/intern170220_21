import csv
import pandas as pd

def readCSVbyPd(readFileName, writeFileName):
    df = pd.read_csv(readFileName)

    df.dropna()                                                     # 欠損値（行）を消すパターン
#    df.dropna(axis=1)                                               # 欠損値（列）を消すパターン
    for colName in df:
        df[colName] = df[colName].fillna(0)                         # 「0」による穴埋め
    colName = 'emp_length'
    df[colName] = df[colName].mask(df[colName] == 'n/a', '0')

    # term: 「months」の削除
    df['term'] = df['term'].map(lambda x: x[0:len(x)-7])

    # int_rate: 「%」の削除
    df['int_rate'] = df['int_rate'].map(lambda x: x[0:len(x) - 1])

    # grade, sub_grade: アルファベットの数値化
    grades = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    df['grade'] = df['grade'].map(lambda x: grades.index(x))
    df['sub_grade'] = df['sub_grade'].map(lambda x: str(grades.index(x[0:1])) + str(x[1:2]))

    # emp_length: 勤続年数の数値値
    df['emp_length'] = df['emp_length'].map(lambda x: x[0:1] if x.find('+') == -1 and x.find('<') == -1 else 10 if x.find('+') else 0)

    # earliest_cr_line: 「日付」の数値化
    monthDist = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    df['earliest_cr_line'] = df['earliest_cr_line'].map(lambda x: x[4:8] + monthDist[x[0:3]])

    # revol_util: 「%」の削除
    df['revol_util'] = df['revol_util'].map(lambda x: x[0:len(x) - 1] if str(x).find('%') > -1 else x)

    # 不要な列の削除
    # [実験1]除去する変数（0.0005以下）
    # 除去方針：地域系は、１地域当たりのサンプル数が少ないため、排除
    del df['zip_code']                 #0.00000 <（米国の）郵便番号の先頭３桁>
    # 除去方針：職業系は、１職業当たりのサンプル数が少ないため、排除
    # 除去方針：月系は基本的に排除
    # 除去方針：口座系は基本的に排除
    del df['acc_now_delinq']           #0.00007 <借手が遅延している口座数>
    del df['num_sats']                 #0.00024 <満足できる口座数>
    del df['num_tl_120dpd_2m']         #0.00007 <１２０日以上延滞中の口座数（過去２ヶ月間更新）>
    del df['num_tl_30dpd']             #0.00003 <３０日以上延滞中の口座数（過去２ヶ月間更新）>
    # 除去方針：correct to target <= 0.0005
    del df['pymnt_plan']               #0.00000 <支払計画の設定の有無>
    del df['open_acc']                 #0.00029 <借手クレジットファイルの未締クレジットライン数>
    del df['open_rv_12m']              #0.00032 <過去１２ヶ月のリボ取引数>
    del df['chargeoff_within_12_mths'] #0.00009 <１２ヶ月以内の貸倒償却件数>
    del df['num_il_tl']                #0.00031 <割賦勘定の数>
    del df['num_op_rev_tl']            #0.00037 <オープンリボルビングアカウントの数>
    del df['pub_rec_bankruptcies']     #0.0003  <公的な倒産件数>

    # [実験2]除去する変数(0.005以下)
    # 除去方針：地域系は、１地域当たりのサンプル数が少ないため、排除
    del df['addr_state']               #0.00186  <（米国の）州>
    # 除去方針：職業系は、１職業当たりのサンプル数が少ないため、排除
    # 除去方針：年月系は基本的に排除
    del df['earliest_cr_line']         #0.0021  <借り手の与信限度額の設定月>
    del df['emp_length']               #0.0014  <継続雇用年数>                      ※注意
    del df['mo_sin_old_il_acct']       #0.00155  <最古の割賦口座開設後の月数>
    del df['mo_sin_old_rev_tl_op']     #0.00294  <最古のリボ口座開設から数ヶ月>
    del df['mo_sin_rcnt_tl']           #0.00105  <最近の口座開設後の月数>
    del df['mths_since_last_delinq']   #0.00233  <最後の延滞からの月数>
    del df['mths_since_last_major_derog']  #0.00294  <最近の９０日以上延滞からの月数>
    del df['mths_since_rcnt_il']       #0.00175  <最近の分割払い口座開設からの月数>
    del df['mths_since_recent_bc_dlq'] #0.00233  <直近の銀行カード延滞以降の月数>
    del df['mo_sin_rcnt_rev_tl_op']    #0.00077  <直近のリボ口座開設からの月数>
    del df['mths_since_recent_bc']     #0.00083  <月系>
    del df['mths_since_recent_inq']    #0.00071  <月系>
    # 除去方針：口座系は基本的に排除
    # 除去方針：correct to target <= 0.005
    # 取引数系
    del df['inq_fi']                   #0.00115  <個人金融の問い合わせ件数>           ※注意
    del df['inq_last_6mths']           #0.00282  <過去６ヶ月間のお問い合わせ件数>
    del df['acc_open_past_24mths']     #0.0008  <過去２４ヶ月間に開かれた取引数>
    del df['inq_last_12m']             #0.00077  <過去１２ヶ月間の信用調査の件数>
    # 金銭系
    del df['annual_inc']               #0.00296  <年間収入>                         ※注意
    del df['delinq_2yrs']              #0.00385  <３０日以上延滞金額>                ※注意
    del df['loan_amnt']                #0.00422  <借り手の申請ローン額>              ※注意
    del df['max_bal_bc']               #0.00286  <全てのリボ最大残高>
    # その他
    del df['purpose']                  #0.00266  <資金用途のカテゴリ>
    del df['title']                    #0.00317  <ローンのタイトル>
    del df['il_util']                  #0.00197  <クレジット限度額に対する現在の合計残高の比率>
    del df['mths_since_last_record']   #0.00193  <最後の公的記録>
    del df['delinq_amnt']              #0.00025  <借り手の延滞中口座の未払額>        ※注意
    del df['num_actv_bc_tl']           #0.00067  <>
    del df['num_bc_sats']              #0.00065  <>
    del df['num_tl_op_past_12m']       #0.00075  <>
    del df['open_rv_24m']              #0.0007  <>
    del df['total_cu_tl']              #0.00082  <>
    del df['mths_since_recent_revol_delinq']      #0.00194  <>
    del df['num_accts_ever_120_pd']    #0.0013  <>
    del df['num_actv_rev_tl']          #0.00165  <>
    del df['num_bc_tl']                #0.00253  <>
    del df['num_rev_accts']            #0.00218  <>
    del df['num_rev_tl_bal_gt_0']      #0.0018  <>
    del df['num_tl_90g_dpd_24m']       #0.00178  <>
    del df['open_acc_6m']              #0.00153  <>
    del df['open_il_12m']              #0.00215  <>
    del df['open_il_24m']              #0.00129  <>
    del df['open_il_6m']               #0.00167  <>
    del df['pct_tl_nvr_dlq']           #0.00276  <>
    del df['pub_rec']                  #0.00124  <>
    del df['revol_bal']                #0.00224  <>
    del df['tax_liens']                #0.0012  <>
    del df['total_acc']                #0.00183  <>
    del df['total_bal_ex_mort']        #0.00246  <>
    del df['total_bal_il']             #0.00283  <>
    del df['total_il_high_credit_limit'] #0.00276  <>

    # ここまでの結論（比較的強い相関がある変数）
    # bc_open_to_buy（リボ銀行カードでの購入可能最大額）
    # grade（ローングレード）
    # int_rate（ローンの金利）
    #

    # [実験3]除去する変数(0.01以下)
    #

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

readCSVbyPd('training_data.csv', 'output.csv')