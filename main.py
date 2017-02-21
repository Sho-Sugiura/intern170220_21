import csv
import pandas as pd

def readCSV(readFileName, writeFileName):
    df = pd.read_csv(readFileName)

    # 初期の前処理
    df.dropna()                                                     # 欠損値（行）を消すパターン
#    df.dropna(axis=1)                                               # 欠損値（列）を消すパターン
    for colName in df:
        df[colName] = df[colName].fillna(0)                         # 「0」による穴埋め
    colName = 'emp_length'
    df[colName] = df[colName].mask(df[colName] == 'n/a', '0')

    # term: ２値化
    df['term'] = df['term'].map(lambda x: 1 if x == ' 60 months' else 0)

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

    # 【不要な列の削除】
    # ---実験1---> 除去する変数（0.0005以下）
    # 除去方針：地域系は、１地域当たりのサンプル数が少ないため、排除
    del df['zip_code']                 #0.00000 <（米国の）郵便番号の先頭３桁>
    # 除去方針：職業系は、１職業当たりのサンプル数が少ないため、排除
    del df['emp_title']                # <>
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

    # ---実験2---> 除去する変数（0.005以下）
    # 除去方針：地域系は、１地域当たりのサンプル数が少ないため、排除
    del df['addr_state']               #0.00186  <（米国の）州>
    # 除去方針：年月系は基本的に排除
    del df['earliest_cr_line']         #0.0021  <借り手の与信限度額の設定月>
    del df['emp_length']               #0.0014  <継続雇用年数>
    del df['mo_sin_rcnt_tl']           #0.00105  <最近の口座開設後の月数>
    del df['mo_sin_old_il_acct']       #0.00155  <最古の割賦口座開設後の月数>
    del df['mths_since_rcnt_il']       #0.00175  <最近の割賦口座開設後の月数>
    del df['mo_sin_old_rev_tl_op']     #0.00294  <最古のリボ口座開設後の月数>
    del df['mo_sin_rcnt_rev_tl_op']    #0.00077  <直近のリボ口座開設後の月数>
    del df['mths_since_last_delinq']   #0.00233  <最後の延滞からの月数>
    del df['mths_since_last_major_derog']  #0.00294  <最近の９０日以上延滞からの月数>
    del df['mths_since_recent_bc']     #0.00083  <銀行口座開設からの経過月数>
    del df['mths_since_recent_bc_dlq'] #0.00233  <直近の銀行カード延滞以降の月数>
    del df['mths_since_recent_inq']    #0.00071  <直近の銀行カード遅延以降の月数>
    del df['mths_since_recent_revol_delinq']      #0.00194  <直近の回転の延滞以降の月間>
    # 除去方針：口座系は基本的に排除
    del df['num_bc_tl']                #0.00253  <銀行口座数>
    del df['num_actv_bc_tl']           #0.00067  <有効な銀行口座数>
    del df['num_bc_sats']              #0.00065  <優良な銀行口座数>
    del df['num_tl_op_past_12m']       #0.00075  <過去１２ヶ月間の開設口座数>
    del df['num_tl_90g_dpd_24m']       #0.00178  <過去２４ヶ月間の、９０日以上延滞した口座数>
    del df['open_il_12m']              #0.00215  <過去１２ヶ月の開設割賦口座数>
    del df['open_il_24m']              #0.00129  <過去２４ヶ月の開設割賦口座数>
    # 除去方針：correct to target <= 0.005
    # 取引数系
    del df['open_il_6m']               #0.00167  <アクティブ割賦取引数>
    del df['inq_fi']                   #0.00115  <個人金融の信用調査の件数>
    del df['inq_last_6mths']           #0.00282  <過去６ヶ月間の信用調査の件数>
    del df['inq_last_12m']             #0.00077  <過去１２ヶ月間の信用調査の件数>
    del df['acc_open_past_24mths']     #0.0008  <過去２４ヶ月間に開かれた取引数>
    del df['open_rv_24m']              #0.0007  <過去２４ヶ月間のリボ取引数>
    del df['num_rev_tl_bal_gt_0']      #0.0018  <残高が０以上のリボ取引数>
    del df['num_actv_rev_tl']          #0.00165  <アクティブなリボ取引数>
    del df['pct_tl_nvr_dlq']           #0.00276  <非延滞取引率>
    del df['total_cu_tl']              #0.00082  <金融取引件数>
    del df['open_acc_6m']              #0.00153  <過去６ヶ月間のオープントレード数>
    # 金銭系
#    del df['annual_inc']               #0.00296  <自己申告の年間収入>
    del df['delinq_2yrs']              #0.00385  <３０日以上延滞金額>
    del df['loan_amnt']                #0.00422  <借り手の申請ローン額>
    del df['max_bal_bc']               #0.00286  <全てのリボ最大残高>
    del df['delinq_amnt']              #0.00025  <借り手の延滞中口座の未払額>
    del df['revol_bal']                #0.00224  <リボ残高総額>
    del df['total_bal_il']             #0.00283  <全割賦勘定の現合計残高>
    del df['total_bal_ex_mort']        #0.00246  <住宅ローンを除く総クレジット残高>
    del df['total_il_high_credit_limit'] #0.00276  <割賦限度総額>
    # その他
    del df['purpose']                  #0.00266  <資金用途のカテゴリ>
    del df['title']                    #0.00317  <ローンのタイトル>
    del df['il_util']                  #0.00197  <クレジット限度額に対する現在の合計残高の比率>
    del df['mths_since_last_record']   #0.00193  <最後の公的記録>
    del df['pub_rec']                  #0.00124  <軽蔑的な公的記録数>
    del df['num_accts_ever_120_pd']    #0.0013  <支払期日１２０日以上のアカウント数>
    del df['num_rev_accts']            #0.00218  <リボアカウント数>
    del df['tax_liens']                #0.0012  <税務上の抵当権数>
    del df['total_acc']                #0.00183  <借り手のクレジットライン数>

    # ---実験3---> 手動による調整
    del df['mort_acc']                 #<住宅ローン口座の数>
#    del df['all_util']                #<全取引での与信限度のバランス>    ... targetと高い相関
    del df['avg_cur_bal']              #<全ての口座の平均残高>
#    del df['bc_util']                 #<銀行残高 / 与信限度額>          ... targetと低い相関
    del df['tot_cur_bal']             #<全口座の合計残高>
#    del df['dti']                     #<債務 / 収入>                   ... targetと低い相関
    del df['installment']              #<ローン発生時の借り手の月額支払い>  ※？
    del df['term']                     #<ローン支払い回数>
#    del df['tot_hi_cred_lim']         #<貸付限度総額>                  ... targetと低い相関
#    del df['total_bc_limit']          #<銀行カードの貸付限度総額>        ... targetと低い相関
#    del df['revol_util']              #<現在のリボ残高 / リボ与信限度額> ... ？
    del df['total_rev_hi_lim']        #<総リボクレジット限度額>
    del df['initial_list_status']      #<ローンの初期リスティングステータス>
    del df['percent_bc_gt_75']      #<>

    # ---ここまで残ったカラム（比較的強い相関がある変数）---> 計13変数
    # <correct to targetの値に基づき、後ろ向き法で残った変数（高い相関）>
    # int_rate              （ローンの金利）
    # grade                 （ローングレード）
    # sub_grade             （ローンの副次グレード）
    # verification_status   （申告所得額の検証の有無）
    # all_util              （全取引での与信限度のバランス）
    # bc_open_to_buy        （リボ銀行カードでの購入可能最大額）
    # home_ownership        （自宅の所有状況）
    # <correct to targetの値に基づき、後ろ向き法で残った変数（低い相関）>
    # dti                   （債務 / 収入）
    # bc_util               （銀行残高 / 与信限度額）
    # revol_util            （現在のリボ残高 / リボ与信限度額）
    # tot_hi_cred_lim       （貸付限度総額）
    # total_bc_limit        （銀行カードの貸付限度総額）
    # <自己判断に基づき、前向き法で加えた変数>
    # annual_inc            （自己申告の年間収入）

    df.to_csv(writeFileName, index=None)

readCSV('training_data.csv', 'output.csv')