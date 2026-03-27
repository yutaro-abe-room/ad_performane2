import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import io

# ============================================================
# 設定
# ============================================================
PROJECT_ID = "spdb-cm-cc-ichiba2"

# ============================================================
# 関数定義
# ============================================================

def get_bigquery_client():
    """BigQueryクライアントを初期化（ローカルのgcloud認証を使用）"""
    try:
        # ローカルのデフォルト認証情報（ADC）を使用
        # ユーザーは事前に `gcloud auth application-default login` を実行している前提
        client = bigquery.Client(project=PROJECT_ID)
        return client
    except Exception as e:
        st.error("BigQueryクライアントの初期化に失敗しました。")
        st.error("ターミナルで `gcloud auth application-default login` を実行して認証してください。")
        st.error(f"エラー詳細: {e}")
        return None

def create_query(anken_id_val, shop_id, item_id, easy_id_list, start_date, end_date):
    """BigQueryクエリを生成する"""
    easy_id_str = ', '.join(map(str, easy_id_list)) if easy_id_list else 'NULL'
    
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # 日付計算ロジック
    sale_end_date = (end_date_dt + timedelta(days=1)).strftime('%Y-%m-%d')
    end_date_plus1month = (start_date_dt + relativedelta(months=1)).strftime('%Y-%m-%d')
    
    query = f"""
    -- 変数宣言
    DECLARE shopid, itemid INT64;
    DECLARE easyid ARRAY<INT64>;
    DECLARE start_datetime, end_datetime, sale_start_datetime, sale_end_datetime datetime;

    -- パラメータ設定
    SET shopid = {shop_id};
    SET itemid = {item_id};
    SET easyid = [{easy_id_str}];
    SET start_datetime = '{start_date} 00:00:00';
    SET end_datetime = '{end_date_plus1month} 00:00:00';
    SET sale_start_datetime = '{start_date} 00:00:00';
    SET sale_end_datetime = '{sale_end_date} 00:00:00';

    -- 集計
    WITH shop_purchaser_tbl AS (
    SELECT distinct easy_id
    FROM `spdb-data.ua_view_mk_ichiba.red_basket_detail_tbl` x
    WHERE reg_datetime >= DATE_ADD(start_datetime,INTERVAL -365 DAY)
      AND reg_datetime < start_datetime
      AND (cancel_datetime IS NULL OR cancel_datetime >= start_datetime)
      AND NOT EXISTS(SELECT * FROM `spdb-data.ua_view_mk_ichiba.illegal_order` y WHERE x.order_no = y.order_number)
      AND shop_id = shopid
    )

    SELECT distinct a.easy_id,
           a.fullname,
           (case when Sale_GMS is null then 0 else Sale_GMS end) as Sale_GMS,
           (case when Sale_GMS_ROOM is null then 0 else Sale_GMS_ROOM end) as Sale_GMS_ROOM,
           (case when Monthly_GMS is null then 0 else Monthly_GMS end) as Monthly_GMS,
           (case when Monthly_GMS_ROOM is null then 0 else Monthly_GMS_ROOM end) as Monthly_GMS_ROOM,

           (case when Sale_Order is null then 0 else Sale_Order end) as Sale_Order,
           (case when Sale_Order_ROOM is null then 0 else Sale_Order_ROOM end) as Sale_Order_ROOM,
           (case when Monthly_Order is null then 0 else Monthly_Order end) as Monthly_Order,
           (case when Monthly_Order_ROOM is null then 0 else Monthly_Order_ROOM end) as Monthly_Order_ROOM,

           (case when Sale_Purchaser is null then 0 else Sale_Purchaser end) as Sale_Purchaser,
           (case when Sale_Purchaser_ROOM is null then 0 else Sale_Purchaser_ROOM end) as Sale_Purchaser_ROOM,
           (case when Monthly_Purchaser is null then 0 else Monthly_Purchaser end) as Monthly_Purchaser,
           (case when Monthly_Purchaser_ROOM is null then 0 else Monthly_Purchaser_ROOM end) as Monthly_Purchaser_ROOM,

           (case when Sale_Purchaser_New is null then 0 else Sale_Purchaser_New end) as Sale_Purchaser_New,
           (case when Sale_Purchaser_ROOM_New is null then 0 else Sale_Purchaser_ROOM_New end) as Sale_Purchaser_ROOM_New,
           (case when Monthly_Purchaser_New is null then 0 else Monthly_Purchaser_New end) as Monthly_Purchaser_New,
           (case when Monthly_Purchaser_ROOM_New is null then 0 else Monthly_Purchaser_ROOM_New end) as Monthly_Purchaser_ROOM_New,

           (case when Sale_Item_GMS is null then 0 else Sale_Item_GMS end) as Sale_Item_GMS,
           (case when Sale_Item_GMS_ROOM is null then 0 else Sale_Item_GMS_ROOM end) as Sale_Item_GMS_ROOM,
           (case when Monthly_Item_GMS is null then 0 else Monthly_Item_GMS end) as Monthly_Item_GMS,
           (case when Monthly_Item_GMS_ROOM is null then 0 else Monthly_Item_GMS_ROOM end) as Monthly_Item_GMS_ROOM,
           
           (case when Sale_Click is null then 0 else Sale_Click end) as Sale_Click,
           (case when Sale_Click_ROOM is null then 0 else Sale_Click_ROOM end) as Sale_Click_ROOM,
           (case when Monthly_Click is null then 0 else Monthly_Click end) as Monthly_Click,
           (case when Monthly_Click_ROOM is null then 0 else Monthly_Click_ROOM end) as Monthly_Click_ROOM,

           (case when Sale_Item_Click is null then 0 else Sale_Item_Click end) as Sale_Item_Click,
           (case when Sale_Item_Click_ROOM is null then 0 else Sale_Item_Click_ROOM end) as Sale_Item_Click_ROOM,
           (case when Monthly_Item_Click is null then 0 else Monthly_Item_Click end) as Monthly_Item_Click,
           (case when Monthly_Item_Click_ROOM is null then 0 else Monthly_Item_Click_ROOM end) as Monthly_Item_Click_ROOM

    FROM (
      SELECT distinct easy_id, fullname
      FROM (SELECT easy_id, fullname, ROW_NUMBER() OVER (PARTITION BY easy_id ORDER BY time_stamp DESC) rn
            FROM `spdb-data.ua_view_mk_room.user_tbl`)
      where rn = 1 and
            easy_id in UNNEST(easyid)
    ) a

    LEFT JOIN (
      SELECT distinct af_id,
              sum(case when dt >= cast(sale_start_datetime as date) and dt < cast(sale_end_datetime as date) then click_num else 0 end) as Sale_Click,
              sum(case when dt >= cast(sale_start_datetime as date) and dt < cast(sale_end_datetime as date) and pointback_prefix = '_RTroom' then click_num else 0 end) as Sale_Click_ROOM,
              sum(click_num) as Monthly_Click,
              sum(case when pointback_prefix = '_RTroom' then click_num else 0 end) as Monthly_Click_ROOM,

              sum(case when dt >= cast(sale_start_datetime as date) and dt < cast(sale_end_datetime as date) and item_id = itemid then click_num else 0 end) as Sale_Item_Click,
              sum(case when dt >= cast(sale_start_datetime as date) and dt < cast(sale_end_datetime as date) and pointback_prefix = '_RTroom'  and item_id = itemid then click_num else 0 end) as Sale_Item_Click_ROOM,
              sum(case when item_id = itemid then click_num else 0 end) as Monthly_Item_Click,
              sum(case when pointback_prefix = '_RTroom' and item_id = itemid then click_num else 0 end) as Monthly_Item_Click_ROOM

      FROM `spdb-data.ua_view_mk_afl.click_log_summary`
      WHERE af_id in UNNEST(easyid) and
            shop_id = shopid and
            dt >= cast(start_datetime as date) and
            dt < cast(end_datetime as date)
      GROUP BY 1
      ) b

    ON a.easy_id = b.af_id

    LEFT JOIN (
      SELECT distinct easy_id,
              sum(case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime then (unit_price * quantity) else 0 end) as Sale_GMS,
              sum(case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and log_pointback like '_RTroom%' then (unit_price * quantity) else 0 end) as Sale_GMS_ROOM,
              sum((unit_price * quantity)) as Monthly_GMS,
              sum(case when log_pointback like '_RTroom%' then (unit_price * quantity) else 0 end) as Monthly_GMS_ROOM,

              count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime then log_oid else null end) as Sale_Order,
              count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and log_pointback like '_RTroom%' then log_oid else null end) as Sale_Order_ROOM,
              count(distinct log_oid) as Monthly_Order,
              count(distinct case when log_pointback like '_RTroom%' then log_oid else null end) as Monthly_Order_ROOM,

              count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and purchase_user_id > 0 then purchase_user_id else null end) as Sale_Purchaser,
              count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and purchase_user_id > 0 and log_pointback like '_RTroom%' then purchase_user_id else null end) as Sale_Purchaser_ROOM,
              count(distinct case when purchase_user_id > 0 then purchase_user_id else null end) as Monthly_Purchaser,
              count(distinct case when purchase_user_id > 0 and log_pointback like '_RTroom%' then purchase_user_id else null end) as Monthly_Purchaser_ROOM,

              count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and purchase_user_id > 0 and purchase_user_id not in (select * from shop_purchaser_tbl) then purchase_user_id else null end) as Sale_Purchaser_New,
              count(distinct case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and purchase_user_id > 0 and purchase_user_id not in (select * from shop_purchaser_tbl) and log_pointback like '_RTroom%' then purchase_user_id else null end) as Sale_Purchaser_ROOM_New,
              count(distinct case when purchase_user_id > 0 and purchase_user_id not in (select * from shop_purchaser_tbl) then purchase_user_id else null end) as Monthly_Purchaser_New,
              count(distinct case when purchase_user_id > 0 and purchase_user_id not in (select * from shop_purchaser_tbl) and log_pointback like '_RTroom%' then purchase_user_id else null end) as Monthly_Purchaser_ROOM_New,

              sum(case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and item_id = itemid then (unit_price * quantity) else 0 end) as Sale_Item_GMS,
              sum(case when log_resulttime >= sale_start_datetime and log_resulttime < sale_end_datetime and log_pointback like '_RTroom%' and item_id = itemid then (unit_price * quantity) else 0 end) as Sale_Item_GMS_ROOM,
              sum(case when item_id = itemid then (unit_price * quantity) else 0 end) as Monthly_Item_GMS,
              sum(case when log_pointback like '_RTroom%' and item_id = itemid then (unit_price * quantity) else 0 end) as Monthly_Item_GMS_ROOM

      FROM `spdb-data.ua_view_mk_afl.result_goods_n`
      WHERE easy_id in UNNEST(easyid) and
            me_id = (1000000 + shopid) and
            me_id = link_me_id and
            log_resulttime >= start_datetime and
            log_resulttime < end_datetime and
            log_clicktime >= start_datetime and
            log_clicktime < end_datetime
      GROUP BY 1
    ) c

    ON a.easy_id = c.easy_id

    ORDER BY 3 desc;
    """
    return query

# ============================================================
# メイン処理 (Streamlit UI)
# ============================================================

st.set_page_config(page_title="効果測定PJT集計ツール", layout="wide")
st.title("📊 効果測定PJT BigQuery集計ツール")

st.markdown("""
### 使い方
1. ターミナルで `gcloud auth application-default login` を実行し、Googleアカウントで認証してください。
2. 以下のボタンからインポート用のExcelファイルをアップロードしてください。
3. 集計ボタンを押すと、BigQueryで集計が開始されます。
4. 完了後、結果のExcelファイルをダウンロードできます。
""")

# ファイルアップロード
uploaded_file = st.file_uploader("インポート用Excelファイルをアップロード", type=['xlsx'])

if uploaded_file is not None:
    try:
        df_input = pd.read_excel(uploaded_file)
        df_input.columns = df_input.columns.str.strip()
        
        # カラムリネーム
        column_rename_map = {
            'EasyID': 'easy_id',
            'shopID': 'shop_id',
            'itemID': 'item_id',
            'SNS紹介開始日': '紹介開始日',
            'SNS紹介終了日': '紹介終了日',
            '店舗URL': '店舗URL',
            'プロモーション予算': 'プロモーション予算',
        }
        actual_rename_map = {k: v for k, v in column_rename_map.items() if k in df_input.columns}
        df_input = df_input.rename(columns=actual_rename_map)

        # 必須カラムチェック
        required_cols = ['案件ID', '案件名', 'easy_id', 'shop_id', 'item_id', '紹介開始日', '紹介終了日']
        missing_cols = [col for col in required_cols if col not in df_input.columns]

        if missing_cols:
            st.error(f"必須カラムが見つかりません: {', '.join(missing_cols)}")
        else:
            st.success(f"{len(df_input)}件のデータを読み込みました。")
            
            if st.button("集計開始"):
                client = get_bigquery_client()
                
                if client:
                    # グループ化
                    if df_input['案件ID'].dtype == 'object':
                        df_input['案件ID'] = df_input['案件ID'].str.strip()
                        
                    grouped_cols = ['案件ID', '案件名', 'shop_id', 'item_id', '紹介開始日', '紹介終了日']
                    grouped = df_input.groupby(grouped_cols)
                    total_groups = len(grouped)
                    
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    all_results = []
                    
                    for idx, (group_key, group) in enumerate(grouped, 1):
                        anken_id, anken_name, shop_id, item_id, start_date, end_date = group_key
                        
                        status_text.text(f"処理中 ({idx}/{total_groups}): {anken_name}")
                        progress_bar.progress(idx / total_groups)
                        
                        easy_id_list = group['easy_id'].unique().tolist()
                        
                        if not easy_id_list:
                            continue
                            
                        start_date_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                        end_date_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')
                        
                        # 店舗URLや予算の取得（グループ内の最初の値を採用）
                        store_url = group['店舗URL'].iloc[0] if '店舗URL' in group.columns else None
                        promo_budget = group['プロモーション予算'].iloc[0] if 'プロモーション予算' in group.columns else None

                        query = create_query(anken_id, shop_id, item_id, easy_id_list, start_date_str, end_date_str)
                        
                        try:
                            query_job = client.query(query)
                            result_df = query_job.result().to_dataframe()
                            
                            if not result_df.empty:
                                if 'AnkenID' in result_df.columns:
                                    result_df = result_df.rename(columns={'AnkenID': '案件ID'})
                                else:
                                    result_df['案件ID'] = anken_id
                                
                                result_df['案件名'] = anken_name
                                result_df['shop_id'] = shop_id
                                result_df['item_id'] = item_id
                                result_df['紹介開始日'] = start_date_str
                                result_df['紹介終了日'] = end_date_str
                                result_df['店舗URL'] = store_url
                                result_df['プロモーション予算'] = promo_budget
                                
                                all_results.append(result_df)
                                
                        except Exception as e:
                            st.warning(f"案件ID: {anken_id} の集計中にエラーが発生しました: {e}")

                    if all_results:
                        df_final = pd.concat(all_results, ignore_index=True)
                        
                        # カラム整理
                        base_columns = ['案件ID', '案件名', 'shop_id', 'item_id', '紹介開始日', '紹介終了日', '店舗URL', 'プロモーション予算', 'easy_id', 'fullname']
                        base_columns = [col for col in base_columns if col in df_final.columns]
                        other_columns = [col for col in df_final.columns if col not in base_columns]
                        df_final = df_final[base_columns + other_columns]
                        
                        # Excelダウンロード
                        buffer = io.BytesIO()
                        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                            df_final.to_excel(writer, index=False)
                        
                        st.success("集計が完了しました！")
                        st.download_button(
                            label="結果をExcelでダウンロード",
                            data=buffer.getvalue(),
                            file_name="効果測定結果.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    else:
                        st.warning("結果データが取得できませんでした。")
                        
    except Exception as e:
        st.error(f"ファイル読み込みエラー: {e}")
