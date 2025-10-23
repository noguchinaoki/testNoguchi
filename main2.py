import os
import streamlit as st
import psycopg2
from openai import AzureOpenAI
import re
import pandas as pd

# PostgreSQL接続情報（適宜変更）
conn = psycopg2.connect(
    host="localhost",
    dbname="unipa",
    user="unipauser",
    password="unipauser",
    port=5433
)

# # --- Azure OpenAI 設定 ---
# azure_endpoint = os.environ["CHATBOT_AZURE_OPENAI_ENDPOINT"]
# api_key = os.environ["CHATBOT_AZURE_OPENAI_API_KEY"]
# deployment_name = "gpt-4.1-nano"
# api_version = "2024-12-01-preview"

# client = AzureOpenAI(
#     azure_endpoint=azure_endpoint,
#     api_key=api_key,
#     api_version=api_version
# )

# # --- PostgreSQL 接続情報 ---
# DB_HOST = "localhost"
# DB_NAME = "unipa"
# DB_USER = "unipauser"
# DB_PASS = "unipauser"

# # --- チャット履歴の初期化 ---
# if "chat_history" not in st.session_state:
#     st.session_state.chat_history = []

# def generate_sql_from_question(question):
# #     system_message_sql = [{
# #         "role": "system",
# #         "content": """
# # あなたはPostgreSQLのSQLクエリを生成するアシスタントです。
# # 以下のテーブル構造と関係性を前提としてください：

# # pkb_cmps(campus_no,campus_name)
# # pkb_class_sbt(class_sbt_cd,class_sbt_name)

# # 以下のルールに従ってください：
# # 質問に応じて SELECT, UPDATE, DELETE, INSERT など適切なSQLを生成してください
# # SQL文のみを返してください(説明や補足は不要)
# # 「sql」などのラベルは付けないでください
# # 1文で完結させてください
# # """
# #     }]
#     system_message_sql = [{
#         "role": "system",
#         "content": """
# You are an assistant that generates PostgreSQL SQL queries.

# Assume the following table schemas:
# pkb_cmps(campus_no, campus_name)
# pkb_class_sbt(class_sbt_cd, class_sbt_name)

# Rules:
# - Generate appropriate SQL (SELECT, UPDATE, DELETE, INSERT) based on the user's question.
# - Return only the SQL query (no explanation, no labels).
# - Use a single SQL statement.
# """
#         }]

#     user_message = [{"role": "user", "content": question}]
#     response = client.chat.completions.create(
#         model=deployment_name,
#         messages=system_message_sql + user_message,
#         stream=False
#     )

#     raw_sql = response.choices[0].message.content.strip()
#     raw_sql = re.sub(r"(?i)^sql\s*\n?", "", raw_sql).strip()

#     if not raw_sql or raw_sql.lower() == "undefined" or not re.search(r"\b(select|update|insert|delete)\b", raw_sql.lower()):
#         return None

#     return raw_sql

# # --- SQL実行関数（全SQL対応） ---
# def run_sql(sql):
#     try:
#         conn = psycopg2.connect(
#             host=DB_HOST,
#             port=5433,
#             dbname=DB_NAME,
#             user=DB_USER,
#             password=DB_PASS,
#             sslmode="disable"
#         )
#         cur = conn.cursor()
#         cur.execute(sql.strip())

#         if sql.strip().lower().startswith("select"):
#             rows = cur.fetchall()
#             result_text = "\n".join("・" + "｜".join(str(x) for x in row) for row in rows)
#         else:
#             conn.commit()
#             result_text = f"✅ SQL実行成功：{cur.rowcount}件処理されました"

#         cur.close()
#         conn.close()
#         return result_text
#     except Exception as e:
#         return f"SQL実行エラー: {e}"

# # --- 通常応答関数（stream=True） ---
# def get_response(prompt: str = ""):
#     st.session_state.chat_history.append({"role": "user", "content": prompt})
#     system_message = [{"role": "system", "content": "You are a helpful assistant."}]
#     chat_messages = [{"role": m["role"], "content": m["content"]} for m in st.session_state.chat_history]
#     response = client.chat.completions.create(
#         model=deployment_name,
#         messages=system_message + chat_messages,
#         stream=True
#     )
#     return response

# # --- 応答履歴追加関数 ---
# def add_history(response):
#     st.session_state.chat_history.append({"role": "assistant", "content": response})

# # --- タイトル表示 ---
# st.title("学生データチャットボット")

# # --- チャット履歴表示（すべての履歴を描画） ---
# for chat in st.session_state.chat_history:
#     with st.chat_message(chat["role"]):
#         st.markdown(chat["content"])

# # --- 入力と応答処理 ---
# if prompt := st.chat_input("質問をどうぞ（例：キャンパステーブルについてwithクラス種別）"):
#     st.session_state.chat_history.append({"role": "user", "content": prompt})
#     with st.chat_message("user"):
#         st.markdown(prompt)

#     # --- SQL生成対象かどうかを判定 ---
#     if any(keyword in prompt for keyword in ["キャンパス"]):
#         with st.chat_message("assistant"):
#             st.markdown("SQLを生成しています…")
#             sql = generate_sql_from_question(prompt)
#             st.write("🔍 実行対象SQL（repr表示）:", repr(sql))

#             if sql is None:
#                 st.markdown("⚠️ SQLの生成に失敗しました。質問内容を見直してください。")
#                 add_history("SQL生成に失敗しました。")
#             else:
#                 st.markdown("生成されたSQL:")
#                 st.code(sql, language="sql")

#                 st.markdown("SQLを実行しています…")
#                 result_text = run_sql(sql.strip())
#                 st.markdown(result_text)
#                 add_history(result_text)
#     else:
#         response_text = ""
#         stream = get_response(prompt)
#         for chunk in stream:
#             if hasattr(chunk, "choices") and chunk.choices:
#                 delta = chunk.choices[0].delta
#                 if hasattr(delta, "content") and delta.content:
#                     response_text += delta.content
#         with st.chat_message("assistant"):
#             st.markdown(response_text)
#         add_history(response_text)

import psycopg2
import pandas as pd

# 1. 変数の定義
A = 2009
B = 1
C = '1162'
D = 200701

# 2. kmg_sskテーブルからkanri_no=Aに一致するkmk_cdを取得
query1 = "SELECT kmk_cd FROM kmg_ssk WHERE kanri_no = %s AND hyoka_cd < %s"
kmk_cd_df = pd.read_sql_query(query1, conn, params=(D, 'Z'))
kmk_cd_list = kmk_cd_df['kmk_cd'].tolist()

# kmk_cdが空の場合の分岐
if not kmk_cd_list:
    print("kmk_cd IN () が空になっている")
else:
    # 3. kmz_kmk_meisaiとkmb_kmk_haitoを結合して、kmkbnr_cdごとにtani_suを合計
    query2 = """
    SELECT h.kmkbnr_cd, SUM(m.tani_su) AS total_tani_su
    FROM kmz_kmk_meisai m
    JOIN kmb_kmk_haito h 
    ON m.kmk_cd = h.kmk_cd 
    AND m.meisai_no = h.meisai_no
    WHERE m.kmk_cd IN %s
    AND h.nyugak_nendo = %s
    AND h.gakki_no = %s
    AND h.cgks_cd = %s
    GROUP BY h.kmkbnr_cd
    """

    tani_su_df = pd.read_sql_query(query2, conn, params=(tuple(kmk_cd_list), A, B, C))

    # 4. kmb_kbrテーブルから条件一致するkmkbnr_cdとsot_yoken_taniを取得
    query3 = """
    SELECT kmkbnr_cd, sot_yoken_tani
    FROM kmb_kbr
    WHERE nyugak_nendo = %s AND gakki_no = %s AND cgks_cd = %s AND sot_yoken_tani IS NOT NULL
    """
    sot_yoken_df = pd.read_sql_query(query3, conn, params=(A, B, C))

    # 5. tani_su_dfに不足しているkmkbnr_cdのレコードを追加（total_tani_su=0）
    missing_kmkbnr = set(sot_yoken_df['kmkbnr_cd']) - set(tani_su_df['kmkbnr_cd'])
    missing_df = pd.DataFrame({
        'kmkbnr_cd': list(missing_kmkbnr),
        'total_tani_su': [0] * len(missing_kmkbnr)
    })
    tani_su_df = pd.concat([tani_su_df, missing_df], ignore_index=True)

    # 6. 階層構造の認識（親子関係の推定）
    def find_parent(code, candidates):
        possible_parents = [c for c in candidates if code != c and code.startswith(c)]
        return max(possible_parents, key=len) if possible_parents else None

    sot_yoken_df['parent_kmkbnr_cd'] = sot_yoken_df['kmkbnr_cd'].apply(lambda x: find_parent(x, sot_yoken_df['kmkbnr_cd'].tolist()))
    tani_su_df['parent_kmkbnr_cd'] = tani_su_df['kmkbnr_cd'].apply(lambda x: find_parent(x, tani_su_df['kmkbnr_cd'].tolist()))

    # 7. 子の単位数を親に加算
    def aggregate_with_children(df):
        agg_df = df.copy()
        for idx, row in df.iterrows():
            parent = row['parent_kmkbnr_cd']
            if parent:
                agg_df.loc[agg_df['kmkbnr_cd'] == parent, 'total_tani_su'] += row['total_tani_su']
        return agg_df

    tani_su_df = aggregate_with_children(tani_su_df)

    # 8. tani_suとsot_yoken_taniをkmkbnr_cdでマージして不足単位数を計算
    merged_df = pd.merge(sot_yoken_df, tani_su_df[['kmkbnr_cd', 'total_tani_su']], on='kmkbnr_cd', how='left')
    merged_df['fusoku_tani'] = merged_df['sot_yoken_tani'] - merged_df['total_tani_su']

    # 結果表示
    print(merged_df)