import os
import streamlit as st
import psycopg2
from openai import AzureOpenAI
import re

# --- Azure OpenAI 設定 ---
azure_endpoint = os.environ["CHATBOT_AZURE_OPENAI_ENDPOINT"]
api_key = os.environ["CHATBOT_AZURE_OPENAI_API_KEY"]
deployment_name = "gpt-4.1-nano"
api_version = "2024-12-01-preview"

client = AzureOpenAI(
    azure_endpoint=azure_endpoint,
    api_key=api_key,
    api_version=api_version
)

# --- PostgreSQL 接続情報 ---
DB_HOST = "localhost"
DB_NAME = "unipa"
DB_USER = "unipauser"
DB_PASS = "unipauser"

# --- チャット履歴の初期化 ---
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

def generate_sql_from_question(question):
#     system_message_sql = [{
#         "role": "system",
#         "content": """
# あなたはPostgreSQLのSQLクエリを生成するアシスタントです。
# 以下のテーブル構造と関係性を前提としてください：

# pkb_cmps(campus_no,campus_name)
# pkb_class_sbt(class_sbt_cd,class_sbt_name)

# 以下のルールに従ってください：
# 質問に応じて SELECT, UPDATE, DELETE, INSERT など適切なSQLを生成してください
# SQL文のみを返してください(説明や補足は不要)
# 「sql」などのラベルは付けないでください
# 1文で完結させてください
# """
#     }]
    system_message_sql = [{
        "role": "system",
        "content": """
You are an assistant that generates PostgreSQL SQL queries.

Assume the following table schemas:
pkb_cmps(campus_no, campus_name)
pkb_class_sbt(class_sbt_cd, class_sbt_name)

Rules:
- Generate appropriate SQL (SELECT, UPDATE, DELETE, INSERT) based on the user's question.
- Return only the SQL query (no explanation, no labels).
- Use a single SQL statement.
"""
        }]

    user_message = [{"role": "user", "content": question}]
    response = client.chat.completions.create(
        model=deployment_name,
        messages=system_message_sql + user_message,
        stream=False
    )

    raw_sql = response.choices[0].message.content.strip()
    raw_sql = re.sub(r"(?i)^sql\s*\n?", "", raw_sql).strip()

    if not raw_sql or raw_sql.lower() == "undefined" or not re.search(r"\b(select|update|insert|delete)\b", raw_sql.lower()):
        return None

    return raw_sql

# --- SQL実行関数（全SQL対応） ---
def run_sql(sql):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=5433,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            sslmode="disable"
        )
        cur = conn.cursor()
        cur.execute(sql.strip())

        if sql.strip().lower().startswith("select"):
            rows = cur.fetchall()
            result_text = "\n".join("・" + "｜".join(str(x) for x in row) for row in rows)
        else:
            conn.commit()
            result_text = f"✅ SQL実行成功：{cur.rowcount}件処理されました"

        cur.close()
        conn.close()
        return result_text
    except Exception as e:
        return f"SQL実行エラー: {e}"

# --- 通常応答関数（stream=True） ---
def get_response(prompt: str = ""):
    st.session_state.chat_history.append({"role": "user", "content": prompt})
    system_message = [{"role": "system", "content": "You are a helpful assistant."}]
    chat_messages = [{"role": m["role"], "content": m["content"]} for m in st.session_state.chat_history]
    response = client.chat.completions.create(
        model=deployment_name,
        messages=system_message + chat_messages,
        stream=True
    )
    return response

# --- 応答履歴追加関数 ---
def add_history(response):
    st.session_state.chat_history.append({"role": "assistant", "content": response})

# --- タイトル表示 ---
st.title("学生データチャットボット")

# --- チャット履歴表示（すべての履歴を描画） ---
for chat in st.session_state.chat_history:
    with st.chat_message(chat["role"]):
        st.markdown(chat["content"])

# --- 入力と応答処理 ---
if prompt := st.chat_input("質問をどうぞ（例：キャンパステーブルについてwithクラス種別）"):
    st.session_state.chat_history.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # --- SQL生成対象かどうかを判定 ---
    if any(keyword in prompt for keyword in ["キャンパス"]):
        with st.chat_message("assistant"):
            st.markdown("SQLを生成しています…")
            sql = generate_sql_from_question(prompt)
            st.write("🔍 実行対象SQL（repr表示）:", repr(sql))

            if sql is None:
                st.markdown("⚠️ SQLの生成に失敗しました。質問内容を見直してください。")
                add_history("SQL生成に失敗しました。")
            else:
                st.markdown("生成されたSQL:")
                st.code(sql, language="sql")

                st.markdown("SQLを実行しています…")
                result_text = run_sql(sql.strip())
                st.markdown(result_text)
                add_history(result_text)
    else:
        response_text = ""
        stream = get_response(prompt)
        for chunk in stream:
            if hasattr(chunk, "choices") and chunk.choices:
                delta = chunk.choices[0].delta
                if hasattr(delta, "content") and delta.content:
                    response_text += delta.content
        with st.chat_message("assistant"):
            st.markdown(response_text)
        add_history(response_text)