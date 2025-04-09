import streamlit as st
import pandas as pd
import io
import os
from io import BytesIO
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import warnings
from datetime import date
from google.oauth2 import service_account
from google_auth_oauthlib.flow import Flow
import time
from functions import map_city_to_two_letters,extract_and_remove_city,extract_and_remove_district,split_address, df_id, df_hang, mapping_city, mapping_districts, get_google_services, list_csv_files_in_folder, load_and_merge_csv

SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
client_config = {
    "web": {
        "client_id": st.secrets["google"]["client_id"],
        "project_id": st.secrets["google"]["project_id"],
        "auth_uri": st.secrets["google"]["auth_uri"],
        "token_uri": st.secrets["google"]["token_uri"],
        "auth_provider_x509_cert_url":st.secrets["google"]["auth_provider_x509_cert_url"],
        "client_secret": st.secrets["google"]["client_secret"],
        "redirect_uris": [st.secrets["google"]["redirect_uris"]],
        "javascript_origins": st.secrets["google"]["javascript_origins"]
    }
}
REDIRECT_URI = 'http://localhost:8501/'

st.set_page_config(page_title="Data_Team", page_icon="🧠", layout="wide")
st.sidebar.markdown("### ✍️ Made by [KMD]('노션추가') 🚀")
st.sidebar.divider()  # 구분선 추가

# ✅ Streamlit UI 제목
st.title("💬 Data Auto system")
st.markdown("✨ 업무효율을 위한 자동화 시스템")

# ✅ 세션 상태 초기화
if "messages" not in st.session_state:
    st.session_state.messages = []
if "task" not in st.session_state:
    st.session_state.task = None
if "phone_string_column" not in st.session_state:
    st.session_state.phone_string_column = None
if "phone_target_column" not in st.session_state:
    st.session_state.phone_target_column = None
if "phone_file_uploaded" not in st.session_state:
    st.session_state.phone_file_uploaded = False
if "phone_df" not in st.session_state:
    st.session_state.phone_df = None
if "address_string_column" not in st.session_state:
    st.session_state.address_string_column = None
if "address_target_column" not in st.session_state:
    st.session_state.address_target_column = None
if "address_file_uploaded" not in st.session_state:
    st.session_state.address_file_uploaded = False
if "address_df" not in st.session_state:
    st.session_state.address_df = None
if "Negative_string_column" not in st.session_state:
    st.session_state.Negative_string_column = None
if "Negative_target_column" not in st.session_state:
    st.session_state.Negative_target_column = None
if "Negative_file_uploaded" not in st.session_state:
    st.session_state.Negative_file_uploaded = False
if "Negative_df" not in st.session_state:
    st.session_state.Negative_df = None
if "credentials" not in st.session_state:
    st.session_state.credentials = None
if "dataframes" not in st.session_state:
    st.session_state.dataframes = {}
if "current_file_id" not in st.session_state:
    st.session_state.current_file_id = None
if "search_results" not in st.session_state:
    st.session_state.search_results = {}
    # 폼 밖에서 사용할 변수들 초기화
if 'selected_sido' not in st.session_state:
    st.session_state.selected_sido = "전체"
if 'selected_sigungu' not in st.session_state:
    st.session_state.selected_sigungu = "전체"
if 'selected_org' not in st.session_state:
    st.session_state.selected_org = "전체"
if 'selected_position' not in st.session_state:
    st.session_state.selected_position = "전체"
if 'db_data' not in st.session_state:
    st.session_state.db_data = {}  # 각 DB별 데이터 저장
if 'db_unique_values' not in st.session_state:
    st.session_state.db_unique_values = {}  # 각 DB별 고유값 저장
if 'search_clicked' not in st.session_state:
    st.session_state.search_clicked = False  # 검색 버튼 클릭 여부
if 'current_filters' not in st.session_state:
    st.session_state.current_filters = {}  # 현재 적용된 필터
if 'filtered_unique_values' not in st.session_state:
    st.session_state.filtered_unique_values = {}  # 필터링된 고유값

def reset_session():
    """세션을 초기화하는 함수"""
    st.session_state.task = None
    st.session_state.phone_string_column = None
    st.session_state.phone_target_column = None
    st.session_state.phone_file_uploaded = False
    st.session_state.phone_df = None
    st.session_state.address_string_column = None
    st.session_state.address_target_column = None
    st.session_state.address_file_uploaded = False
    st.session_state.address_df = None  # 데이터프레임 초기화 추가
    st.session_state.Negative_string_column = None
    st.session_state.Negative_target_column = None
    st.session_state.Negative_file_uploaded = False
    st.session_state.Negative_df = None  # 데이터프레임 초기화 추가
    st.session_state.messages = []
    st.session_state.creds = None
    st.session_state.search_results = {}
    st.session_state.current_file_id = None
    st.session_state.dataframes = {}

if "code" in st.query_params:
    auth_code = st.query_params["code"]
    st.success("🔑 Google 인증 코드가 아래에 표시됩니다. 복사해서 사용하세요!")
    st.text_input("인증 코드", value=auth_code, label_visibility="collapsed")
# ✅ 사이드바 명령어 안내
st.sidebar.title("🧠 New Chat")
if st.sidebar.button("🔄 대화 초기화", key="new_chat_sidebar",use_container_width=True, type="primary") and "code" not in st.query_params:
    reset_session()
    st.success("✅ 대화가 초기화되었습니다.")
    st.rerun()


# ✅ 이전 대화 기록 표시 (채팅 UI)
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

if not st.session_state.credentials and "code" not in st.query_params:
    st.info("Google Drive 접근을 위해 인증이 필요합니다.")
    if st.button("🔐 Google Drive 인증 시작"):
        flow = Flow.from_client_config(
    client_config,
    scopes=SCOPES,
    redirect_uri=st.secrets["google"]["redirect_uris"]
)
        auth_url, _ = flow.authorization_url(prompt='consent')
        st.session_state.flow = flow
        st.markdown(f"[여기 클릭해서 인증하기]({auth_url})")

    auth_code = st.text_input("🔑 인증 코드 붙여넣기")
    if auth_code and "flow" in st.session_state:
        flow = st.session_state.flow
        flow.fetch_token(code=auth_code)
        creds = flow.credentials
        st.session_state.credentials = creds
        st.success("✅ 인증 성공!")

# ✅ 인증된 경우에만 작업 선택 UI 표시
if st.session_state.credentials:
    selected_task = st.selectbox("💬 수행할 작업을 선택하세요:", ["", "중복 확인", "주소 정제", "강성데이터삭제","Google_Driver_Search","DB_Search"])

    if selected_task and st.button("✅ 작업 선택"):
        st.session_state.task = selected_task
        st.session_state.messages.append({"role": "user", "content": f"📌 선택한 작업: {selected_task}"})

        if selected_task == "중복 확인":
            st.session_state.messages.append({"role": "assistant", "content": "🔤 문자열로 읽을 열을 입력해주세요. (예: '이름' 또는 '주소')"})
        elif selected_task == "주소 정제":
            st.session_state.messages.append({"role": "assistant", "content": "📍 주소 정제를 진행할 열을 입력해주세요!"})
        elif selected_task == "강성데이터삭제":
            st.session_state.messages.append({"role": "assistant", "content": "📍 삭제를 진행할 열을 입력해주세요!"})

        st.rerun()
#-------------------------------------------------------중복확인------------------------------------------------------------------------------------------------
# ✅ 2. phone 문자열로 읽을 열 선택
if st.session_state.task == "중복 확인" and st.session_state.phone_file_uploaded == False :
    uploaded_file = st.file_uploader("CSV 또는 Excel 파일 업로드", type=["csv", "xlsx"])

    if uploaded_file:
        ext = uploaded_file.name.split('.')[-1]
        file_bytes = BytesIO(uploaded_file.read())

        # 열 이름만 먼저 추출
        if ext == "csv":
            temp_df = pd.read_csv(file_bytes, nrows=0)
        else:
            temp_df = pd.read_excel(file_bytes, nrows=0)

        str_cols = st.multiselect("문자(str)로 읽을 열 선택", temp_df.columns.tolist())


        if str_cols:
            file_bytes.seek(0)
            if ext == "csv":
                df = pd.read_csv(file_bytes, dtype={col: str for col in str_cols}, low_memory=False)
            else:
                df = pd.read_excel(file_bytes, dtype={col: str for col in str_cols})
            
            st.subheader("📑 데이터 미리보기")
            st.dataframe(df.head())
            
            if st.button("➡️ 다음"):
                st.session_state.phone_df = df
                st.session_state.phone_file_uploaded = True
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": "✅ 파일이 업로드되었습니다! 중복 확인할 열을 입력해주세요."
                })
                st.rerun()  

 
if st.session_state.phone_file_uploaded and st.session_state.phone_target_column is None:
    available_cols = st.session_state.phone_df.columns.tolist()
    user_target_column = st.selectbox("🔍 중복 확인할 열을 선택하세요", [""] + available_cols, index=0)

    if user_target_column and user_target_column != "":
        st.session_state.phone_target_column = user_target_column
        st.session_state.messages.append({"role": "user", "content": user_target_column})
        st.session_state.messages.append({
            "role": "assistant",
            "content": f"⏳ '{user_target_column}' 열에서 중복을 확인 중입니다. 잠시만 기다려주세요!"
        })
        st.rerun()

# ✅ 5. 중복 확인 실행 및 결과 출력
if st.session_state.phone_file_uploaded and st.session_state.phone_target_column:
    df_phone = st.session_state.phone_df.copy()
    df_phone['중복_횟수'] = df_phone[st.session_state.phone_target_column].map(df_phone[st.session_state.phone_target_column].value_counts())
    df_phone['등장_순서'] = df_phone.groupby(st.session_state.phone_target_column).cumcount() + 1

    # ✅ 결과 메시지 추가
    st.session_state.messages.append({"role": "assistant", "content": "✅ 중복 확인이 완료되었습니다! 아래에서 결과를 확인하세요."})
    
    # ✅ 채팅 형식으로 출력
    with st.chat_message("assistant"):
        st.write(df_phone)

    # ✅ CSV 다운로드 버튼 추가
    csv_file = io.BytesIO()
    df_phone.to_csv(csv_file, index=False, encoding='utf-8-sig')
    csv_file.seek(0)

    st.download_button(
        label="💾 CSV로 저장하기",
        data=csv_file,
        file_name="중복_확인_결과.csv",
        mime="text/csv"
    )

    # ✅ 다시 시작 버튼 추가
    if st.button("🆕 새 채팅", key="new_chat_phone"):
        reset_session()
        st.success("✅ 대화가 초기화되었습니다.")
        st.rerun()

#----------------------------------------------------------주소정제---------------------------------------------------------------------------------------------
# ✅ 문자로 읽을 열이름 선택
if st.session_state.task == "주소 정제" and st.session_state.address_string_column is None:
    uploaded_file = st.file_uploader("CSV 또는 Excel 파일 업로드", type=["csv", "xlsx"])

    if uploaded_file:
        ext = uploaded_file.name.split('.')[-1]
        file_bytes = BytesIO(uploaded_file.read())

        # 열 이름만 먼저 추출
        if ext == "csv":
            temp_df = pd.read_csv(file_bytes, nrows=0)
        else:
            temp_df = pd.read_excel(file_bytes, nrows=0)

        str_cols = st.multiselect("문자(str)로 읽을 열 선택", temp_df.columns.tolist())


        if str_cols:
            file_bytes.seek(0)
            if ext == "csv":
                df = pd.read_csv(file_bytes, dtype={col: str for col in str_cols}, low_memory=False)
            else:
                df = pd.read_excel(file_bytes, dtype={col: str for col in str_cols})
            
            st.subheader("📑 데이터 미리보기")
            st.dataframe(df.head())
            
            if st.button("➡️ 다음"):
                st.session_state.address_df = df
                st.session_state.address_file_uploaded = True
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": "✅ 파일이 업로드되었습니다! 중복 확인할 열을 입력해주세요."
                })
                st.rerun() 


# 5. 정제 할 열이름 입력
if st.session_state.address_df is not None and st.session_state.address_target_column is None:
    available_cols = st.session_state.address_df.columns.tolist()
    user_target_column = st.selectbox("🔍 주소를 나눌 열을 선택하세요", [""] + available_cols, index=0)
    
    # "건너뛰기" 버튼 추가
    if st.button("🚶‍♂️ 건너뛰기"):
        st.session_state.address_target_column = "건너뛰기"
        st.session_state.messages.append({"role": "user", "content": "건너뛰기"})
        st.session_state.messages.append({"role": "assistant", "content": "⏳ 주소 정제를 건너뛰고 진행합니다."})
        st.rerun()

    # "건너뛰기" 외에 다른 열을 입력한 경우
    if user_target_column:
        if user_target_column not in st.session_state.address_df.columns:
            st.warning(f"⚠️ '{user_target_column}' 열이 데이터에 없습니다. 다시 입력해주세요!")
            st.session_state.messages.append({
                "role": "assistant", 
                "content": f"⚠️ '{user_target_column}' 열이 데이터에 없습니다. 가능한 열: {', '.join(st.session_state.address_df.columns)}"
            })
        else:
            st.session_state.address_target_column = user_target_column
            st.session_state.messages.append({"role": "user", "content": user_target_column})
            st.session_state.messages.append({"role": "assistant", "content": f"⏳ '{user_target_column}' 열에서 주소를 정제 중 입니다. 잠시만 기다려주세요!"})
            st.rerun()


#주소 정제 시작
if st.session_state.address_df is not None and st.session_state.address_target_column and st.session_state.address_target_column != "건너뛰기":
    df = st.session_state.address_df.copy()
    df['원본주소'] = df[st.session_state.address_target_column]
    df[st.session_state.address_target_column] = df[st.session_state.address_target_column].apply(map_city_to_two_letters)
    df[['시도', st.session_state.address_target_column]] = df[st.session_state.address_target_column].apply(lambda x: pd.Series(extract_and_remove_city(x)))
    df[['시군구', st.session_state.address_target_column]] = df[st.session_state.address_target_column].apply(lambda x: pd.Series(extract_and_remove_district(x)))
    df[['읍면동', st.session_state.address_target_column]] = df[st.session_state.address_target_column].apply(lambda x: pd.Series(split_address(x)))
    df.rename(columns={st.session_state.address_target_column: '세부주소'}, inplace=True)

    df['시도'] = df['시도'].str.replace(r'[^\w\s]', '', regex=True)
    df['시도'] = df['시도'].str.replace(r'\s+', '', regex=True)
    df['시군구'] = df['시군구'].astype(str).str.replace(r'\s+', '', regex=True)
    df['시군구'] = df['시군구'].str.replace(r'[^\w\s]', '', regex=True)
    df['시군구'] = df['시군구'].str.replace(r'\s+', '', regex=True)
    df['읍면동'] = df['읍면동'].str.replace(r'[^\w\s]', '', regex=True)
    df['읍면동'] = df['읍면동'].str.replace(r'\s+', '', regex=True)
    df['시도'].apply(mapping_city)
    df['시군구'].apply(mapping_districts)
    df = df.merge(df_hang, on=["시도", "시군구", "읍면동"], how="left")
    for index, row in df.iterrows():
    # Check if '행정동' is empty or NaN
        if pd.isna(row['행정동']) or row['행정동'].strip() == "":
            # Match '시도', '시군구', and '읍면동' from df to '시도', '시군구', '행정동' from df_hang
            match = df_hang[
                (df_hang['시도'] == row['시도']) & 
                (df_hang['시군구'] == row['시군구']) & 
                (df_hang['행정동'] == row['읍면동'])
            ]
            
            # If a match is found, update the '행정동' column in df
            if not match.empty:
                df.at[index, '행정동'] = match.iloc[0]['행정동']
    df["행정동"] = df["행정동"].fillna("F")
    df = df.merge(df_id, on=["시도", "시군구", "행정동"], how="left")
    df["ID"] = df["ID"].fillna("F")
    df = df[['원본주소', '시도', '시군구', '읍면동', '세부주소', '행정동', 'ID']]

    # ✅ 결과 메시지 추가
    st.session_state.messages.append({"role": "assistant", "content": "✅ 주소정제가 완료되었습니다! 아래에서 결과를 확인하세요."})
    
    # ✅ 채팅 형식으로 출력
    with st.chat_message("assistant"):
        st.write(df)

    # ✅ CSV 다운로드 버튼 추가
    csv_file = io.BytesIO()
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    csv_file.seek(0)

    st.download_button(
        label="📥 주소 정제 결과 다운로드",
        data=csv_file,
        file_name="주소_정제_결과.csv",
        mime="text/csv"
    )

    # ✅ 다시 시작 버튼 추가
    if st.button("🆕 새 채팅", key="new_chat_phone"):
        reset_session()
        st.rerun()

#주소 정제 건너뛰기 선택 후 
if st.session_state.address_df is not None and st.session_state.address_target_column == "건너뛰기":
    df = st.session_state.address_df.copy()
    df['시도'] = df['시도'].str.replace(r'[^\w\s]', '', regex=True)
    df['시도'] = df['시도'].str.replace(r'\s+', '', regex=True)
    df['시군구'] = df['시군구'].astype(str).str.replace(r'\s+', '', regex=True)
    df['시군구'] = df['시군구'].str.replace(r'[^\w\s]', '', regex=True)
    df['시군구'] = df['시군구'].str.replace(r'\s+', '', regex=True)
    df['읍면동'] = df['읍면동'].str.replace(r'[^\w\s]', '', regex=True)
    df['읍면동'] = df['읍면동'].str.replace(r'\s+', '', regex=True)
    df['시도'] = df['시도'].apply(mapping_city)
    df['시군구'] = df['시군구'].apply(mapping_districts)

    df = df.merge(df_hang, on=["시도", "시군구", "읍면동"], how="left")
    for index, row in df.iterrows():
    # Check if '행정동' is empty or NaN
        if pd.isna(row['행정동']) or row['행정동'].strip() == "":
            # Match '시도', '시군구', and '읍면동' from df to '시도', '시군구', '행정동' from df_hang
            match = df_hang[
                (df_hang['시도'] == row['시도']) & 
                (df_hang['시군구'] == row['시군구']) & 
                (df_hang['행정동'] == row['읍면동'])
            ]
            
            # If a match is found, update the '행정동' column in df
            if not match.empty:
                df.at[index, '행정동'] = match.iloc[0]['행정동']
    df["행정동"] = df["행정동"].fillna("F")
    df = df.merge(df_id, on=["시도", "시군구", "행정동"], how="left")
    df["ID"] = df["ID"].fillna("F")
    df = df[['시도', '시군구', '읍면동', '세부주소', '행정동', 'ID']]

    # ✅ 결과 메시지 추가
    st.session_state.messages.append({"role": "assistant", "content": "✅ 주소정제가 완료되었습니다! 아래에서 결과를 확인하세요."})
    
    # ✅ 채팅 형식으로 출력
    with st.chat_message("assistant"):
        st.write(df)

    # ✅ CSV 다운로드 버튼 추가
    csv_file = io.BytesIO()
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    csv_file.seek(0)

    st.download_button(
        label="💾 CSV로 저장하기",
        data=csv_file,
        file_name="주소_정제_결과.csv",
        mime="text/csv"
    )

    # ✅ 다시 시작 버튼 추가
    if st.button("🆕 새 채팅", key="new_chat_phone"):
        reset_session()
        st.success("✅ 대화가 초기화되었습니다.")
        st.rerun()

#----------------------------------------------------------강성DB삭제---------------------------------------------------------------------------------------------
# ✅ 문자로 읽을 열이름 선택
if st.session_state.task == "주소 정제" and st.session_state.address_string_column is None:
    uploaded_file = st.file_uploader("CSV 또는 Excel 파일 업로드", type=["csv", "xlsx"])

    if uploaded_file:
        ext = uploaded_file.name.split('.')[-1]
        file_bytes = BytesIO(uploaded_file.read())

        # 열 이름만 먼저 추출
        if ext == "csv":
            temp_df = pd.read_csv(file_bytes, nrows=0)
        else:
            temp_df = pd.read_excel(file_bytes, nrows=0)

        str_cols = st.multiselect("문자(str)로 읽을 열 선택", temp_df.columns.tolist())


        if str_cols:
            file_bytes.seek(0)
            if ext == "csv":
                df = pd.read_csv(file_bytes, dtype={col: str for col in str_cols}, low_memory=False)
            else:
                df = pd.read_excel(file_bytes, dtype={col: str for col in str_cols})
            
            st.subheader("📑 데이터 미리보기")
            st.dataframe(df.head())
            
            if st.button("➡️ 다음"):
                st.session_state.Negative_df = df
                st.session_state.Negative_file_uploaded = True
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": "✅ 파일이 업로드되었습니다! 중복 확인할 열을 입력해주세요."
                })
                st.rerun() 

if st.session_state.Negative_df is not None and st.session_state.Negative_target_column is None:
    available_cols = st.session_state.Negative_df.columns.tolist()
    user_target_column = st.selectbox("🔍 주소를 나눌 열을 선택하세요", [""] + available_cols, index=0)
    
    if user_target_column:
        st.session_state.Negative_target_column = user_target_column
        st.session_state.messages.append({"role": "user", "content": user_target_column})
        st.session_state.messages.append({"role": "assistant", "content": f"⏳ '{user_target_column}' 열에서 삭제를 진행 중입니다. 잠시만 기다려주세요!"})
        st.rerun()

if st.session_state.Negative_df is not None and st.session_state.Negative_target_column:
    df = st.session_state.Negative_df.copy()
    creds = st.session_state.credentials

#-----------------------------------------------------------강성DB불러오기-----------------------------------------------------
    uploaded_files = st.file_uploader("080수신거부 엑셀 파일을 업로드하세요", type=["xls","xlsx"], accept_multiple_files=True)

    if uploaded_files:
        df_list = []  # 데이터프레임을 저장할 리스트
        
        # 파일 하나씩 읽어서 처리
        for uploaded_file in uploaded_files:
            try:
                # 업로드된 파일 읽기
                temp_df  = pd.read_csv(uploaded_file, sep="\t", encoding="cp949", skiprows=1, on_bad_lines='skip')
                df_list.append(temp_df )
            except Exception as e:
                st.error(f"파일 '{uploaded_file.name}' 처리 실패 - 오류: {e}")

        # 데이터프레임 하나로 합치기
        if df_list:
            call_refusal_080  = pd.concat(df_list, ignore_index=True)
            call_refusal_080 ['전화번호'] = call_refusal_080 ['전화번호'].str.replace(r'\D', '', regex=True)
            st.write("080수신거부:", call_refusal_080 .head())
        else:
            st.warning("파일을 제대로 업로드하거나 읽지 못했습니다.")
        if creds is not None:
            gc, drive_service, sheets_service = get_google_services(creds)

            warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

            current_year = date.now().year

            # Google Drive에서 최신 엑셀 파일 가져오기
            folder_id = '1NiTuONWRv7jWsqwmAzY0qEJkdls3__AO'
            exclude_sheets = ['드랍', '픽업', '자통당TM 구분']

            response = drive_service.files().list(
                q=f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
                spaces='drive',
                fields='files(id, name, createdTime)',
                orderBy='createdTime desc'
            ).execute()

            files = response.get('files', [])

            if not files:
                st.error("해당 폴더에 .xlsx 파일이 없습니다.")
            else:
                # 가장 최신 파일 다운로드
                latest_file = files[0]
                file_id = latest_file['id']
                file_name = latest_file['name']
                st.write(f"가장 최신 파일: {file_name}")

                request = drive_service.files().get_media(fileId=file_id)
                file_stream = io.BytesIO()
                downloader = MediaIoBaseDownload(file_stream, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()

                file_stream.seek(0)

                # 모든 시트 읽기 (특정 시트 제외)
                excel_file = pd.ExcelFile(file_stream)
                sheets = [sheet for sheet in excel_file.sheet_names if sheet not in exclude_sheets]

                # 로딩바 표시: 진행 상황을 0부터 100까지 업데이트
                progress_bar = st.progress(0)  # 로딩바 초기화

                dtype_mapping = {
                    '연락처': str,
                    '고유값': str,
                    '발신 전화번호': str,
                    '픽업코드': str,
                    '드랍코드': str,
                    '결번': str,
                    '부재중': str,
                    '이미 가입': str,
                    '가입 원함': str,
                    '미온': str,
                    '가입 거절': str,
                    '삭제 요청': str,
                    '타인': str,
                    '투표 긍정': str,
                    '다른 당 지지': str,
                    '긍정': str,
                    '번호변경': str
                }

                # 각 시트를 읽을 때마다 진행 상태 업데이트
                outcall_df = pd.DataFrame()  # 빈 데이터프레임으로 시작
                total_sheets = len(sheets)
                for idx, sheet in enumerate(sheets):
                    sheet_df = excel_file.parse(sheet, dtype=dtype_mapping)
                    outcall_df = pd.concat([outcall_df, sheet_df], ignore_index=True)
                    # 진행 상태 업데이트 (시트마다 진행도 100/전체시트수로 나누기)
                    progress_bar.progress(int(((idx + 1) / total_sheets) * 100))

                # 진행 상황이 끝났을 때 (100%)
                progress_bar.progress(100)

                outcall_df = outcall_df[outcall_df['삭제 요청'] == 1]
                st.write("아웃콜삭제요청:", outcall_df .head())
#----------------------------------------------------------------------------------------------------------------
                # 가져올 Google 스프레드시트 파일 ID
                SPREADSHEET_ID = '1O5IaTXvBQnVTSJhrhPlMI45LxHcL2BkHCHO6IhNA7Bs'

                # 1. 스프레드시트 열기
                sh = gc.open_by_key(SPREADSHEET_ID)

                # 2. 특정 시트 데이터 가져오기 (예: 첫 번째 시트)
                worksheet = sh.get_worksheet(0)  # 0은 첫 번째 시트

                # 3. 모든 데이터 가져와 pandas DataFrame으로 변환
                data = worksheet.get_all_values()  # 리스트 형태로 가져오기
                Unsubscribed_df = pd.DataFrame(data[1:], columns=data[0])  # 첫 번째 행을 헤더로 사용
                st.write("탈퇴자:", Unsubscribed_df .head())
#----------------------------------------------------------------------------------------------------------------

                # 이후 데이터 처리
                df = df[~df[st.session_state.Negative_target_column].isin(outcall_df['연락처'])]
                df = df[~df[st.session_state.Negative_target_column].isin(call_refusal_080['전화번호'])]
                df = df[~df[st.session_state.Negative_target_column].isin(Unsubscribed_df['phone'])]

                # ✅ 결과 메시지 추가
                st.session_state.messages.append({"role": "assistant", "content": "✅ 삭제가 완료되었습니다! 아래에서 결과를 확인하세요."})
                
                # ✅ 채팅 형식으로 출력
                with st.chat_message("assistant"):
                    st.write(df)

                # ✅ CSV 다운로드 버튼 추가
                csv_file = io.BytesIO()
                df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                csv_file.seek(0)

                st.download_button(
                    label="💾 CSV로 저장하기",
                    data=csv_file,
                    file_name="중복_확인_결과.csv",
                    mime="text/csv"
                )

                # ✅ 다시 시작 버튼 추가
                if st.button("🆕 새 채팅", key="new_chat_phone"):
                    reset_session()
                    st.success("✅ 대화가 초기화되었습니다.")
                    st.rerun()

#----------------------------------------------------------Google_Driver_Search---------------------------------------------------------------------------------------------
if st.session_state.task == "Google_Driver_Search" :
    creds = st.session_state.credentials
    service = build('drive', 'v3', credentials=creds)

    query = st.text_input("🔍 검색할 키워드")
    if query:
        # 여러 파일 형식 지원
        mime_types = [
            "application/vnd.google-apps.spreadsheet",  # Google 스프레드시트
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # Excel
            "text/csv"  # CSV
        ]
        
        # 검색 결과가 캐시에 없으면 새로 검색
        if query not in st.session_state.search_results:
            matching_files = []
            for mime in mime_types:
                search_query = f"fullText contains '{query}' and mimeType='{mime}'"
                results = service.files().list(
                    q=search_query,
                    fields="files(id, name, mimeType, parents, description)"
                ).execute()
                matching_files.extend(results.get('files', []))
            
            st.session_state.search_results[query] = matching_files
        else:
            matching_files = st.session_state.search_results[query]

        if matching_files:
            st.write(f"💡 총 {len(matching_files)}개의 파일이 검색되었습니다.")
            
            # 파일 정보를 표시하는 테이블 생성
            file_info = []
            for i, f in enumerate(matching_files):
                file_info.append({
                    "번호": i+1,
                    "파일명": f['name'],
                    "파일 형식": f['mimeType'].split('.')[-1],
                    "ID": f['id'],
                })
            
            # 검색 결과 테이블 표시 (선택사항)
            with st.expander("📋 검색된 파일 목록 보기", expanded=False):
                st.dataframe(pd.DataFrame(file_info), use_container_width=True)
                        
            file_names = [f"{i+1}. {f['name']} ({f['mimeType'].split('.')[-1]})" for i, f in enumerate(matching_files)]
            selected_idx = st.selectbox("📄 파일 선택", range(len(matching_files)), 
                                       format_func=lambda i: file_names[i],
                                       key="file_selector")

            selected_file = matching_files[selected_idx]
            file_id = selected_file['id']
            file_name = selected_file['name']
            mime_type = selected_file['mimeType']
            parent_ids = selected_file.get('parents', [])
            description = selected_file.get('description', '')
            
            # 파일 경로 정보 표시
            if parent_ids:
                folder_id = parent_ids[0]
                folder_url = f"https://drive.google.com/drive/folders/{folder_id}"
                st.markdown(f"📂 [파일이 위치한 폴더 열기]({folder_url})")
            
            # 새 파일 선택 시에만 로딩
            if file_id not in st.session_state.dataframes:
                with st.spinner(f"'{file_name}' 파일을 로딩 중..."):
                    fh = BytesIO()
                    
                    try:
                        if mime_type == "application/vnd.google-apps.spreadsheet":
                            # Google 스프레드시트 처리
                            request = service.files().export_media(
                                fileId=file_id,
                                mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                            )
                            downloader = MediaIoBaseDownload(fh, request)
                            done = False
                            while not done:
                                status, done = downloader.next_chunk()
                            fh.seek(0)
                            # 모든 시트 로드
                            sheets = pd.read_excel(fh, sheet_name=None, dtype=str)
                            
                        elif mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                            # Excel 파일 처리
                            request = service.files().get_media(fileId=file_id)
                            downloader = MediaIoBaseDownload(fh, request)
                            done = False
                            while not done:
                                status, done = downloader.next_chunk()
                            fh.seek(0)
                            # 모든 시트 로드
                            sheets = pd.read_excel(fh, sheet_name=None, dtype=str)
                            
                        elif mime_type == "text/csv":
                            # CSV 파일 처리
                            request = service.files().get_media(fileId=file_id)
                            downloader = MediaIoBaseDownload(fh, request)
                            done = False
                            while not done:
                                status, done = downloader.next_chunk()
                            fh.seek(0)
                            df = pd.read_csv(fh, dtype=str)
                            sheets = {"CSV 데이터": df}  # CSV는 단일 시트로 처리
                        
                        # 검색어 포함된 위치 찾기 및 표시
                        matching_sheets = {}
                        search_results_info = {}
                        
                        for sheet_name, sheet_df in sheets.items():
                            sheet_df.columns = sheet_df.columns.map(str)  # 컬럼명을 문자열로 변환
                            
                            # 검색어가 포함된 위치 찾기
                            found_in_content = False
                            matching_columns = []
                            
                            for col in sheet_df.columns:
                                if sheet_df[col].astype(str).str.contains(query, case=False, na=False).any():
                                    found_in_content = True
                                    matching_columns.append(col)
                            
                            search_results_info[sheet_name] = {
                                "found_in_content": found_in_content,
                                "matching_columns": matching_columns,
                                "found_in_filename": query.lower() in file_name.lower(),
                                "found_in_sheetname": query.lower() in sheet_name.lower(),
                                "found_in_description": description and query.lower() in description.lower()
                            }
                            
                            # 모든 시트 포함 (필터링은 나중에 UI에서 수행)
                            matching_sheets[sheet_name] = sheet_df
                        
                        st.session_state.dataframes[file_id] = {
                            "sheets": matching_sheets,
                            "search_info": search_results_info
                        }
                            
                    except Exception as e:
                        st.error(f"❌ 파일 로딩 중 오류: {e}")
            
            # 현재 파일 ID 업데이트
            st.session_state.current_file_id = file_id
            
            # 선택된 파일의 데이터프레임 가져오기
            if file_id in st.session_state.dataframes:
                file_data = st.session_state.dataframes[file_id]
                sheets = file_data["sheets"]
                search_info = file_data["search_info"]
                
                # 여러 시트가 있는 경우 시트 선택 옵션 제공
                if len(sheets) > 1:
                    # 시트별 검색 결과 표시
                    sheet_options = []
                    for sheet_name in sheets.keys():
                        info = search_info[sheet_name]
                        if info["found_in_content"]:
                            sheet_options.append(f"{sheet_name} ✓")
                        else:
                            sheet_options.append(sheet_name)
                    
                    selected_sheet_idx = st.selectbox(
                        "📄 시트 선택 (✓ 표시는 내용에 검색어 포함)",
                        range(len(sheet_options)),
                        format_func=lambda i: sheet_options[i],
                        key=f"sheet_selector_{file_id}"
                    )
                    selected_sheet = list(sheets.keys())[selected_sheet_idx]
                    df = sheets[selected_sheet]
                    st.subheader(f"📊 {file_name} - {selected_sheet} 미리보기")
                else:
                    # 단일 시트(CSV 등)인 경우
                    selected_sheet = list(sheets.keys())[0]
                    df = sheets[selected_sheet]
                    st.subheader(f"📊 {file_name} 미리보기")
                
                # 검색어 발견 위치 정보 표시
                info = search_info[selected_sheet]
                search_details = []
                
                if info["found_in_filename"]:
                    search_details.append("✓ 파일명에서 발견")
                if info["found_in_sheetname"]:
                    search_details.append("✓ 시트명에서 발견")
                if info["found_in_description"]:
                    search_details.append("✓ 파일 설명에서 발견")
                if info["found_in_content"]:
                    matching_cols = ", ".join(info["matching_columns"])
                    search_details.append(f"✓ 데이터 내용에서 발견 (열: {matching_cols})")
                
                if search_details:
                    st.info(f"🔍 '{query}' 검색 결과: " + " | ".join(search_details))
                else:
                    st.warning(f"⚠️ 이 시트에서는 '{query}'를 직접 찾을 수 없습니다. Google Drive 검색 알고리즘이 다른 기준(메타데이터 등)으로 이 파일을 검색 결과에 포함했을 수 있습니다.")
                
                # 데이터가 많은 경우 페이징 처리
                if len(df) > 1000000:
                    page_size = 1000000
                    total_pages = (len(df) - 1) // page_size + 1
                    page = st.number_input("📄 페이지 번호", min_value=1, max_value=total_pages, value=1, key=f"page_{file_id}")
                    start = (page - 1) * page_size
                    end = start + page_size
                    st.write(f"총 {len(df):,}행 중 {start+1:,}~{min(end, len(df)):,}행 표시")
                    display_df = df.iloc[start:end]
                else:
                    display_df = df
                
                # 데이터프레임 표시
                st.dataframe(display_df, use_container_width=True)
                
                # 검색어 하이라이트 옵션 (시각적으로 도움)
                if info["found_in_content"] and st.checkbox("🔦 검색어 하이라이트", key=f"highlight_{file_id}"):
                    matching_cols = info["matching_columns"]
                    if matching_cols:
                        st.write("검색어가 포함된 행:")
                        highlight_rows = None
                        
                        for col in matching_cols:
                            mask = display_df[col].astype(str).str.contains(query, case=False, na=False)
                            if highlight_rows is None:
                                highlight_rows = mask
                            else:
                                highlight_rows = highlight_rows | mask
                        
                        if highlight_rows.any():
                            st.dataframe(display_df[highlight_rows], use_container_width=True)
                        else:
                            st.info("현재 페이지에서는 검색어가 포함된 행이 없습니다.")
                
                # 기본 통계 정보 옵션 추가
                with st.expander("📊 기본 통계 정보 보기"):
                    # 숫자형 열 식별 시도
                    numeric_cols = []
                    for col in df.columns:
                        try:
                            pd.to_numeric(df[col])
                            numeric_cols.append(col)
                        except:
                            pass
                    
                    if numeric_cols:
                        st.write("숫자형 열에 대한 기본 통계:")
                        st.dataframe(df[numeric_cols].describe(), use_container_width=True)
                    else:
                        st.write("데이터에 숫자형 열이 없습니다.")
                    
                    # 범주형 열에 대한 고유값 개수
                    st.write("각 열의 고유값 개수:")
                    st.dataframe(pd.DataFrame({'고유값 개수': df.nunique()}).sort_values('고유값 개수', ascending=False))
                
                # 필터링 기능 추가
                with st.expander("🔍 데이터 필터링"):
                    col1, col2 = st.columns(2)
                    with col1:
                        filter_column = st.selectbox("필터링할 열 선택", df.columns, key=f"filter_col_{file_id}")
                    with col2:
                        unique_values = df[filter_column].unique()
                        if len(unique_values) > 30:  # 값이 너무 많으면 텍스트 입력으로
                            filter_value = st.text_input("검색할 값 입력", key=f"filter_val_{file_id}")
                            if filter_value:
                                filtered_df = df[df[filter_column].str.contains(filter_value, case=False, na=False)]
                                st.write(f"결과: {len(filtered_df):,}행")
                                st.dataframe(filtered_df)
                        else:  # 값이 적으면 다중 선택으로
                            filter_values = st.multiselect("값 선택", unique_values, key=f"filter_vals_{file_id}")
                            if filter_values:
                                filtered_df = df[df[filter_column].isin(filter_values)]
                                st.write(f"결과: {len(filtered_df):,}행")
                                st.dataframe(filtered_df)

                if st.button("💾 CSV로 저장하기", key=f"save_{file_id}"):
                    csv = df.to_csv(index=False).encode('utf-8-sig')
                    st.download_button(
                        label="📥 결과 CSV 다운로드",
                        data=csv,
                        file_name=f"{file_name}_{selected_sheet}.csv",
                        mime='text/csv'
                    )
        else:
            st.warning(f"❗️ '{query}'에 대한 검색 결과가 없습니다.")
            st.info("💡 다른 키워드로 검색해 보세요. Google Drive 검색은 파일 내용, 파일명, 설명 등에서 검색합니다.")
#----------------------------------------------------------DB_Search---------------------------------------------------------------------------------------------
if st.session_state.task == "DB_Search":
    # 폴더명과 ID 매핑
    key = {
        "서명DB": "17DHCAxDOWVP_TvNfeOt9GxhvGOqOrJod",
        "조직DB": "1G1oprZ6WZG5JBRcBI3ePYlSGr-osX1be",
        "행사DB": "1QH-RnXWITvWTylKgEzkaXWG5VGqrbdtB"
    }

    # DB별 필드 설정 (각 DB마다 다른 검색 필드 제공)
    db_fields = {
        "서명DB": ["이름", "연락처", "시도", "시군구", "읍면동"],
        "조직DB": ["이름", "연락처", "시도", "시군구", "읍면동", "직책", "조직명"],
        "행사DB": ["이름", "연락처", "시도", "시군구", "읍면동", "행사명", "행사시작일"]
    }


    # 폴더 선택
    user_target_folder = st.selectbox("📂 DB 선택", [""] + list(key.keys()), key="folder_select")

    if user_target_folder:
        # 선택된 DB가 바뀌면 검색 결과 초기화
        if 'last_selected_db' not in st.session_state or st.session_state.last_selected_db != user_target_folder:
            st.session_state.search_clicked = False
            st.session_state.current_filters = {}
            st.session_state.selected_sido = "전체"
            st.session_state.selected_sigungu = "전체"
            st.session_state.selected_org = "전체"
            st.session_state.selected_position = "전체"
            st.session_state.last_selected_db = user_target_folder

        # 해당 DB 데이터를 아직 로드하지 않았다면 로드
        if user_target_folder not in st.session_state.db_data:
            with st.spinner(f"{user_target_folder} 데이터를 로드 중입니다..."):
                # Google Drive 서비스 연결
                creds = st.session_state.credentials
                service = build('drive', 'v3', credentials=creds)

                # 해당 폴더 내 csv 파일 병합
                folder_id = key[user_target_folder]
                files = list_csv_files_in_folder(service, folder_id=folder_id)
                df = load_and_merge_csv(service, files)
                
                # 세션 상태에 저장
                st.session_state.db_data[user_target_folder] = df
                
                # 고유 값 계산 및 저장
                unique_values = {}
                for field in db_fields[user_target_folder]:
                    if field in df.columns:
                        unique_values[field] = ["전체"] + sorted(df[field].dropna().unique().tolist())
                st.session_state.db_unique_values[user_target_folder] = unique_values
                
                st.success(f"{user_target_folder} 로드 완료! ({len(df):,}개 데이터)")

        # 이미 로드된 데이터 사용
        df = st.session_state.db_data[user_target_folder]
        unique_values = st.session_state.db_unique_values[user_target_folder]
        
       
        # DB별 맞춤형 검색 UI 표시
        st.subheader(f"🔍 {user_target_folder} 검색")
        
        # 폼 밖에서 시도/시군구/읍면동 및 조직명/직책 필터링
        # 시도 선택을 폼 바깥에서 처리
        if "시도" in unique_values:
            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:

                selected_sido = st.selectbox("🏙️ 시도 선택", unique_values["시도"], key="sido_outside_form")
                st.session_state.selected_sido = selected_sido
            
           
                if selected_sido != "전체":
                    df_filtered_sigungu = df[df["시도"] == selected_sido]
                    sigungu_list = ["전체"] + sorted(df_filtered_sigungu["시군구"].dropna().unique().tolist())
                else:
                    sigungu_list = unique_values["시군구"]

            with col2:    
                # 시군구 선택
                selected_sigungu = st.selectbox("🏞️ 시군구 선택", sigungu_list, key="sigungu_outside_form")
                st.session_state.selected_sigungu = selected_sigungu
                
                # 선택된 시도와 시군구에 따라 읍면동 목록 필터링
                if selected_sido != "전체" and selected_sigungu != "전체":
                    df_filtered_emd = df[(df["시도"] == selected_sido) & (df["시군구"] == selected_sigungu)]
                    emd_list = ["전체"] + sorted(df_filtered_emd["읍면동"].dropna().unique().tolist())
                else:
                    emd_list = unique_values["읍면동"]
            
            with col3:
                selected_emd = st.selectbox("🏡 읍면동 선택", emd_list, key="emd_outside_form")
                st.session_state.selected_emd = selected_emd
                
        # 조직DB인 경우 조직명/직책 필터링
        if user_target_folder == "조직DB":
            col1, col2 = st.columns(2)
            with col1:
                # 조직명 선택
                selected_org = st.selectbox("🏢 조직명 선택", unique_values["조직명"], key="org_outside_form")
                st.session_state.selected_org = selected_org
                
                # 선택된 조직명에 따라 직책 목록 필터링
                if selected_org != "전체":
                    df_filtered_position = df[df["조직명"] == selected_org]
                    position_list = ["전체"] + sorted(df_filtered_position["직책"].dropna().unique().tolist())
                else:
                    position_list = unique_values["직책"]
            
            with col2:
                # 직책 선택
                selected_position = st.selectbox("👑 직책 선택", position_list, key="position_outside_form")
                st.session_state.selected_position = selected_position
                
                # 선택된 직책에 따라 조직명 목록 필터링 (이미 조직명이 선택되어 있으면 할 필요 없음)
                if selected_position != "전체" and selected_org == "전체":
                    df_filtered_org = df[df["직책"] == selected_position]
                    updated_org_list = ["전체"] + sorted(df_filtered_org["조직명"].dropna().unique().tolist())
                    # 이 부분은 실제로 적용되지 않음 (동시에 업데이트 불가능)
        
        # 필터 입력값을 저장할 딕셔너리
        filters = {}
        
        # 실제 검색을 위한 폼
        with st.form(key=f'search_form_{user_target_folder}'):
            if user_target_folder == "서명DB":
                col1, col2 = st.columns([1, 2])
                with col1:
                    filters["이름"] = st.text_input("👤 이름", placeholder="예: 홍길동", key="name_input")
                with col2:
                    filters["연락처"] = st.text_input("📱 연락처", placeholder="숫자만 입력", key="phone_input")
                
                
                
                # 실제 선택값은 폼 밖에서 선택한 값을 숨겨진 필드로 저장
                filters["시도"] = st.session_state.selected_sido
                filters["시군구"] = st.session_state.selected_sigungu
                filters["읍면동"] = st.session_state.selected_emd
                
            elif user_target_folder == "조직DB":
                col1, col2 = st.columns([1, 2])
                with col1:
                    filters["이름"] = st.text_input("👤 이름", placeholder="예: 홍길동", key="name_input")
                with col2:
                    filters["연락처"] = st.text_input("📱 연락처", placeholder="숫자만 입력", key="phone_input")
                

                
                # 실제 선택값은 폼 밖에서 선택한 값 사용
                filters["시도"] = st.session_state.selected_sido
                filters["시군구"] = st.session_state.selected_sigungu
                filters["읍면동"] = st.session_state.selected_emd
                filters["조직명"] = st.session_state.selected_org
                filters["직책"] = st.session_state.selected_position
                
            elif user_target_folder == "행사DB":
                col1, col2 = st.columns([1, 2])
                with col1:
                    filters["이름"] = st.text_input("👤 이름", placeholder="예: 홍길동", key="name_input")
                with col2:
                    filters["연락처"] = st.text_input("📱 연락처", placeholder="숫자만 입력", key="phone_input")
                
                                
                # 실제 선택값은 폼 밖에서 선택한 값 사용
                filters["시도"] = st.session_state.selected_sido
                filters["시군구"] = st.session_state.selected_sigungu
                filters["읍면동"] = st.session_state.selected_emd
                
                # 행사DB 전용 필드
                col6, col7 = st.columns(2)
                with col6:
                    if "행사명" in unique_values:
                        filters["행사명"] = st.selectbox("🎪 행사명", unique_values["행사명"], key="event_select")
                with col7:
                    if "행사시작일" in df.columns:
                        st.write("📅 행사일자 범위")
                        date_col1, date_col2 = st.columns(2)
                        with date_col1:
                            start_date = st.date_input("시작일",value=(date(1900,1,1)), key="start_date")
                        with date_col2:
                            end_date = st.date_input("종료일", key="end_date")
                        filters["행사_시작일"] = start_date
                        filters["행사_종료일"] = end_date
            
            # 검색 버튼
            search_button = st.form_submit_button(label="🔍 검색", use_container_width=True)
            
            # 검색 버튼 클릭 시 세션 상태 업데이트
            if search_button:
                st.session_state.search_clicked = True
                st.session_state.current_filters = filters.copy()  # 현재 필터 저장

        # 검색 버튼이 클릭되었을 때만 결과 표시
        if st.session_state.search_clicked:
            # 세션에 저장된 필터 사용
            applied_filters = st.session_state.current_filters
            
            # 필터링 로직 적용
            filtered = df.copy()
            
            # 텍스트 필드 필터링
            for field in ["이름", "연락처"]:
                if field in applied_filters and applied_filters[field]:
                    filtered = filtered[filtered[field].str.contains(applied_filters[field], na=False)]
            
            # 카테고리 필드 필터링 ('전체'가 아닌 경우에만)
            for field in ["시도", "시군구", "읍면동", "직책", "조직명", "행사명"]:
                if field in applied_filters and applied_filters[field] != "전체":
                    filtered = filtered[filtered[field] == applied_filters[field]]
            
            # 날짜 필드 필터링 (DB별 특수 처리)
            if user_target_folder == "행사DB" and "행사시작일" in df.columns:
                if applied_filters.get("행사_시작일") and applied_filters.get("행사_종료일"):
                    # 날짜 형식 변환이 필요할 수 있음
                    try:
                        # 날짜 형식 자동 감지 및 변환
                        if isinstance(filtered["행사시작일"].iloc[0], str):
                            filtered = filtered[(filtered["행사시작일"] >= applied_filters["행사_시작일"].strftime("%Y-%m-%d")) & 
                                               (filtered["행사시작일"] <= applied_filters["행사_종료일"].strftime("%Y-%m-%d"))]
                        else:
                            # 이미 날짜 객체인 경우
                            filtered = filtered[(filtered["행사시작일"] >= applied_filters["행사_시작일"]) & 
                                               (filtered["행사시작일"] <= applied_filters["행사_종료일"])]
                    except:
                        st.warning("행사일자 필터링 중 오류가 발생했습니다. 날짜 형식을 확인해주세요.")

            # 결과 표시
            st.write(f"검색 결과: {len(filtered):,}개 행")
            
            # 페이지 나누기
            if len(filtered) > 500000:
                page_size = 500000
                total_pages = (len(filtered) - 1) // page_size + 1
                page = st.number_input("📄 페이지 번호", min_value=1, max_value=total_pages, value=1, key="page_number")
                start = (page - 1) * page_size
                end = start + page_size
                st.write(f"총 {len(filtered):,}행 중 {start+1:,} ~ {min(end, len(filtered)):,}행 표시 중")
                display_df = filtered.iloc[start:end]
            else:
                display_df = filtered

            # 데이터프레임 표시
            st.dataframe(display_df)

            # 다운로드 버튼
            csv_download = display_df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button("📥 검색 결과 다운로드", data=csv_download, 
                              file_name=f"{user_target_folder}_검색결과.csv", 
                              mime="text/csv")