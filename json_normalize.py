import pandas as pd
import json

source_file = r"C:\Users\클루커스\Desktop\src\scms\blob_list.json"
target_file = r"C:\Users\클루커스\Desktop\src\scms\blob_list.csv"
# JSON 파일 읽기
with open(source_file, encoding="utf-8") as file:
    data = json.load(file)

# JSON 데이터를 DataFrame으로 변환
df = pd.json_normalize(data)

# CSV 파일로 저장
df.to_csv(target_file, index=False, encoding="utf-8-sig")
