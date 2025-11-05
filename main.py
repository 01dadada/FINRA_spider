import argparse
import time

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.exceptions import RequestException

load_dotenv()

import os

TOKEN_URL = os.getenv("TOKEN_URL", "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token")
BASE_URL = os.getenv("BASE_URL", "https://api.finra.org/data/group/OTCMarket/name/")
LIMIT = int(os.getenv("limit", "5000"))
RETRY_DELAY_SECONDS = int(os.getenv("retry_delay_seconds", "60"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
TOKEN_REFRESH_INTERVAL = int(os.getenv("TOKEN_REFRESH_INTERVAL", "300"))
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SAVE_INTERVEL = int(os.getenv("SAVE_INTERVEL", "100"))
DATASET = os.getenv("DATASET", "monthlySummary")


def main():
    all_data = []

    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="要获取的数据集名称", default="monthlySummary")
    parser.add_argument("--save_intervel", required=False, help="保存间隔", default=SAVE_INTERVEL)

    parser.add_argument("--limit", required=False, help="每次请求的数据量限制", default=LIMIT)

    parser.add_argument(
        "--retry_delay_seconds", required=False, help="速率限制后的重试等待时间", default=RETRY_DELAY_SECONDS
    )
    parser.add_argument("--max_retries", required=False, help="网络错误的重试次数", default=MAX_RETRIES)
    parser.add_argument(
        "--token_refresh_interval", required=False, help="Token 刷新间隔", default=TOKEN_REFRESH_INTERVAL
    )
    parser.add_argument("--username", required=False, help="用户名", default=USERNAME)
    parser.add_argument("--password", required=False, help="密码", default=PASSWORD)
    args = parser.parse_args()

    dataset = args.dataset
    save_intervel = args.save_intervel
    limit = args.limit
    retry_delay_seconds = args.retry_delay_seconds
    max_retries = args.max_retries
    token_refresh_interval = args.token_refresh_interval
    username = args.username
    password = args.password

    def get_access_token():
        TOKEN_PAYLOAD = {
            "grant_type": "client_credentials",
            "apiclientid": username,
            "apiclientsecret": password,
        }
        token_headers = {"Accept": "application/json", "Content-Type": "application/json"}

        print("--- 🔄 正在请求新的 Access Token ---")

        for attempt in range(max_retries):
            try:
                response = requests.post(TOKEN_URL, auth=(username, password), data=TOKEN_PAYLOAD, timeout=30)
                response.raise_for_status()
                token_data = response.json()
                access_token = token_data.get("access_token")

                if access_token:
                    print("--- ✅ Access Token 获取成功 ---")
                    return access_token, time.time()
                else:
                    print(f"--- ❌ Token 响应中缺少 'access_token' 字段: {token_data} ---")
                    time.sleep(retry_delay_seconds)

            except RequestException as e:
                print(f"--- ⚠️ 网络错误，尝试第 {attempt + 1}/{max_retries} 次重试：{e} ---")
                time.sleep(retry_delay_seconds)

    offset = 0
    url = f"{BASE_URL}{dataset}"
    c = 0
    current_access_token, token_acquisition_time = get_access_token()

    while True:
        c = c + 1
        if time.time() - token_acquisition_time > token_refresh_interval:
            try:
                current_access_token, token_acquisition_time = get_access_token()
            except Exception as e:
                print(f"--- ❌ Token 更新失败，跳过 {dataset}：{e} ---")
                break  # 跳过当前数据集
        data_headers = {"accept": "application/json", "Authorization": f"Bearer {current_access_token}"}
        param = {
            "limit": limit,
            "offset": offset,
        }
        print(f"  ➡️ 请求 {dataset} offset: {offset}")
        try:
            response = requests.get(url, headers=data_headers, params=param, timeout=60)
            response.raise_for_status()
            data = response.json()
            count = len(data)
            all_data.extend(data)
            print(f"  ✅ 成功获取 {count} 条记录。总计: {len(all_data)}")
            # 如果返回的记录数少于 limit，说明已是最后一页
            if count < limit:
                print(f"--- 🎉 {dataset} 数据获取完成，总共 {len(all_data)} 条记录 ---")
                break
            offset += limit
        except RequestException as e:
            print(f"--- ❌ 数据请求发生错误：{e} ---")
            print("--- ⚠️ 尝试等待 30 秒后重试... ---")
            df = pd.DataFrame(all_data)
            df.to_csv(f"{dataset}.csv", sep="|", index=False, encoding="utf-8")
            print(f"--- 文件保存成功: {dataset}.csv ---")
            time.sleep(retry_delay_seconds)
        except Exception as e:
            print(f"--- ❌ 发生意外错误：{e} ---")
            df = pd.DataFrame(all_data)
            df.to_csv(f"{dataset}.csv", sep="|", index=False, encoding="utf-8")
            print(f"--- 文件保存成功: {dataset}.csv ---")
            raise e
        if c % SAVE_INTERVEL == 0:
            df = pd.DataFrame(all_data)
            df.to_csv(f"{dataset}.csv", sep="|", index=False, encoding="utf-8")
            print(f"--- 文件保存成功: {dataset}.csv ---")
    df = pd.DataFrame(all_data)
    df.to_csv(f"{dataset}.csv", sep="|", index=False, encoding="utf-8")
    print(f"--- 文件保存成功: {dataset}.csv ---")


if __name__ == "__main__":
    main()
