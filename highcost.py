from flask import Flask, jsonify, request
import requests
import pandas as pd
import time
import traceback

app = Flask(__name__)

# ------------------- روابط Dataslayer -------------------
ADS_URL = "https://query-manager.dataslayer.ai/get_results/f7c65286119ec815fe2f66dc64d857ff4d399397b4b8f73c9e9ceb56454f60f9:1f539603fca748cf971a7500a61ebcb2?"
AVG_URL = "https://query-manager.dataslayer.ai/get_results/3497f00804eac919c7b9e6c652d5b6eecbcdbb2e60c1dcbaad287606fd81b46f:249c8499ee734d309911b43440e87660?"

# ------------------- كاش داخلي -------------------
CACHE = {}
CACHE_TTL = 600

def get_from_cache(key):
    if key in CACHE:
        data, timestamp = CACHE[key]
        if time.time() - timestamp < CACHE_TTL:
            return data
        else:
            del CACHE[key]
    return None

def save_to_cache(key, data):
    CACHE[key] = (data, time.time())

# ------------------- جلب البيانات -------------------
def fetch_data(url):
    cached = get_from_cache(url)
    if cached is not None:
        return cached

    try:
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()

        if "result" not in json_data:
            return pd.DataFrame()

        result = json_data["result"]
        if not result or len(result) < 2:
            return pd.DataFrame()

        df = pd.DataFrame(result[1:], columns=result[0])
        save_to_cache(url, df)
        return df

    except Exception as e:
        print("❌ Error fetching data:", e)
        return pd.DataFrame()

# ------------------- معالجة البيانات -------------------
def process_data():
    df_ads = fetch_data(ADS_URL)  # جدول الإعلانات
    df_avg = fetch_data(AVG_URL)  # جدول متوسط التكلفة

    if df_ads.empty or df_avg.empty:
        return pd.DataFrame()

    # إعادة تسمية الأعمدة
    rename_ads = {
        "Account id": "account_id",
        "Account name": "account_name",
        "Account Currency": "account_currency",
        "Ad id": "ad_id",
        "Ad name": "ad_name",
        "AdSet id": "adset_id",
        "AdSet name": "adset_name",
        "AdSet status": "adset_status",
        "AdSet start date": "adset_start_date",
        "Thumbnail url": "thumbnail_url",
        "Thumbnail Image": "thumbnail_image",
        "Link to promoted post": "link_to_promoted_post",
        "Cost per New Conversation started": "cost_per_new_conversation_started",
        "Ad status": "ad_status",
        "Campaign id": "campaign_id",
        "Campaign name": "campaign_name",
        "Campaign status": "campaign_status"
    }

    rename_avg = {
        "Account id": "account_id",
        "Account name": "account_name",
        "Account Currency": "account_currency",
        "Campaign id": "campaign_id",
        "Campaign name": "campaign_name",
        "Campaign status": "campaign_status",
        "Cost per New Conversation started": "cost_per_new_conversation_started"
    }

    df_ads.rename(columns=rename_ads, inplace=True)
    df_avg.rename(columns=rename_avg, inplace=True)

    # تحويل الأعمدة للأرقام
    df_ads["cost_per_new_conversation_started"] = pd.to_numeric(
        df_ads["cost_per_new_conversation_started"], errors="coerce"
    )
    df_avg["cost_per_new_conversation_started"] = pd.to_numeric(
        df_avg["cost_per_new_conversation_started"], errors="coerce"
    )

    # حساب Threshold لكل إعلان
    def calc_threshold(row):
        avg_cost = row.get("avg_cost", 0)
        currency = str(row.get("account_currency", "")).upper()
        if currency in ["ILS", "SHEKEL", "₪"]:
            return avg_cost + (1.3 * 3.5)
        return avg_cost + 1.3

    # دمج بيانات الإعلان مع متوسط التكلفة حسب الـ Campaign
    merged = pd.merge(
        df_ads,
        df_avg[["campaign_id", "cost_per_new_conversation_started", "account_currency"]],
        on="campaign_id",
        how="left",
        suffixes=("", "_avg")
    )

    merged = merged.rename(columns={"cost_per_new_conversation_started_avg": "avg_cost"})
    merged["avg_cost"] = merged["avg_cost"].fillna(0)
    merged["threshold"] = merged.apply(calc_threshold, axis=1)
    
    merged["ad_link"] = merged.apply(
        lambda row: f"https://adsmanager.facebook.com/adsmanager/manage/ads?act={row['account_id']}&filter_set=SEARCH_BY_AD_ID-STRING%1EEQUAL%1E%22{row['ad_id']}%22&selected_ad_ids={row['ad_id']}&sort=delivery_info~1",
        axis=1
    )

    return merged

# ------------------- API -------------------
@app.route("/analyze", methods=["GET"])
def analyze():
    try:
        df = process_data()
        if df.empty:
            return jsonify({"data": [], "message": "No data found"})

        # فقط الإعلانات عالية التكلفة مع التكرار
        high_cost = df[df["cost_per_new_conversation_started"] > df["threshold"]]

        return jsonify({
            "data": high_cost.to_dict(orient="records"),
            "message": f"تم العثور على {len(high_cost)} إعلان أعلى من المتوسط (High Cost)"
        })
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()})

# ------------------- تشغيل التطبيق -------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000)
