from flask import Flask, jsonify
import requests
import pandas as pd
import traceback
import time

app = Flask(__name__)

# --- روابط البيانات (تبقى كما هي) ---
ADS_URL = "https://query-manager.dataslayer.ai/get_results/ff365bcc2a636a19aa3631abdb7cb3dcffbdd904db7f3d68fac1a5f070f56b61:2f740ef0cb34496d9c5621142f03be8b?accounts%3D%5B%7B%22id%22%3A%20%22act_347492718%22%2C%20%22name%22%3A%20%22SHOWMAN%22%2C%20%22connection_id%22%3A%20%22samuelsendsb11337%40hotmail.com%22%7D%2C%20%7B%22id%22%3A%20%22act_240939565%22%2C%20%22name%22%3A%20%22Your%20event%22%2C%20%22connection_id%22%3A%20%22samuelsendsb11337%40hotmail.com%22%7D%2C%20%7B%22id%22%3A%20%22act_175142654%22%2C%20%22name%22%3A%20%22Flamenco%22%2C%20%22connection_id%22%3A%20%22samuelsendsb11337%40hotmail.com%22%7D%2C%20%7B%22id%22%3A%20%22act_285101402%22%2C%20%22name%22%3A%20%223alm%20altafil%22%2C%20%22connection_id%22%3A%20%22samuelsendsb11337%40hotmail.com%22%7D%5D%26dates%3D%7B%22date_range_type%22%3A%20%22last5daysinc%22%7D%26compare_dates%3D%7B%7D%26metrics%3D%5B%22cost_per_new_conversation_started%22%5D%26dimensions%3D%7B%22rows%22%3A%20%5B%22account_name%22%2C%20%22account_id%22%2C%20%22campaign_name%22%2C%20%22campaign_id%22%2C%20%22effective_status%22%2C%20%22adset_name%22%2C%20%22adset_id%22%2C%20%22adset_effective_status%22%2C%20%22ad_name%22%2C%20%22ad_id%22%2C%20%22ad_effective_status%22%2C%20%22thumbnail_url%22%2C%20%22thumbnail_image%22%2C%20%22adset_start_date%22%2C%20%22start_date_campaign%22%2C%20%22account_currency%22%5D%2C%20%22limit_rows%22%3A%200%2C%20%22cols%22%3A%20%5B%5D%2C%20%22limit_cols%22%3A%200%7D%26filters%3D%5B%7B%22dimension%22%3A%20%22effective_status%22%2C%20%22value%22%3A%20%22ACTIVE%22%2C%20%22operator%22%3A%20%22equal_to%22%2C%20%22logic_gate%22%3A%20%22%22%2C%20%22is_dimension%22%3A%20true%7D%2C%20%7B%22dimension%22%3A%20%22ad_effective_status%22%2C%20%22value%22%3A%20%22ACTIVE%22%2C%20%22operator%22%3A%20%22equal_to%22%2C%20%22logic_gate%22%3A%20%22AND%22%2C%20%22is_dimension%22%3A%20true%7D%2C%20%7B%22dimension%22%3A%20%22adset_start_date%22%2C%20%22value%22%3A%20%22today%22%2C%20%22operator%22%3A%20%22not_equal_to%22%2C%20%22logic_gate%22%3A%20%22AND%22%2C%20%22is_dimension%22%3A%20true%7D%5D%26sort_by%3D%5B%5D%26sort_dims_by%3D%5B%5D%26options%3D%7B%7D%26connections%3D%5B%7B%22id%22%3A%20%22samuelsendsb11337%40hotmail.com%22%7D%5D%26timezone%3DUTC&output_type=json"
AVG_URL = "https://query-manager.dataslayer.ai/get_results/6c78354685ec10b9eb744ab25fa059c8ac12403d77123f8ca40f24bd9b3c28a2:96f39296a4734f24ab37c437e29fa699?accounts%3D%5B%7B%22id%22%3A%20%22act_347492718%22%2C%20%22name%22%3A%20%22SHOWMAN%22%2C%20%22connection_id%22%3A%20%22samuelsendsb11337%40hotmail.com%22%7D%2C%20%7B%22id%22%3A%20%22act_240939565%22%2C%20%22name%22%3A%20%22Your%20event%22%2C%20%22connection_id%22%3A%20%22samuelsendsb11337%40hotmail.com%22%7D%2C%20%7B%22id%22%3A%20%22act_175142654%22%2C%20%22name%22%3A%20%22Flamenco%22%2C%20%22connection_id%22%3A%20%22samuelsendsb11337%40hotmail.com%22%7D%2C%20%7B%22id%22%3A%20%22act_285101402%22%2C%20%22name%22%3A%20%223alm%20altafil%22%2C%20%22connection_id%22%3A%20%22samuelsendsb11337%40hotmail.com%22%7D%5D%26dates%3D%7B%22date_range_type%22%3A%20%22last5daysinc%22%7D%26compare_dates%3D%7B%7D%26metrics%3D%5B%22cost_per_new_conversation_started%22%5D%26dimensions%3D%7B%22rows%22%3A%20%5B%22account_name%22%2C%20%22account_id%22%2C%20%22campaign_name%22%2C%20%22campaign_id%22%2C%20%22effective_status%22%2C%20%22account_currency%22%5D%2C%20%22limit_rows%22%3A%200%2C%20%22cols%22%3A%20%5B%5D%2C%20%22limit_cols%22%3A%200%7D%26filters%3D%5B%7B%22dimension%22%3A%20%22effective_status%22%2C%20%22value%22%3A%20%22ACTIVE%22%2C%20%22operator%22%3A%20%22equal_to%22%2C%20%22logic_gate%22%3A%20%22%22%2C%20%22is_dimension%22%3A%20true%7D%5D%26sort_by%3D%5B%5D%26sort_dims_by%3D%5B%5D%26options%3D%7B%7D%26connections%3D%5B%7B%22id%22%3A%20%22samuelsendsb11337%40hotmail.com%22%7D%5D%26timezone%3DUTC&output_type=json"

# --- نقطة نهاية جديدة للإعلانات منخفضة التكلفة ---
@app.route('/low_cost_ads', methods=['GET'] )
def get_low_cost_ads():
    try:
        # 1. جلب البيانات
        cache_buster = int(time.time())
        ads_data = requests.get(f"{ADS_URL}&cache_buster={cache_buster}").json()
        avg_data = requests.get(f"{AVG_URL}&cache_buster={cache_buster}").json()

        ads_rows = ads_data.get("result", [])
        avg_rows = avg_data.get("result", [])
        if not ads_rows or not avg_rows:
            return jsonify({"error": "No data found in JSON"}), 500

        # 2. إنشاء ومعالجة DataFrames
        ads_df = pd.DataFrame(ads_rows[1:], columns=ads_rows[0])
        avg_df = pd.DataFrame(avg_rows[1:], columns=avg_rows[0])

        ads_df.rename(columns={"Account id": "account_id", "Campaign id": "campaign_id", "Cost per New Conversation started": "cost_per_new_conversation_started", "Account name": "account_name", "Campaign name": "campaign_name", "Ad name": "ad_name"}, inplace=True)
        avg_df.rename(columns={"Account id": "account_id", "Campaign id": "campaign_id", "Cost per New Conversation started": "cost_per_new_conversation_started"}, inplace=True)

        merged_df = pd.merge(ads_df, avg_df[["account_id", "campaign_id", "cost_per_new_conversation_started"]], on=["account_id", "campaign_id"], how="left", suffixes=("", "_avg"))

        merged_df['cost_per_new_conversation_started'] = pd.to_numeric(merged_df['cost_per_new_conversation_started'], errors='coerce').fillna(0)
        merged_df['cost_per_new_conversation_started_avg'] = pd.to_numeric(merged_df['cost_per_new_conversation_started_avg'], errors='coerce').fillna(0)

        # 3. الفلترة للمساواة أو الأقل من المتوسط
        filtered_df = merged_df[merged_df["cost_per_new_conversation_started"] <= merged_df["cost_per_new_conversation_started_avg"]].copy()

        # 4. تجهيز النتيجة النهائية
        result_df = filtered_df[["account_name", "campaign_name", "ad_name", "cost_per_new_conversation_started", "cost_per_new_conversation_started_avg"]]
        result_df = result_df.round(2)

        return jsonify(result_df.to_dict(orient="records"))

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        return jsonify({"error": "An internal server error occurred.", "details": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)
