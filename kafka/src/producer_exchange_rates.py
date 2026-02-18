import json
import asyncio
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from kafka import KafkaProducer

# Khởi tạo Producer (Dùng try-catch để tránh crash nếu Kafka chưa bật)
producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Tự động chuyển dict thành JSON
        acks='all', # Đảm bảo Kafka đã nhận được tin
        retries=3
    )
    print("Connected to Kafka successfully.")
except Exception as e:
    print(f"Warning: Cannot connect to Kafka. Error: {e}")

def get_exchange_rates_api():
    # URL của API cung cấp tỷ giá ngoại tệ của Vietcombank
    url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"
    try:
        # 1. Lấy dữ liệu
        response = requests.get(url)
        root = ET.fromstring(response.content)

        # 2. Tìm thẻ Exrate có CurrencyCode là 'USD'
        usd_item = root.find(".//Exrate[@CurrencyCode='USD']")

        if usd_item is not None:
            # Lấy giá mua chuyển khoản
            buy = usd_item.get('Transfer').replace(',', '')
            return "OK", int(float(buy))
            
        else:
            return "ERROR", "Cannot find USD exchange rate"

    except Exception as e:
        return "ERROR", "Error parsing XML: " + str(e)
    
async def get_exchange_rates_crawler():
    url = "https://www.vietcombank.com.vn/vi-VN/KHCN/Cong-cu-Tien-ich/Ty-gia"
    print("Scraping Vietcombank webpage to get exchange rates...")
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_selector("table.table-responsive tbody tr")
        html = await page.content()
        
        await browser.close()

    soup = BeautifulSoup(html, "html.parser")
    
    rows = soup.select("table.table-responsive tbody tr")
    for row in rows:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) >= 5 and cols[0].upper() == "USD":
            rate = int(float(cols[4].replace(",", "").strip()))
            
            return "OK", rate
        
    return "ERROR", "Cannot find USD exchange rate"

def send_to_kafka(currency, rate):
    if producer:
        try:
            message = {
                "currency": currency,
                "rate": rate,
                "timestamp": datetime.now().isoformat() # Định dạng thời gian chuẩn ISO
            }
            # Gửi tin nhắn bất đồng bộ
            producer.send(KAFKA_TOPIC, 
                          value=message,
                          key=currency.encode('utf-8'))
            producer.flush() # Đẩy tin đi ngay lập tức
            print(f"📨 [Kafka] Message sent: {message}")
        except Exception as e:
            print(f"❌ [Kafka] Failed to send: {e}")
    else:
        print("⚠️ [Kafka] Producer is not connected.")

async def coordinator():
    print("Starting exchange rate fetcher...")
    
    while True:
        timestamp = datetime.now().strftime("%H:%M:%S %d/%m/%Y")
        print(f"\n[{timestamp}] Start session...")

        # BƯỚC 1: Gọi API
        status, data = get_exchange_rates_api()
        
        final_rate = None
        
        if status == "OK":
            print(f"✅ [API] Get exchange rate successfully: 1 USD = {data:,.0f} VND")
            final_rate = data
        else:
            # BƯỚC 2: Nếu API lỗi -> Gọi Crawler
            print(f"⚠️ [API] Failed ({data}). Fallback to crawler...")
            status_crawl, data_crawl = await get_exchange_rates_crawler()
            
            if status_crawl == "OK":
                print(f"✅ [Crawler] Get exchange rate successfully: 1 USD = {data_crawl:,.0f} VND")
                final_rate = data_crawl
            else:
                print(f"❌ [ALL FAIL] Both API and Crawler failed. Error: {data_crawl}")

        # BƯỚC 3: Gửi Kafka nếu lấy được dữ liệu
        if final_rate is not None:
            send_to_kafka("USD", final_rate)
        
        # BƯỚC 3: Ngủ 5 phút (300 giây)
        print("Sleeping for 5 minutes...")
        await asyncio.sleep(300)
        
if __name__ == "__main__":
    try:
        asyncio.run(coordinator())
    except KeyboardInterrupt:
        if producer:
            producer.close()
        print("\nProgram terminated by user.")