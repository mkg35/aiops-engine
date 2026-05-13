from fastapi import FastAPI
# Asenkron çalışmak için InfluxDB'nin Async kütüphanesini içeri alıyoruz
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from fastapi import FastAPI, HTTPException
import pandas as pd
from prophet import Prophet
import numpy as np

app = FastAPI(title="AIOps Engine", version="1.0")

# --- INFLUXDB BAĞLANTI AYARLARI ---
# Bu kısımları kendi InfluxDB arayüzündeki (localhost:8086) bilgilerle değiştirmelisin
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "rleuh-K5DUeRTWQcL-NvD5bXugOfJ5q3r_TE7MMreOESZKwQv-vQyzojMuP9_yg2JYJphnktoSzJDWdHFmynMQ=="
INFLUX_ORG = "aiops-org"
INFLUX_BUCKET = "microservices-metrics"

@app.get("/")
def read_root():
    return {"message": "AIOps Motoru Ayakta!", "status": "Çalışıyor"}

# InfluxDB'den metrik çekeceğimiz asıl endpoint
@app.get("/api/v1/metrics/student")
async def get_student_metrics():
    # Asenkron bir bağlantı bloğu (context manager) açıyoruz
    async with InfluxDBClientAsync(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        
        # InfluxDB'ye sorgu atacak nesneyi oluşturuyoruz
        query_api = client.query_api()
        
        # InfluxDB'nin kendi dili olan 'Flux' ile sorgumuzu yazıyoruz.
        # Anlamı: "Benim bucket'ımdan son 5 dakikanın verilerini getir"
        flux_query = f'''
            from(bucket: "{INFLUX_BUCKET}")
            |> range(start: -5m)
        '''
        
        # SİHİRLİ NOKTA BURASI: await kullanarak InfluxDB'den verinin gelmesini
        # sistemi kilitlemeden, arka planda asenkron olarak bekliyoruz!
        result = await query_api.query(flux_query)
        
        # Gelen karmaşık veriyi okunabilir bir JSON (Sözlük) formatına çeviriyoruz
        metric_list = []
        for table in result:
            for record in table.records:
                metric_list.append({
                    "time": record.get_time(),
                    "measurement": record.get_measurement(),
                    "field": record.get_field(),
                    "value": record.get_value()
                })
                
        # Dışarıya REST API cevabı (Response) olarak döndürüyoruz
        return {
            "status": "Başarılı", 
            "veri_sayisi": len(metric_list), 
            "data": metric_list
        }

@app.get("/api/v1/metrics/student/analyze")
async def analyze_student_cpu():
    async with InfluxDBClientAsync(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        query_api = client.query_api()
        
        # 1. SADECE CPU VERİSİNİ FİLTRELEME (Optimize edilmiş Flux sorgusu)
        # Spring Actuator varsayılan olarak CPU verisini "process.cpu.usage" adıyla gönderir.
        flux_query = f'''
            from(bucket: "{INFLUX_BUCKET}")
            |> range(start: -2m)
            |> filter(fn: (r) => r._measurement == "system_cpu_usage")
        '''
        
        result = await query_api.query(flux_query)
        
        # 2. VERİLERİ TOPLAMA VE MATEMATİKSEL ANALİZ
        cpu_degerleri = []
        for table in result:
            for record in table.records:
                cpu_degerleri.append(record.get_value())
                
        # Eğer InfluxDB'den hiç veri gelmediyse sistemi çökertmemek için kontrol
        if len(cpu_degerleri) == 0:
            return {"status": "Beklemede", "mesaj": "Son 2 dakikaya ait CPU verisi bulunamadı."}
            
        # Ortalama CPU kullanımını hesaplıyoruz
        ortalama_cpu = sum(cpu_degerleri) / len(cpu_degerleri)
        
        # Spring Actuator CPU'yu genelde 0.0 ile 1.0 arasında verir (%80 için 0.80)
        KIRITIK_ESIK = 0.40
        
        # 3. KURAL TABANLI KARAR MEKANİZMASI (React Aşamasına Hazırlık)
        alinacak_aksiyon = "NONE"
        if ortalama_cpu > KIRITIK_ESIK:
            alinacak_aksiyon = "SCALE_UP"
            durum_mesaji = "Kritik Yük Tespiti! Yeni instanceler ayağa kaldırılmalı."
        else:
            alinacak_aksiyon = "NONE"
            durum_mesaji = "Sistem normal seyrinde çalışıyor."
            
        # Sonucu orkestratöre bildireceğimiz formatta döndürüyoruz
        return {
            "service": "student-service",
            "avg_cpu": ortalama_cpu,
            "threshold": KIRITIK_ESIK,
            "action": alinacak_aksiyon,
            "message": durum_mesaji
        }

@app.get("/predict-load")
async def predict_system_load():
    """
    InfluxDB'den son 20 dakikalık CPU verisini çeker, Prophet modeli ile 
    önümüzdeki 3 dakikanın iş yükünü tahmin eder, proaktif karar üretir 
    ve tez başarı kriteri için MAPE (Hata Oranı) hesaplar.
    """
    async with InfluxDBClientAsync(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        query_api = client.query_api()
        
        query = f"""
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -20m)
          |> filter(fn: (r) => r["_measurement"] == "system_cpu_usage")
          |> filter(fn: (r) => r["_field"] == "value")
        """
        
        try:
            result = await query_api.query(org=INFLUX_ORG, query=query)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"InfluxDB'den veri çekilemedi: {str(e)}")

        records = []
        for table in result:
            for record in table.records:
                records.append({
                    "ds": record.get_time(),
                    "y": record.get_value()
                })
                
        if len(records) < 20:
            return {
                "status": "Yetersiz Veri", 
                "message": "MAPE hesaplaması ve tahmin için en az 20 veri noktası gerekiyor."
            }

        df = pd.DataFrame(records)
        df['ds'] = pd.to_datetime(df['ds']).dt.tz_localize(None)

        # --- 1. MAPE HESAPLAMASI İÇİN VERİYİ BÖLME (Train-Test Split) ---
        # Verinin ilk %80'ini eğitim, son %20'sini test (doğrulama) için ayırıyoruz
        split_idx = int(len(df) * 0.8)
        train_df = df.iloc[:split_idx]
        test_df = df.iloc[split_idx:]

        # Modeli sadece Train verisiyle eğitiyoruz
        eval_model = Prophet(changepoint_prior_scale=0.05)
        eval_model.fit(train_df)

        # Test verisinin zaman damgaları için tahmin yapıyoruz
        eval_future = pd.DataFrame({"ds": test_df["ds"]})
        eval_forecast = eval_model.predict(eval_future)

        # Gerçek değerler ile tahmin edilen değerleri (yhat) karşılaştırıp MAPE hesaplıyoruz
        # Gerçek değerlerin 0 olma ihtimaline karşı küçük bir epsilon (1e-5) ekliyoruz
        y_true = test_df["y"].values
        y_pred = eval_forecast["yhat"].values
        mape = np.mean(np.abs((y_true - y_pred) / (y_true + 1e-5))) * 100

        # --- 2. GERÇEK GELECEK TAHMİNİ (Tüm Veriyle) ---
        # Artık proaktif karar için modelimizi tüm veriyle (df) eğitiyoruz
        prod_model = Prophet(changepoint_prior_scale=0.05)
        prod_model.fit(df)

        # Önümüzdeki 180 saniye (3 dakika)
        future = prod_model.make_future_dataframe(periods=180, freq='S')
        forecast = prod_model.predict(future)

        future_forecast = forecast[forecast['ds'] > df['ds'].max()]
        max_predicted_load = float(future_forecast['yhat'].max())

        # --- 3. PROAKTİF KARAR ---
        PROAKTIF_ESIK = 0.50 
        decision = "🚨 PROAKTİF SCALE UP TETİKLENMELİ" if max_predicted_load > PROAKTIF_ESIK else "✅ Sistem Stabil"

        # MAPE sonucuna göre Başarı Durumu (Tez Formu Hedefi: < %15)
        mape_status = "PASSED" if mape < 15.0 else "NEEDS_OPTIMIZATION"

        return {
            "current_max_load": float(df['y'].max()),
            "predicted_max_load": round(max_predicted_load, 4),
            "decision": decision,
            "mape_score_percentage": round(mape, 2),
            "thesis_target_status": mape_status,
            "data_points_used": len(records)
        }