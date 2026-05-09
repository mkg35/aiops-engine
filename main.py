from fastapi import FastAPI
# Asenkron çalışmak için InfluxDB'nin Async kütüphanesini içeri alıyoruz
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

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