import urllib.request
import threading
import time

# Student servisinin adresi (Eğer özel bir endpoint varsa sonuna ekleyebilirsin, örn: 8090/api/student)
HEDEF_URL = "http://localhost:8090/" 

def spam_request():
    while True:
        try:
            urllib.request.urlopen(HEDEF_URL)
        except Exception:
            pass # Hata alırsak durma, istek atmaya devam et

print("🔥 Yük testi başlatılıyor... Student servisi (8090) bombardımana tutuluyor!")

# Aynı anda 50 farklı koldan (thread) durmaksızın istek atıyoruz
for i in range(50):
    threading.Thread(target=spam_request, daemon=True).start()

# Kodu açık tutmak için sonsuz döngü
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Test durduruldu.")