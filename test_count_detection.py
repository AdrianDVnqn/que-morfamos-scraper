"""
Script temporal para debuggear la detección de conteo de reviews.
"""
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scraping_utils import crear_driver
import time
import re

# URL correcta
TEST_URL = "https://www.google.com/maps/place/El+Boliche+de+Alberto+%22Parrilla%22/@-38.9516778,-68.0667883,15z/data=!4m7!3m6!1s0x960a33b511e297ad:0x2f94c22e2e22a989!8m2!3d-38.9598603!4d-68.0736252!15sChVFbCBCb2xpY2hlIGRlIEFsYmVydG8iA4gBAVoXIhVlbCBib2xpY2hlIGRlIGFsYmVydG-SAQpyZXN0YXVyYW504AEA!16s%2Fg%2F1q62cz34w?hl=es"

def check_count(driver):
    """Intenta extraer el conteo de varias formas"""
    
    # Método 1: div.F7nice
    try:
        f7nice = driver.find_element(By.CSS_SELECTOR, "div.F7nice")
        text = f7nice.text
        print(f"   div.F7nice: '{text}'")
        match = re.search(r'\(([\d\.]+)\)', text)
        if match:
            return int(match.group(1).replace('.', ''))
    except:
        print("   div.F7nice: No encontrado")
    
    # Método 2: Buscar span con paréntesis y número
    try:
        spans = driver.find_elements(By.TAG_NAME, "span")
        for span in spans:
            text = span.text
            if re.match(r'^\([\d\.]+\)$', text):  # Exactamente "(X.XXX)"
                print(f"   Span con count: '{text}'")
                num = text.strip('()').replace('.', '')
                return int(num)
    except:
        pass
    
    # Método 3: aria-label de botón de reseñas
    try:
        botones = driver.find_elements(By.CSS_SELECTOR, "button[aria-label]")
        for btn in botones:
            lbl = btn.get_attribute("aria-label") or ""
            if re.search(r'\d+.*reseña', lbl.lower()) or re.search(r'\d+.*review', lbl.lower()):
                print(f"   Botón con count: '{lbl}'")
                nums = re.findall(r'[\d\.]+', lbl)
                if nums:
                    return int(nums[0].replace('.', ''))
    except:
        pass
    
    return 0

def main():
    print("🧪 Debugging detección de conteo...")
    
    driver = crear_driver(headless=True)
    
    try:
        for intento in range(3):
            print(f"\n📍 Intento {intento + 1}/3...")
            driver.get(TEST_URL)
            
            # Esperar carga
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            except:
                pass
            
            time.sleep(4)  # Esperar más
            
            # Verificar H1
            try:
                h1 = driver.find_element(By.TAG_NAME, "h1")
                print(f"   H1: {h1.text}")
            except:
                print("   H1: No encontrado")
                continue
            
            # Intentar obtener count
            count = check_count(driver)
            
            if count > 0:
                print(f"\n✅ Count detectado: {count}")
                break
            else:
                print("   Count: 0 - refrescando...")
                driver.refresh()
                time.sleep(2)
        
        # Screenshot final
        driver.save_screenshot("debug_screenshot.png")
        print("\n📸 Screenshot guardado")
        
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
