#!/usr/bin/env python3
"""
Captures fresh, high-resolution screenshots of the live Streamlit dashboard (port 8501)
with 100% modern, zero-emoji UI styling for the documentation.
"""

import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

DOCS_IMAGES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docs", "images"))
os.makedirs(DOCS_IMAGES_DIR, exist_ok=True)

def capture_all():
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--window-size=1600,1200')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--force-device-scale-factor=1')

    driver = webdriver.Chrome(options=chrome_options)

    try:
        print("Connecting to http://localhost:8501 ...")
        driver.get('http://localhost:8501')
        time.sleep(6)  # Wait for Streamlit components, WebSocket, PyDeck to mount

        # 1. Capture Header & 3D Map View
        map_path = os.path.join(DOCS_IMAGES_DIR, "3d_tactical_map_overview.png")
        driver.set_window_size(1600, 1100)
        time.sleep(1)
        driver.save_screenshot(map_path)
        print(f"Captured: {map_path}")

        # 2. Scroll down to Neighborhood Cards & Capture
        driver.execute_script("window.scrollTo(0, 750);")
        time.sleep(2)
        cards_path = os.path.join(DOCS_IMAGES_DIR, "adversarial_console_and_neighborhoods.png")
        driver.save_screenshot(cards_path)
        print(f"Captured: {cards_path}")

        # 3. Switch to Tab 2: Telemetry & Attack Divergence
        tabs = driver.find_elements(By.CSS_SELECTOR, "button[data-baseweb='tab']")
        if len(tabs) >= 2:
            print("Switching to Tab 2 (Telemetry & Attack Divergence)...")
            tabs[1].click()
            time.sleep(3)
            divergence_path = os.path.join(DOCS_IMAGES_DIR, "divergence_curves_view.png")
            driver.save_screenshot(divergence_path)
            print(f"Captured: {divergence_path}")

            # Switch back to Tab 1
            tabs[0].click()
            time.sleep(2)

        # 4. Capture Clean Sidebar Threat Matrix View
        sidebar = driver.find_element(By.CSS_SELECTOR, "section[data-testid='stSidebar']")
        if sidebar:
            sidebar_path = os.path.join(DOCS_IMAGES_DIR, "red_team_sidebar_controls.png")
            sidebar.screenshot(sidebar_path)
            print(f"Captured: {sidebar_path}")

        print("All live screenshots successfully captured.")

    finally:
        driver.quit()

if __name__ == '__main__':
    capture_all()
