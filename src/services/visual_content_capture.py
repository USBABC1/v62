#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Visual Content Capture
Captura de screenshots e conteúdo visual usando Selenium
"""

import os
import logging
import time
from typing import Dict, Any
from datetime import datetime
from pathlib import Path

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)

class VisualContentCapture:
    """Capturador de conteúdo visual usando Selenium"""

    def __init__(self):
        """Inicializa o capturador visual"""
        self.driver = None
        self.wait_timeout = 10
        self.page_load_timeout = 30
        
        logger.info("📸 Visual Content Capture inicializado")

    def _setup_driver(self) -> webdriver.Chrome:
        """Configura o driver do Chrome em modo headless"""
        try:
            chrome_options = Options()
            
            # Configurações para modo headless e otimização no Replit
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            chrome_options.add_argument("--remote-debugging-port=9222")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")  # Para economizar banda
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
            
            # Usa selenium_checker para configuração robusta
            from .selenium_checker import selenium_checker
            
            # Executa verificação completa
            check_results = selenium_checker.full_check()
            
            if not check_results["selenium_ready"]:
                raise Exception("Selenium não está configurado corretamente")
            
            # Configura o Chrome com o melhor caminho encontrado
            best_chrome_path = check_results["best_chrome_path"]
            if best_chrome_path:
                chrome_options.binary_location = best_chrome_path
                logger.info(f"✅ Chrome configurado: {best_chrome_path}")
            
            # Tenta usar ChromeDriverManager primeiro
            try:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.info("✅ ChromeDriverManager funcionou")
            except Exception as e:
                logger.warning(f"⚠️ ChromeDriverManager falhou: {e}, usando chromedriver do sistema")
                # Fallback para chromedriver do sistema
                driver = webdriver.Chrome(options=chrome_options)
            
            driver.set_page_load_timeout(self.page_load_timeout)
            
            logger.info("✅ Chrome driver configurado com sucesso")
            return driver
            
        except Exception as e:
            logger.error(f"❌ Erro ao configurar Chrome driver: {e}")
            raise

    def _create_session_directory(self, session_id: str) -> Path:
        """Cria diretório para a sessão"""
        try:
            session_dir = Path("analyses_data") / "files" / session_id
            session_dir.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"📁 Diretório criado: {session_dir}")
            return session_dir
            
        except Exception as e:
            logger.error(f"❌ Erro ao criar diretório: {e}")
            raise

    def capture_screenshot(self, url: str, output_path: str) -> Dict[str, Any]:
        """Captura screenshot de uma URL específica e salva no caminho indicado."""
        try:
            logger.info(f"📸 Capturando screenshot de: {url} para {output_path}")
            
            if not self.driver:
                self.driver = self._setup_driver()

            # Acessa a URL
            self.driver.get(url)
            
            # Aguarda o carregamento da página
            try:
                WebDriverWait(self.driver, self.wait_timeout).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                logger.warning(f"⚠️ Timeout aguardando carregamento de {url}")
            
            # Aguarda um pouco mais para renderização completa
            time.sleep(2)
            
            # Captura informações da página
            page_title = self.driver.title or "Sem título"
            page_url = self.driver.current_url
            
            # Tenta obter meta description
            meta_description = ""
            try:
                meta_element = self.driver.find_element(By.CSS_SELECTOR, 'meta[name="description"]')
                meta_description = meta_element.get_attribute("content") or ""
            except:
                pass
            
            # Define o caminho do arquivo
            screenshot_path = Path(output_path)
            screenshot_path.parent.mkdir(parents=True, exist_ok=True) # Garante que o diretório exista
            
            # Captura o screenshot
            self.driver.save_screenshot(str(screenshot_path))
            
            # Verifica se o arquivo foi criado
            if screenshot_path.exists() and screenshot_path.stat().st_size > 0:
                logger.info(f"✅ Screenshot salvo: {screenshot_path}")
                
                return {
                    "success": True,
                    "url": url,
                    "final_url": page_url,
                    "title": page_title,
                    "description": meta_description,
                    "filepath": str(screenshot_path),
                    "filesize": screenshot_path.stat().st_size,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                raise Exception("Screenshot não foi criado ou está vazio")
                
        except Exception as e:
            error_msg = f"Erro ao capturar screenshot de {url}: {e}"
            logger.error(f"❌ {error_msg}")
            
            return {
                "success": False,
                "url": url,
                "error": error_msg,
                "timestamp": datetime.now().isoformat()
            }
        finally:
            # É importante fechar o driver após cada uso ou gerenciar o ciclo de vida
            # de forma mais robusta para evitar vazamento de recursos.
            # Para este caso, vamos fechar após cada captura para garantir isolamento.
            if self.driver:
                try:
                    self.driver.quit()
                    logger.info("✅ Chrome driver fechado")
                except Exception as e:
                    logger.error(f"❌ Erro ao fechar driver: {e}")
                self.driver = None

    def cleanup_old_screenshots(self, days_old: int = 7):
        """Remove screenshots antigos para economizar espaço"""
        try:
            files_dir = Path("analyses_data") / "files"
            if not files_dir.exists():
                return
            
            cutoff_time = time.time() - (days_old * 24 * 60 * 60)
            removed_count = 0
            
            for session_dir in files_dir.iterdir():
                if session_dir.is_dir():
                    for screenshot in session_dir.glob("*.png"):
                        if screenshot.stat().st_mtime < cutoff_time:
                            screenshot.unlink()
                            removed_count += 1
                    
                    # Remove diretório se estiver vazio
                    try:
                        session_dir.rmdir()
                    except OSError:
                        pass  # Diretório não está vazio
            
            if removed_count > 0:
                logger.info(f"🧹 Removidos {removed_count} screenshots antigos")
                
        except Exception as e:
            logger.error(f"❌ Erro na limpeza: {e}")

# Instância global
visual_content_capture = VisualContentCapture()


