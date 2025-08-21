#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Firecrwal Social Media Client
Cliente robusto para busca massiva em redes sociais usando Firecrwal
"""

import os
import logging
import requests
import time
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class FirecrwalSocialClient:
    """Cliente Firecrwal para busca massiva em redes sociais"""

    def __init__(self):
        """Inicializa o cliente Firecrwal"""
        self.api_key = os.getenv("FIRECRWAL_API_KEY")
        self.base_url = os.getenv("FIRECRWAL_API_URL", "https://api.firecrawl.com/v1")
        self.enabled = bool(self.api_key)
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

        if self.enabled:
            logger.info("🔥 Firecrwal Social Client ATIVO")
        else:
            logger.warning("⚠️ FIRECRWAL_API_KEY não configurado")

    def get_provider_status(self) -> Dict[str, Any]:
        """Retorna status do provedor Firecrwal"""
        return {
            "name": "firecrwal",
            "enabled": self.enabled,
            "status": "active" if self.enabled else "disabled",
            "api_configured": bool(self.api_key),
            "base_url": self.base_url
        }

    def search_social_media(self, query: str, platforms: List[str] = None) -> Dict[str, Any]:
        """Executa busca em redes sociais para plataformas específicas"""
        if not self.enabled:
            logger.warning("Firecrwal Social Client não está habilitado. Não é possível realizar a busca.")
            return {"success": False, "error": "Firecrwal Social Client não habilitado"}

        try:
            platforms = platforms or ["instagram", "facebook"]
            results = {}
            for platform in platforms:
                platform_query = f"{query} site:{self._get_platform_domain(platform)}"
                platform_results = self._execute_firecrawl_search(platform_query, platform)
                results[platform] = platform_results
            
            return {
                "total_platforms": len(platforms),
                "platforms_searched": platforms,
                "results": results,
                "timestamp": datetime.now().isoformat(),
                "source": "firecrawl_real"
            }
        except Exception as e:
            logger.error(f"❌ Erro na busca social Firecrawl: {e}")
            raise

    def _execute_firecrawl_search(self, query: str, context: str) -> Dict[str, Any]:
        """Executa busca via API Firecrawl"""
        if not self.api_key:
            raise ValueError("FIRECRWAL_API_KEY não configurada.")
        
        try:
            payload = {
                "query": query,
                "page_options": {"onlyMainContent": True},
                "crawl_options": {"depth": 1},
                "return_only_urls": False
            }
            
            response = requests.post(
                f"{self.base_url}/search",
                headers=self.headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"❌ Firecrawl API erro {response.status_code}: {response.text}")
                response.raise_for_status() # Levanta exceção para erros HTTP
        except Exception as e:
            logger.error(f"❌ Erro na execução Firecrawl: {e}")
            raise

    def _get_platform_domain(self, platform: str) -> str:
        """Retorna domínio da plataforma"""
        domains = {
            "youtube": "youtube.com",
            "twitter": "twitter.com",
            "linkedin": "linkedin.com",
            "instagram": "instagram.com",
            "facebook": "facebook.com",
            "tiktok": "tiktok.com"
        }
        return domains.get(platform, platform)

    def is_available(self) -> bool:
        """Verifica se o cliente está disponível"""
        return self.enabled

# Instância global
firecrawl_social_client = FirecrwalSocialClient()


