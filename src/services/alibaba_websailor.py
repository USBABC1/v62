#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Alibaba WebSailor Agent
Agente de navegação web inteligente com busca profunda e análise contextual
"""

import os
import logging
import time
import requests
import json
import random
from typing import Dict, List, Optional, Any
from urllib.parse import quote_plus, urljoin, urlparse
from bs4 import BeautifulSoup

from datetime import datetime
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from services.auto_save_manager import salvar_etapa, salvar_erro, salvar_trecho_pesquisa_web
from services.predictive_analytics_service import predictive_analytics_service
from services.visual_content_capture import visual_content_capture

# Import para integração com Exa
try:
    from services.exa_client import exa_client, extract_content_with_exa
    HAS_EXA = True
except ImportError:
    HAS_EXA = False
    logging.warning("⚠️ Exa client não encontrado. A extração de conteúdo via Exa será desativada.")

logger = logging.getLogger(__name__)

class AlibabaWebSailorAgent:
    """Agente WebSailor inteligente para navegação e análise web profunda"""

    def __init__(self):
        """Inicializa agente WebSailor"""
        self.enabled = True
        self.google_search_key = os.getenv("GOOGLE_SEARCH_KEY")
        self.jina_api_key = os.getenv("JINA_API_KEY")
        self.google_cse_id = os.getenv("GOOGLE_CSE_ID")
        self.serper_api_key = os.getenv("SERPER_API_KEY")

        # URLs das APIs
        self.google_search_url = "https://www.googleapis.com/customsearch/v1"
        self.jina_reader_url = "https://r.jina.ai/"
        self.serper_url = "https://google.serper.dev/search"

        # Headers inteligentes para navegação
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none"
        }

        # Domínios brasileiros preferenciais
        self.preferred_domains = {
            "g1.globo.com", "exame.com", "valor.globo.com", "estadao.com.br",
            "folha.uol.com.br", "canaltech.com.br", "tecmundo.com.br",
            "olhardigital.com.br", "infomoney.com.br", "startse.com",
            "revistapegn.globo.com", "epocanegocios.globo.com", "istoedinheiro.com.br",
            "convergenciadigital.com.br", "mobiletime.com.br", "teletime.com.br",
            "portaltelemedicina.com.br", "saudedigitalbrasil.com.br", "amb.org.br",
            "portal.cfm.org.br", "scielo.br", "ibge.gov.br", "fiocruz.br"
        }

        # Domínios bloqueados (irrelevantes)
        self.blocked_domains = {"airbnb.com"}

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # Estatísticas de navegação
        self.navigation_stats = {
            "total_searches": 0,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "blocked_urls": 0,
            "preferred_sources": 0,
            "total_content_chars": 0,
            "avg_quality_score": 0.0
        }

        logger.info("🌐 Alibaba WebSailor Agent inicializado - Navegação inteligente ativada")

    def navigate_and_research_deep(
        self,
        query: str,
        context: Dict[str, Any],
        max_pages: int = 25,
        depth_levels: int = 3,
        session_id: str = None
    ) -> Dict[str, Any]:
        """Navegação e pesquisa profunda com múltiplos níveis"""
        try:
            logger.info(f"🚀 INICIANDO NAVEGAÇÃO PROFUNDA para: {query}")
            start_time = time.time()

            # Salva início da navegação
            salvar_etapa("websailor_iniciado", {
                "query": query,
                "context": context,
                "max_pages": max_pages,
                "depth_levels": depth_levels
            }, categoria="pesquisa_web")

            all_content = []
            search_engines_used = []

            # NÍVEL 1: BUSCA MASSIVA MULTI-ENGINE
            logger.info("🔍 NÍVEL 1: Busca massiva com múltiplos engines")

            # Engines de busca em ordem de prioridade
            search_engines = [
                ("Google Custom Search", self._google_search_deep),
                ("Serper API", self._serper_search_deep),
            ]

            # Executa buscas em paralelo
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_engine = {
                    executor.submit(func, query, min(max_pages, 10)): name
                    for name, func in search_engines if func is not None
                }

                for future in as_completed(future_to_engine):
                    engine_name = future_to_engine[future]
                    try:
                        results = future.result()
                        if results:
                            search_engines_used.append(engine_name)
                            logger.info(f"✅ {engine_name}: {len(results)} resultados")

                            # Processa cada resultado
                            for result in results[:5]:  # Limita resultados por engine
                                url = result.get("url") or result.get("link")
                                if not url:
                                    continue

                                # Extrai conteúdo com múltiplas estratégias
                                content_data = self._extract_content_multi_strategy(
                                    url, result.get("title", ""), context, session_id
                                )

                                if content_data and content_data.get("success"):
                                    content_data["search_engine"] = engine_name
                                    content_data["content_length"] = len(content_data.get("content", ""))
                                    all_content.append(content_data)

                                    # === NOVO: Salva o trecho extraído ===
                                    salvar_trecho_pesquisa_web(
                                        url=url,
                                        titulo=content_data.get("title", ""),
                                        conteudo=content_data.get("content", ""),
                                        metodo_extracao=content_data.get("extraction_method", "desconhecido"),
                                        qualidade=content_data.get("quality_score", 0.0),
                                        session_id=session_id or "sessao_desconhecida"
                                    )
                                    # ======================================

                                    # Salva cada extração bem-sucedida (mantido para compatibilidade)
                                    salvar_etapa(f"websailor_extracao_{len(all_content)}", {
                                        "url": url,
                                        "engine": engine_name,
                                        "content_length": len(content_data.get("content", "")),
                                        "quality_score": content_data.get("quality_score", 0.0)
                                    }, categoria="pesquisa_web")

                                time.sleep(0.5)  # Rate limiting

                    except Exception as e:
                        logger.error(f"❌ Erro em {engine_name}: {str(e)}")
                        continue

            # NÍVEL 2: BUSCA EM PROFUNDIDADE (Links internos)
            if depth_levels > 1 and all_content:
                logger.info("🔍 NÍVEL 2: Busca em profundidade - Links internos")

            # NÍVEL 3: BUSCA CONTEXTUAL AVANÇADA (Queries relacionadas)
            if depth_levels > 2 and all_content:
                logger.info("🔍 NÍVEL 3: Busca contextual avançada - Queries relacionadas")
                related_queries = self._generate_related_queries(query, context, all_content)

                for related_query in related_queries[:3]:  # Limita queries relacionadas
                    try:
                        logger.info(f"🔍 Buscando por query relacionada: {related_query}")
                        related_content = self.navigate_and_research_deep(
                            related_query, context, max_pages=5, depth_levels=1, session_id=session_id
                        )
                        related_content["related_query"] = related_query
                        all_content.append(related_content)
                        time.sleep(0.4)
                    except Exception as e:
                        logger.warning(f"⚠️ Erro em query relacionada \'{related_query}\': {str(e)}")
                        continue

            # PROCESSAMENTO E ANÁLISE FINAL
            processed_research = self._process_and_analyze_content(all_content, query, context)

            # Atualiza estatísticas
            self._update_navigation_stats(all_content)

            end_time = time.time()

            # Salva resultado final da navegação
            salvar_etapa("websailor_resultado", processed_research, categoria="pesquisa_web")

            logger.info(f"✅ NAVEGAÇÃO PROFUNDA CONCLUÍDA em {end_time - start_time:.2f} segundos")
            logger.info(f"📊 {len(all_content)} páginas analisadas com {len(search_engines_used)} engines")

            return processed_research

        except Exception as e:
            logger.error(f"❌ ERRO CRÍTICO na navegação WebSailor: {str(e)}")
            salvar_erro("websailor_critico", e, contexto={"query": query})
            return self._generate_emergency_research(query, context)

    def _extract_content_multi_strategy(
        self,
        url: str,
        title: str,
        context: Dict[str, Any],
        session_id: str
    ) -> Optional[Dict[str, Any]]:
        """Estratégia multi-método para extração de conteúdo"""
        content_data = None
        methods_tried = []

        # 1. Tenta Jina Reader (método principal)
        if self.jina_api_key:
            try:
                logger.info(f"🔍 Tentando Jina Reader para {url}")
                headers = {**self.headers, "Authorization": f"Bearer {self.jina_api_key}"}
                response = self.session.get(f"{self.jina_reader_url}{url}", headers=headers, timeout=20)

                if response.status_code == 200:
                    content = response.text
                    if content and len(content) > 100: # Threshold mínimo
                        content_data = {
                            "success": True,
                            "url": url,
                            "title": title,
                            "content": content,
                            "quality_score": self._calculate_content_quality(content, url, context),
                            "extraction_method": "jina_reader"
                        }
                        logger.info(f"✅ Jina Reader: {len(content)} caracteres")
                else:
                    logger.warning(f"⚠️ Jina Reader retornou status {response.status_code} para {url}")
                    methods_tried.append(f"Jina ({response.status_code})")
            except Exception as e:
                logger.warning(f"⚠️ Jina Reader falhou para {url}: {e}")
                methods_tried.append(f"Jina (Erro)")

        # 2. Se Jina falhar, usa fallbacks (Exa -> BeautifulSoup)
        if not content_data or not content_data.get("success"):
            content_data = self._fallback_extraction(url, title, context, session_id)
            if content_data and content_data.get("success"):
                 methods_tried.append(content_data.get("extraction_method", "fallback"))
            else:
                 methods_tried.append("Todos_Fallbacks_Falharam")

        if content_data and content_data.get("success"):
            content_data["methods_tried"] = methods_tried
            is_preferred = any(domain in urlparse(url).netloc.lower() for domain in self.preferred_domains)
            content_data["is_preferred_source"] = is_preferred
            if is_preferred:
                self.navigation_stats["preferred_sources"] += 1
            
            # Integração com PredictiveAnalyticsService para score de qualidade
            if content_data.get("content"):
                try:
                    quality_score = predictive_analytics_service.get_content_quality_score(content_data["content"])
                    content_data["quality_score_predictive"] = quality_score
                    logger.info(f"Score de qualidade preditivo para {url}: {quality_score}")

                    # Captura screenshot se o score de qualidade for alto
                    if quality_score > 80: # Threshold definido
                        screenshot_path = f"./analyses_data/files/{session_id}/websailor_screenshot_{len(content_data["content"])}.png"
                        capture_result = visual_content_capture.capture_screenshot(url, screenshot_path)
                        if capture_result.get("success"):
                            content_data["screenshot_path"] = capture_result["filepath"]
                            logger.info(f"Screenshot capturado para {url}: {capture_result["filepath"]}")
                        else:
                            logger.warning(f"Falha ao capturar screenshot para {url}: {capture_result.get("error")}")

                except Exception as e:
                    logger.error(f"Erro ao obter score de qualidade preditivo ou capturar screenshot para {url}: {e}")

            return content_data
        else:
            self.navigation_stats["failed_extractions"] += 1
            logger.warning(f"⚠️ Falha em todos os métodos de extração para {url}. Métodos tentados: {methods_tried}")
            return None

    def _fallback_extraction(
        self,
        url: str,
        title: str,
        context: Dict[str, Any],
        session_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fallback para extração de conteúdo quando métodos principais falham.
        Ordem: Exa -> BeautifulSoup
        """
        logger.info(f"🔄 Usando fallback para extrair conteúdo de {url}")
        content = None
        extraction_method = "unknown"

        # --- Tenta Exa primeiro (se disponível) ---
        if HAS_EXA and exa_client.is_available():
            try:
                logger.info(f"🔍 Tentando Exa para {url}")
                content = extract_content_with_exa(url)
                if content and len(content.strip()) > 50:
                    logger.info(f"✅ Conteúdo extraído com Exa ({len(content)} caracteres)")
                    extraction_method = "exa"
                    return {
                        "success": True,
                        "url": url,
                        "title": title,
                        "content": content,
                        "quality_score": self._calculate_content_quality(content, url, context),
                        "extraction_method": extraction_method
                    }
                else:
                    logger.info(f"ℹ️ Exa não retornou conteúdo suficiente ou falhou.")
            except Exception as e:
                logger.warning(f"⚠️ Exa falhou como fallback para {url}: {e}")

        # --- Tenta BeautifulSoup como último recurso ---
        try:
            logger.info(f"🔍 Tentando BeautifulSoup para {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            # Remove elementos indesejados (scripts, styles, etc.)
            for script in soup(["script", "style", "nav", "footer", "aside"]):
                script.decompose()

            # Tenta encontrar o conteúdo principal (heurística simples)
            main_content = soup.find("main") or soup.find("article") or soup.find("div", class_=re.compile(r"content|main|post"))
            if main_content:
                text_content = main_content.get_text(separator=" ", strip=True)
            else:
                # Fallback para todo o body
                body = soup.find("body")
                text_content = body.get_text(separator=" ", strip=True) if body else ""

            # Limita o tamanho se necessário e remove excesso de espaços
            content = " ".join(text_content.split())
            if len(content) > 50:
                logger.info(f"✅ Conteúdo extraído com BeautifulSoup ({len(content)} caracteres)")
                extraction_method = "beautifulsoup"
                return {
                    "success": True,
                    "url": url,
                    "title": title,
                    "content": content,
                    "quality_score": self._calculate_content_quality(content, url, context),
                    "extraction_method": extraction_method
                }
            else:
                logger.warning(f"⚠️ BeautifulSoup não retornou conteúdo suficiente para {url}")
                return None

        except Exception as e:
            logger.warning(f"⚠️ BeautifulSoup falhou como fallback para {url}: {e}")
            return None

    def _calculate_content_quality(self, content: str, url: str, context: Dict[str, Any]) -> float:
        """Calcula um score de qualidade para o conteúdo extraído."""
        # Este é um cálculo heurístico e pode ser aprimorado
        score = 0.0

        # Comprimento do conteúdo
        length = len(content)
        if length > 1000:
            score += 40
        elif length > 500:
            score += 20
        elif length > 100:
            score += 10

        # Relevância de palavras-chave (simplificado)
        query_keywords = context.get("keywords", [])
        if query_keywords:
            for keyword in query_keywords:
                if keyword.lower() in content.lower():
                    score += 5

        # Presença em domínios preferenciais
        if any(domain in urlparse(url).netloc.lower() for domain in self.preferred_domains):
            score += 20

        # Evitar domínios bloqueados
        if any(domain in urlparse(url).netloc.lower() for domain in self.blocked_domains):
            score -= 30 # Penalidade

        # Normaliza para 0-100
        return min(100.0, max(0.0, score))

    def _process_and_analyze_content(self, all_content: List[Dict[str, Any]], query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Processa e analisa o conteúdo coletado."""
        # Implementação placeholder. Em um sistema real, isso envolveria NLP, sumarização, etc.
        total_chars = sum(len(item.get("content", "")) for item in all_content)
        avg_quality = sum(item.get("quality_score", 0) for item in all_content) / len(all_content) if all_content else 0

        return {
            "query": query,
            "total_documents": len(all_content),
            "total_characters": total_chars,
            "average_quality_score": avg_quality,
            "extracted_content_summary": [{"url": c["url"], "title": c["title"], "length": len(c.get("content", "")), "quality": c.get("quality_score", 0)} for c in all_content],
            "analysis_timestamp": datetime.now().isoformat()
        }

    def _generate_related_queries(self, query: str, context: Dict[str, Any], all_content: List[Dict[str, Any]]) -> List[str]:
        """Gera queries relacionadas com base no conteúdo e contexto."""
        # Placeholder para geração de queries mais inteligentes
        return [f"{query} tendências", f"{query} mercado", f"{query} inovações"]

    def _generate_emergency_research(self, query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Gera um resultado de pesquisa de emergência em caso de falha crítica."""
        logger.warning(f"🚨 Gerando pesquisa de emergência para {query}")
        return {
            "query": query,
            "total_documents": 0,
            "total_characters": 0,
            "average_quality_score": 0,
            "extracted_content_summary": [],
            "analysis_timestamp": datetime.now().isoformat(),
            "status": "emergency_fallback",
            "message": "Pesquisa de emergência: falha crítica na navegação principal."
        }

    def _update_navigation_stats(self, all_content: List[Dict[str, Any]]):
        """Atualiza as estatísticas de navegação."""
        self.navigation_stats["total_searches"] += 1
        self.navigation_stats["successful_extractions"] += len([c for c in all_content if c.get("success")])
        self.navigation_stats["total_content_chars"] += sum(len(c.get("content", "")) for c in all_content)
        
        total_quality_score = sum(c.get("quality_score", 0) for c in all_content)
        if self.navigation_stats["successful_extractions"] > 0:
            self.navigation_stats["avg_quality_score"] = total_quality_score / self.navigation_stats["successful_extractions"]

    # Métodos de busca específicos (Google Custom Search, Serper)
    def _google_search_deep(self, query: str, num_results: int = 10) -> List[Dict[str, Any]]:
        """Realiza busca profunda usando Google Custom Search API."""
        if not self.google_search_key or not self.google_cse_id:
            logger.warning("⚠️ Google Custom Search API não configurada.")
            return []
        
        try:
            params = {
                "key": self.google_search_key,
                "cx": self.google_cse_id,
                "q": query,
                "num": num_results
            }
            response = requests.get(self.google_search_url, params=params, headers=self.headers, timeout=10)
            response.raise_for_status()
            search_results = response.json()
            return search_results.get("items", [])
        except Exception as e:
            logger.error(f"❌ Erro na busca Google Custom Search: {e}")
            return []

    def _serper_search_deep(self, query: str, num_results: int = 10) -> List[Dict[str, Any]]:
        """Realiza busca profunda usando Serper API."""
        if not self.serper_api_key:
            logger.warning("⚠️ Serper API não configurada.")
            return []
        
        try:
            headers = {
                "X-API-KEY": self.serper_api_key,
                "Content-Type": "application/json"
            }
            payload = json.dumps({"q": query, "num": num_results})
            response = requests.post(self.serper_url, headers=headers, data=payload, timeout=10)
            response.raise_for_status()
            search_results = response.json()
            return search_results.get("organic", [])
        except Exception as e:
            logger.error(f"❌ Erro na busca Serper API: {e}")
            return []

# Instância global
alibaba_websailor_agent = AlibabaWebSailorAgent()


