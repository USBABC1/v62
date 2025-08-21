#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Massive Data Collector
Coletor massivo de dados com integração robusta
"""

import os
import logging
import time
import json
import asyncio
from datetime import datetime
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Importa serviços existentes
from services.enhanced_search_coordinator import enhanced_search_coordinator
from services.social_media_extractor import social_media_extractor
from services.auto_save_manager import salvar_etapa, salvar_erro

# Importa novos serviços da Etapa 1
from services.search_api_manager import search_api_manager
from services.trendfinder_client import trendfinder_client
from services.supadata_mcp_client import supadata_client
from services.visual_content_capture import visual_content_capture
from services.predictive_analytics_service import predictive_analytics_service # Import adicionado

logger = logging.getLogger(__name__)

class MassiveDataCollector:
    """Coletor de dados massivo para criar JSON gigante"""

    def __init__(self):
        """Inicializa o coletor massivo"""
        self.collected_data = {}
        self.total_content_length = 0
        self.sources_count = 0

        logger.info("🚀 Massive Data Collector inicializado")

    def collect_comprehensive_data(
        self,
        produto: str,
        nicho: str,
        publico: str,
        session_id: str
    ) -> Dict[str, Any]:
        """Método de compatibilidade para coleta de dados"""
        try:
            # Constrói query a partir dos parâmetros
            query_parts = []
            if produto:
                query_parts.append(produto)
            if nicho:
                query_parts.append(nicho)
            if publico:
                query_parts.append(publico)
            
            query = " ".join(query_parts) if query_parts else "análise de mercado"
            
            # Contexto da análise
            context = {
                "produto": produto,
                "nicho": nicho,
                "publico": publico,
                "session_id": session_id
            }
            
            # Chama o método assíncrono de forma síncrona
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(
                    self.execute_massive_collection(query, context, session_id)
                )
            finally:
                loop.close()
                
        except Exception as e:
            logger.error(f"Erro na coleta de dados: {e}")
            return {"error": str(e), "success": False}

    async def execute_massive_collection(
        self,
        query: str,
        context: Dict[str, Any],
        session_id: str
    ) -> Dict[str, Any]:
        """Executa coleta massiva de dados com novos serviços"""

        logger.info(f"🚀 INICIANDO COLETA MASSIVA APRIMORADA - Sessão: {session_id}")
        start_time = time.time()

        # Estrutura de dados consolidados
        massive_data = {
            "session_id": session_id,
            "query": query,
            "context": context,
            "collection_started": datetime.now().isoformat(),
            "web_search_data": {},
            "social_media_data": {},
            "trends_data": {},
            "supadata_results": {},
            "visual_content": {},
            "extracted_content": [],
            "screenshots": [], # Adicionado para armazenar caminhos dos screenshots
            "statistics": {
                "total_sources": 0,
                "total_content_length": 0,
                "collection_time": 0,
                "sources_by_type": {},
                "screenshot_count": 0,
                "api_rotations": {}
            }
        }

        try:
            # FASE 1: Busca Web Intercalada com Rotação de APIs
            logger.info("🔍 FASE 1: Executando busca web intercalada...")
            web_results = await search_api_manager.interleaved_search(query)
            massive_data["web_search_data"] = web_results

            # FASE 2: Coleta de Tendências via TrendFinder MCP
            logger.info("📈 FASE 2: Coletando tendências via TrendFinder...")
            if trendfinder_client.is_available():
                trends_results = await trendfinder_client.search(query)
                massive_data["trends_data"] = trends_results
            else:
                logger.warning("⚠️ TrendFinder não disponível")
                massive_data["trends_data"] = {"success": False, "error": "TrendFinder não configurado"}

            # FASE 3: Dados Sociais via Supadata MCP
            logger.info("📊 FASE 3: Coletando dados sociais via Supadata...")
            if supadata_client.is_available():
                supadata_results = await supadata_client.search(query, "all")
                massive_data["supadata_results"] = supadata_results
            else:
                logger.warning("⚠️ Supadata não disponível")
                massive_data["supadata_results"] = {"success": False, "error": "Supadata não configurado"}

            # FASE 4: Extração de Redes Sociais (método existente como fallback)
            logger.info("📱 FASE 4: Extraindo dados de redes sociais (fallback)...")
            try:
                # social_media_extractor.search_all_platforms agora retorna um dicionário com 'platforms' dentro
                social_results_raw = await social_media_extractor.search_all_platforms(query)
                
                if social_results_raw.get("success"):
                    social_results = {
                        "success": True,
                        "all_platforms_data": social_results_raw,
                        "total_posts": social_results_raw.get("total_results", 0),
                        "platforms_analyzed": len(social_results_raw.get("platforms", {})),
                        "extracted_at": datetime.now().isoformat()
                    }
                else:
                    social_results = {
                        "success": False,
                        "error": "Falha na extração de redes sociais",
                        "all_platforms_data": {"platforms": {}},
                        "total_posts": 0
                    }
            except Exception as social_error:
                logger.error(f"❌ Erro na extração social: {social_error}")
                social_results = {
                    "success": False,
                    "error": str(social_error),
                    "all_platforms_data": {"platforms": {}},
                    "total_posts": 0
                }
                
            massive_data["social_media_data"] = social_results

            # FASE 5: Seleção de URLs Relevantes
            logger.info("🎯 FASE 5: Selecionando URLs mais relevantes...")
            selected_urls = visual_content_capture.select_top_urls(web_results, max_urls=8)

            # FASE 6: Captura de Screenshots
            logger.info("📸 FASE 6: Capturando screenshots das URLs selecionadas...")
            if selected_urls:
                try:
                    screenshot_results = await visual_content_capture.capture_screenshots(
                        selected_urls, session_id
                    )
                    massive_data["visual_content"] = screenshot_results
                    massive_data["statistics"]["screenshot_count"] = screenshot_results.get("successful_captures", 0)
                    massive_data["screenshots"] = screenshot_results.get("successful_captures_paths", []) # Salva os caminhos dos screenshots
                except Exception as capture_error:
                    logger.error(f"❌ Erro na captura de screenshots: {capture_error}")
                    massive_data["visual_content"] = {"success": False, "error": str(capture_error)}
                    massive_data["statistics"]["screenshot_count"] = 0
            else:
                logger.warning("⚠️ Nenhuma URL selecionada para screenshots")
                massive_data["visual_content"] = {"success": False, "error": "Nenhuma URL disponível"}

            # FASE 7: Consolidação e Processamento
            logger.info("🔗 FASE 7: Consolidando dados coletados...")

            all_extracted_content = []

            # Processa resultados web
            if web_results.get("all_results"):
                for provider_result in web_results["all_results"]:
                    if provider_result.get("success") and provider_result.get("results"):
                        all_extracted_content.extend(provider_result["results"])

            # Processa resultados sociais existentes
            if social_results.get("all_platforms_data"):
                platforms = social_results["all_platforms_data"].get("platforms", {})
                
                if isinstance(platforms, dict):
                    for platform, data in platforms.items():
                        if isinstance(data, dict) and "results" in data:
                            all_extracted_content.extend(data["results"])
                elif isinstance(platforms, list):
                    for platform_data in platforms:
                        if isinstance(platform_data, dict) and "results" in platform_data:
                            all_extracted_content.extend(platform_data["results"])
                        elif isinstance(platform_data, dict) and "platform" in platform_data:
                            platform_results = platform_data.get("data", {}).get("results", [])
                            all_extracted_content.extend(platform_results)

            # Processa tendências do TrendFinder
            if massive_data["trends_data"].get("success"):
                trends = massive_data["trends_data"].get("trends", [])
                all_extracted_content.extend([{"source": "TrendFinder", "content": trend} for trend in trends])

            # Processa dados do Supadata
            if massive_data["supadata_results"].get("success"):
                posts = massive_data["supadata_results"].get("posts", [])
                all_extracted_content.extend([{"source": "Supadata", "content": post} for post in posts])

            massive_data["extracted_content"] = all_extracted_content

            # Calcula estatísticas finais
            collection_time = time.time() - start_time
            total_sources = len(all_extracted_content)
            total_content = sum(len(str(item)) for item in all_extracted_content)

            # Atualiza estatísticas com informações dos novos serviços
            sources_by_type = {
                "web_search_intercalado": web_results.get("successful_searches", 0),
                "social_media_fallback": self._count_social_results(social_results),
                "trendfinder_mcp": len(massive_data["trends_data"].get("trends", [])),
                "supadata_mcp": massive_data["supadata_results"].get("total_results", 0),
                "screenshots": massive_data["statistics"]["screenshot_count"]
            }

            massive_data["statistics"].update({
                "total_sources": total_sources,
                "total_content_length": total_content,
                "collection_time": collection_time,
                "sources_by_type": sources_by_type,
                "api_rotations": search_api_manager.get_provider_stats()
            })

            # Salva dados coletados em um arquivo JSON para a sessão
            session_dir = Path(f"analyses_data/{session_id}")
            session_dir.mkdir(parents=True, exist_ok=True)
            massive_data_path = session_dir / "massive_data_collected.json"
            with open(massive_data_path, "w", encoding="utf-8") as f:
                json.dump(massive_data, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ Dados massivos coletados salvos em: {massive_data_path}")

            # Gera relatório de coleta com referências às imagens
            collection_report = await self._generate_collection_report(massive_data, session_id)

            # Salva dados coletados via auto_save_manager também
            salvar_etapa("massive_data_collected", massive_data, categoria="coleta_massiva", session_id=session_id)

            logger.info(f"✅ COLETA MASSIVA APRIMORADA CONCLUÍDA")
            logger.info(f"📊 {total_sources} fontes coletadas em {collection_time:.2f}s")
            logger.info(f"📝 {total_content:,} caracteres de conteúdo")
            logger.info(f"📸 {massive_data['statistics']['screenshot_count']} screenshots capturados")

            return massive_data

        except Exception as e:
            logger.error(f"❌ Erro durante a coleta massiva: {e}", exc_info=True)
            salvar_erro("massive_data_collection", e, contexto={"query": query, "session_id": session_id})
            return {"error": "Falha na coleta massiva de dados", "details": str(e)}

    def _count_social_results(self, social_results: Dict[str, Any]) -> int:
        """Conta resultados sociais de forma segura"""
        try:
            platforms = social_results.get("all_platforms_data", {}).get("platforms", {})
            total_count = 0
            
            if isinstance(platforms, dict):
                for data in platforms.values():
                    if isinstance(data, dict) and "results" in data:
                        total_count += len(data["results"])
            elif isinstance(platforms, list):
                for platform_data in platforms:
                    if isinstance(platform_data, dict):
                        if "results" in platform_data:
                            total_count += len(platform_data["results"])
                        elif "data" in platform_data and isinstance(platform_data["data"], dict):
                            results = platform_data["data"].get("results", [])
                            total_count += len(results)
            
            return total_count
        except Exception as e:
            logger.error(f"Erro ao contar resultados sociais: {e}")
            return 0

    async def _generate_collection_report(self, massive_data: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Gera um relatório de coleta de dados com referências a screenshots."""
        report = {
            "title": "Relatório Detalhado de Coleta de Dados",
            "session_id": session_id,
            "query": massive_data.get("query", "N/A"),
            "timestamp": datetime.now().isoformat(),
            "statistics": massive_data.get("statistics", {}),
            "visual_content_summary": {
                "screenshot_count": massive_data.get("statistics", {}).get("screenshot_count", 0),
                "screenshot_paths": massive_data.get("visual_content", {}).get("successful_captures_paths", [])
            },
            "data_sources_summary": {
                "web_search": massive_data.get("web_search_data", {}).get("statistics", {}).get("total_results", 0),
                "social_media": massive_data.get("social_media_data", {}).get("total_posts", 0),
                "trends": len(massive_data.get("trends_data", {}).get("trends", [])),
                "supadata": massive_data.get("supadata_results", {}).get("total_results", 0)
            },
            "notes": "Este relatório resume os dados coletados. Para detalhes completos, consulte os arquivos JSON na pasta da sessão."
        }

        # Salva o relatório em um arquivo JSON
        report_dir = Path(f"analyses_data/{session_id}")
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "relatorio_coleta_detalhado.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ Relatório de coleta detalhado salvo em: {report_path}")

        return report

# Instância global
massive_data_collector = MassiveDataCollector()
