#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Predictive Analytics Service
Serviço para orquestrar o motor de análise preditiva
"""

import logging
import asyncio
import json
from typing import Dict, List, Any
from datetime import datetime
from pathlib import Path

from engine.predictive_analytics_engine import PredictiveAnalyticsEngine
from services.auto_save_manager import salvar_etapa, salvar_erro

logger = logging.getLogger(__name__)

class PredictiveAnalyticsService:
    """Serviço para orquestrar o motor de análise preditiva"""

    def __init__(self):
        """Inicializa o serviço"""
        self.engine = PredictiveAnalyticsEngine()
        logger.info("📊 Predictive Analytics Service inicializado")

    async def analyze_all_data(self, session_id: str) -> Dict[str, Any]:
        """Realiza a análise preditiva de todos os dados coletados para uma sessão."""
        logger.info(f"📊 Iniciando análise preditiva para sessão: {session_id}")
        
        try:
            # A chamada principal para o engine que orquestra todas as análises
            predictive_analysis_results = await self.engine.analyze_session_data(session_id)

            salvar_etapa("predictive_analysis_results", predictive_analysis_results, categoria="analise_preditiva", session_id=session_id)
            logger.info(f"✅ Análise preditiva concluída para sessão: {session_id}")
            return predictive_analysis_results

        except FileNotFoundError as fnfe:
            logger.error(f"❌ Erro na análise preditiva: {fnfe}")
            return {"success": False, "error": str(fnfe)}
        except Exception as e:
            logger.error(f"❌ Erro inesperado na análise preditiva: {e}", exc_info=True)
            salvar_erro("predictive_analysis", e, contexto={"session_id": session_id})
            return {"success": False, "error": str(e)}

    async def analyze_content_chunk(self, text_content: str) -> Dict[str, Any]:
        """Expõe o método de análise de chunk do engine para uso externo, se necessário."""
        return await self.engine.analyze_content_chunk(text_content)

    async def analyze_data_quality(self, massive_data: Dict[str, Any]) -> Dict[str, Any]:
        """Expõe o método de análise de qualidade de dados do engine para uso externo, se necessário."""
        return await self.engine.analyze_data_quality(massive_data)

    async def refine_search_queries(self, original_query: str, search_results: Dict[str, Any]) -> List[str]:
        """Expõe o método de refinamento de queries do engine para uso externo, se necessário."""
        return await self.engine.refine_search_queries(original_query, search_results)

# Instância global
predictive_analytics_service = PredictiveAnalyticsService()


