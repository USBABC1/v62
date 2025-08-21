#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v2.0 - Social Media Extractor
Extrator robusto para redes sociais
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from services.tavily_mcp_client import tavily_mcp_client
from services.firecrawl_social_client import firecrawl_social_client
from services.predictive_analytics_service import predictive_analytics_service # Import adicionado

logger = logging.getLogger(__name__)

class SocialMediaExtractor:
    """Extrator para análise de redes sociais"""

    def __init__(self):
        """Inicializa o extrator de redes sociais"""
        self.enabled = True
        self.tavily_client = tavily_mcp_client
        self.firecrawl_client = firecrawl_social_client
        logger.info("✅ Social Media Extractor inicializado")

    async def extract_comprehensive_data(self, query: str, context: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Extrai dados abrangentes de redes sociais"""
        logger.info(f"🔍 Extraindo dados abrangentes para: {query}")
        
        try:
            all_platforms_data = await self.search_all_platforms(query)
            sentiment_analysis = await self.analyze_sentiment_trends(all_platforms_data) # Chamada assíncrona
            
            return {
                "success": True,
                "query": query,
                "session_id": session_id,
                "all_platforms_data": all_platforms_data,
                "sentiment_analysis": sentiment_analysis,
                "total_posts": all_platforms_data.get("total_results", 0),
                "platforms_analyzed": len(all_platforms_data.get("platforms", [])),
                "extracted_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Erro na extração abrangente: {e}")
            return {
                "success": False,
                "error": str(e),
                "query": query,
                "session_id": session_id
            }

    async def search_all_platforms(self, query: str) -> Dict[str, Any]:
        """Busca em todas as plataformas de redes sociais usando Tavily e Firecrawl"""

        logger.info(f"🔍 Iniciando busca em redes sociais para: {query}")

        results = {
            "query": query,
            "platforms": {},
            "total_results": 0,
            "search_quality": "real_data",
            "generated_at": datetime.now().isoformat()
        }

        # Tenta usar Tavily para YouTube e Twitter
        if self.tavily_client.is_available():
            youtube_data = await self.tavily_client.search_youtube_content(query)
            if youtube_data.get("total_results", 0) > 0:
                results["platforms"]["youtube"] = youtube_data
                results["total_results"] += youtube_data["total_results"]
            
            twitter_data = await self.tavily_client.search_social_media(query, platforms=["twitter"])
            if twitter_data.get("results", {}).get("twitter", {}).get("total_results", 0) > 0:
                results["platforms"]["twitter"] = twitter_data["results"]["twitter"]
                results["total_results"] += twitter_data["results"]["twitter"]["total_results"]

        # Tenta usar Firecrawl para Instagram e Facebook
        if self.firecrawl_client.is_available():
            instagram_data = await self.firecrawl_client.search_social_media(query, platforms=["instagram"])
            if instagram_data.get("results", {}).get("instagram", {}).get("total_results", 0) > 0:
                results["platforms"]["instagram"] = instagram_data["results"]["instagram"]
                results["total_results"] += instagram_data["results"]["instagram"]["total_results"]

            facebook_data = await self.firecrawl_client.search_social_media(query, platforms=["facebook"])
            if facebook_data.get("results", {}).get("facebook", {}).get("total_results", 0) > 0:
                results["platforms"]["facebook"] = facebook_data["results"]["facebook"]
                results["total_results"] += facebook_data["results"]["facebook"]["total_results"]

        if results["total_results"] == 0:
            logger.warning("Nenhuma API de rede social disponível ou retornou dados para a consulta.")
            results["success"] = False
        else:
            results["success"] = True

        logger.info(f"✅ Busca concluída: {results['total_results']} posts encontrados")

        return results

    async def analyze_sentiment_trends(self, platforms_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analisa tendências de sentimento across platforms usando PredictiveAnalyticsService."""

        all_text_content = []
        platform_sentiments = {}

        for platform_name, platform_info in platforms_data.get("platforms", {}).items():
            if isinstance(platform_info, dict) and "results" in platform_info:
                platform_texts = []
                for item in platform_info["results"]:
                    if "content" in item: 
                        platform_texts.append(item["content"])
                    elif "text" in item: 
                        platform_texts.append(item["text"])
                    elif "title" in item and "description" in item: 
                        platform_texts.append(f"{item['title']} {item['description']}")

                if platform_texts:
                    all_text_content.extend(platform_texts)

        if not all_text_content:
            logger.warning("⚠️ Nenhum conteúdo textual encontrado para análise de sentimento.")
            return {
                "overall_sentiment": "neutral",
                "overall_positive_percentage": 0,
                "overall_negative_percentage": 0,
                "overall_neutral_percentage": 0,
                "total_posts_analyzed": 0,
                "platform_breakdown": {},
                "confidence_score": 0,
                "analysis_timestamp": datetime.now().isoformat()
            }

        combined_text = " ".join(all_text_content)
        sentiment_insights = await predictive_analytics_service.analyze_content_chunk(combined_text)

        overall_sentiment_data = sentiment_insights.get("sentiment_analysis", {})
        
        total_posts_analyzed = len(all_text_content) 

        overall_positive = overall_sentiment_data.get("pos", 0.0)
        overall_negative = overall_sentiment_data.get("neg", 0.0)
        overall_neutral = overall_sentiment_data.get("neu", 0.0)
        overall_compound = overall_sentiment_data.get("compound", 0.0)

        overall_sentiment_label = "neutral"
        if overall_compound >= 0.05:
            overall_sentiment_label = "positive"
        elif overall_compound <= -0.05:
            overall_sentiment_label = "negative"

        return {
            "overall_sentiment": overall_sentiment_label,
            "overall_positive_percentage": round(overall_positive * 100, 1),
            "overall_negative_percentage": round(overall_negative * 100, 1),
            "overall_neutral_percentage": round(overall_neutral * 100, 1),
            "total_posts_analyzed": total_posts_analyzed,
            "platform_breakdown": platform_sentiments, 
            "confidence_score": round(abs(overall_positive - overall_negative) * 100, 1) if total_posts_analyzed > 0 else 0,
            "analysis_timestamp": datetime.now().isoformat()
        }

# Instância global
social_media_extractor = SocialMediaExtractor()

# Função para compatibilidade
def get_social_media_extractor():
    """Retorna a instância global do social media extractor"""
    return social_media_extractor
