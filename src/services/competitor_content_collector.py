import os
import logging
import requests
from typing import List, Dict, Any
from services.social_media_extractor import social_media_extractor
from services.visual_content_capture import visual_content_capture
from services.auto_save_manager import auto_save_manager
from datetime import datetime

logger = logging.getLogger(__name__)

class CompetitorContentCollector:
    def __init__(self):
        self.social_media_extractor = social_media_extractor
        self.visual_content_capture = visual_content_capture
        self.auto_save_manager = auto_save_manager
        self.competitors_config = {}
        self.competitor_content_db = []

    def add_competitor(self, name: str, social_media_queries: List[str]):
        """Adiciona ou atualiza a configuração de um concorrente com queries de redes sociais."""
        self.competitors_config[name] = {"social_media_queries": social_media_queries, "last_crawled": None}
        logger.info(f"Concorrente {name} adicionado/atualizado com queries: {social_media_queries}")

    def collect_and_analyze_social_media_content(self, competitor_name: str, session_id: str) -> List[Dict[str, Any]]:
        """Coleta e analisa o conteúdo de redes sociais de um concorrente específico."""
        config = self.competitors_config.get(competitor_name)
        if not config:
            logger.warning(f"Concorrente {competitor_name} não configurado para redes sociais.")
            return []

        all_new_content_items = []
        for query in config["social_media_queries"]:
            logger.info(f"Extraindo conteúdo de redes sociais para '{query}' do concorrente {competitor_name}")
            extracted_data = self.social_media_extractor.extract_comprehensive_data(query, {}, session_id)
            
            if extracted_data.get("success"):
                platform_data = extracted_data.get("all_platforms_data", {})
                for platform, data in platform_data.items():
                    if isinstance(data, dict) and "results" in data:
                        for item in data["results"]:
                            content_item = {
                                "competitor": competitor_name,
                                "platform": platform,
                                "query": query,
                                "content": item,
                                "extracted_at": datetime.now().isoformat()
                            }
                            self.competitor_content_db.append(content_item)
                            all_new_content_items.append(content_item)
                            
                            # Captura de screenshots para conteúdo de alto engajamento
                            if platform == "youtube" and item.get("view_count") and int(item["view_count"]) > 10000:
                                logger.info(f"Capturando screenshot de vídeo YouTube de alto engajamento: {item['url']}")
                                screenshot_path = f"./screenshots/{session_id}/{competitor_name}_{platform}_{item['title'][:20].replace(' ', '_')}.png"
                                self.visual_content_capture.capture_screenshot(item["url"], screenshot_path)
                                content_item["screenshot_path"] = screenshot_path
                            elif (platform == "instagram" or platform == "facebook") and item.get("like_count") and int(item["like_count"]) > 500:
                                logger.info(f"Capturando screenshot de post {platform} de alto engajamento: {item['url']}")
                                screenshot_path = f"./screenshots/{session_id}/{competitor_name}_{platform}_{item['caption'][:20].replace(' ', '_')}.png"
                                self.visual_content_capture.capture_screenshot(item["url"], screenshot_path)
                                content_item["screenshot_path"] = screenshot_path

                self.auto_save_manager.salvar_etapa(
                    f"social_media_analysis_{competitor_name}_{query.replace(' ', '_')}",
                    extracted_data,
                    "social_media_analysis",
                    session_id
                )
            else:
                logger.warning(f"Falha ao extrair conteúdo de redes sociais para '{query}': {extracted_data.get('error', 'Erro desconhecido')}")
        
        config["last_crawled"] = datetime.now().isoformat()
        logger.info(f"Coleta e análise de redes sociais para {competitor_name} concluída. Novos itens: {len(all_new_content_items)}")
        return all_new_content_items

    def get_competitor_content_summary(self, competitor_name: str = None) -> Dict[str, Any]:
        """Retorna um resumo do conteúdo coletado para um concorrente ou todos."""
        filtered_content = self.competitor_content_db
        if competitor_name:
            filtered_content = [c for c in self.competitor_content_db if c["competitor"] == competitor_name]

        total_items = len(filtered_content)
        unique_platforms = set(item["platform"] for item in filtered_content)
        
        # Exemplo de agregação de métricas de engajamento
        total_likes = sum(item["content"].get("like_count", 0) for item in filtered_content if "like_count" in item["content"])
        total_comments = sum(item["content"].get("comment_count", 0) for item in filtered_content if "comment_count" in item["content"])
        total_views = sum(int(item["content"].get("view_count", "0").replace('K', '000').replace('M', '000000')) for item in filtered_content if "view_count" in item["content"])

        return {
            "total_content_items": total_items,
            "unique_platforms": list(unique_platforms),
            "total_likes": total_likes,
            "total_comments": total_comments,
            "total_views": total_views,
            "content_list": filtered_content
        }

# Instância global
competitor_content_collector = CompetitorContentCollector()


