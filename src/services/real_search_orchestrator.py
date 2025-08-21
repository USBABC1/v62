#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Real Search Orchestrator
Orquestrador de busca REAL massiva com rotação de APIs e captura visual
"""

import os
import logging
import asyncio
import aiohttp
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote_plus
import json

from services.predictive_analytics_service import predictive_analytics_service # Import adicionado

logger = logging.getLogger(__name__)

class RealSearchOrchestrator:
    """Orquestrador de busca REAL massiva - ZERO SIMULAÇÃO"""

    def __init__(self):
        """Inicializa orquestrador com todas as APIs reais"""
        self.api_keys = self._load_all_api_keys()
        self.key_indices = {provider: 0 for provider in self.api_keys.keys()}

        # Provedores em ordem de prioridade
        self.providers = [
            'ALIBABA_WEBSAILOR',  # Adicionado como prioridade máxima
            'FIRECRAWL',
            'JINA', 
            'GOOGLE',
            'EXA',
            'SERPER',
            'YOUTUBE',
            'SUPADATA'
        ]

        # URLs base dos serviços
        self.service_urls = {
            'FIRECRAWL': 'https://api.firecrawl.dev/v0/scrape',
            'JINA': 'https://r.jina.ai/',
            'GOOGLE': 'https://www.googleapis.com/customsearch/v1',
            'EXA': 'https://api.exa.ai/search',
            'SERPER': 'https://google.serper.dev/search',
            'YOUTUBE': 'https://www.googleapis.com/youtube/v3/search',
            'SUPADATA': os.getenv('SUPADATA_API_URL', 'https://server.smithery.ai/@supadata-ai/mcp/mcp')
        }

        self.session_stats = {
            'total_searches': 0,
            'successful_searches': 0,
            'failed_searches': 0,
            'api_rotations': {},
            'content_extracted': 0,
            'screenshots_captured': 0
        }

        logger.info(f"🚀 Real Search Orchestrator inicializado com {sum(len(keys) for keys in self.api_keys.values())} chaves totais")

    def _load_all_api_keys(self) -> Dict[str, List[str]]:
        """Carrega todas as chaves de API do ambiente"""
        api_keys = {}

        for provider in ['FIRECRAWL', 'JINA', 'GOOGLE', 'EXA', 'SERPER', 'YOUTUBE', 'SUPADATA', 'X']:
            keys = []

            # Chave principal
            main_key = os.getenv(f"{provider}_API_KEY")
            if main_key:
                keys.append(main_key)

            # Chaves numeradas
            counter = 1
            while True:
                numbered_key = os.getenv(f"{provider}_API_KEY_{counter}")
                if numbered_key:
                    keys.append(numbered_key)
                    counter += 1
                else:
                    break

            if keys:
                api_keys[provider] = keys
                logger.info(f"✅ {provider}: {len(keys)} chaves carregadas")

        return api_keys

    def get_next_api_key(self, provider: str) -> Optional[str]:
        """Obtém próxima chave de API com rotação automática"""
        if provider not in self.api_keys or not self.api_keys[provider]:
            return None

        keys = self.api_keys[provider]
        current_index = self.key_indices[provider]

        # Obtém chave atual
        key = keys[current_index]

        # Rotaciona para próxima
        self.key_indices[provider] = (current_index + 1) % len(keys)

        # Atualiza estatísticas
        if provider not in self.session_stats['api_rotations']:
            self.session_stats['api_rotations'][provider] = 0
        self.session_stats['api_rotations'][provider] += 1

        logger.debug(f"🔄 {provider}: Usando chave {current_index + 1}/{len(keys)}")
        return key

    async def execute_massive_real_search(
        self, 
        query: str, 
        context: Dict[str, Any],
        session_id: str
    ) -> Dict[str, Any]:
        """Executa busca REAL massiva com todos os provedores"""

        logger.info(f"🚀 INICIANDO BUSCA REAL MASSIVA para: {query}")
        start_time = time.time()

        # Estrutura de resultados
        search_results = {
            'query': query,
            'session_id': session_id,
            'search_started': datetime.now().isoformat(),
            'providers_used': [],
            'web_results': [],
            'social_results': [],
            'youtube_results': [],
            'viral_content': [],
            'screenshots_captured': [],
            'statistics': {
                'total_sources': 0,
                'unique_urls': 0,
                'content_extracted': 0,
                'api_calls_made': 0,
                'search_duration': 0
            }
        }

        try:
            # FASE 1: Busca com Alibaba WebSailor (prioritária)
            logger.info("🔍 FASE 1: Busca com Alibaba WebSailor")
            websailor_results = await self._search_alibaba_websailor(query, context)
            
            if websailor_results.get('success'):
                search_results['web_results'].extend(websailor_results['results'])
                search_results['providers_used'].append('ALIBABA_WEBSAILOR')
                logger.info(f"✅ Alibaba WebSailor retornou {len(websailor_results['results'])} resultados")

            # FASE 2: Busca Web Massiva Simultânea (provedores restantes)
            logger.info("🌐 FASE 2: Busca web massiva simultânea")
            web_tasks = []

            # Firecrawl
            if 'FIRECRAWL' in self.api_keys:
                web_tasks.append(self._search_firecrawl(query))

            # Jina
            if 'JINA' in self.api_keys:
                web_tasks.append(self._search_jina(query))

            # Google
            if 'GOOGLE' in self.api_keys:
                web_tasks.append(self._search_google(query))

            # Exa
            if 'EXA' in self.api_keys:
                web_tasks.append(self._search_exa(query))

            # Serper
            if 'SERPER' in self.api_keys:
                web_tasks.append(self._search_serper(query))

            # Executa todas as buscas web simultaneamente
            if web_tasks:
                web_results = await asyncio.gather(*web_tasks, return_exceptions=True)

                for result in web_results:
                    if isinstance(result, Exception):
                        logger.error(f"❌ Erro na busca web: {result}")
                        continue

                    if result.get('success') and result.get('results'):
                        search_results['web_results'].extend(result['results'])
                        search_results['providers_used'].append(result.get('provider', 'unknown'))

            # FASE 3: Busca em Redes Sociais
            logger.info("📱 FASE 3: Busca massiva em redes sociais")
            social_tasks = []

            # YouTube
            if 'YOUTUBE' in self.api_keys:
                social_tasks.append(self._search_youtube(query))

            # Supadata (Instagram, Facebook, TikTok)
            # if 'SUPADATA' in self.api_keys:
            #     social_tasks.append(self._search_supadata(query))

            # Executa buscas sociais
            if social_tasks:
                social_results = await asyncio.gather(*social_tasks, return_exceptions=True)

                for result in social_results:
                    if isinstance(result, Exception):
                        logger.error(f"❌ Erro na busca social: {result}")
                        continue

                    if result.get('success'):
                        if result.get('platform') == 'youtube':
                            search_results['youtube_results'].extend(result.get('results', []))
                        else:
                            search_results['social_results'].extend(result.get('results', []))

            # FASE 4: Identificação de Conteúdo Viral
            logger.info("🔥 FASE 4: Identificando conteúdo viral")
            viral_content = self._identify_viral_content(
                search_results['youtube_results'] + search_results['social_results']
            )
            search_results['viral_content'] = viral_content

            # FASE 5: Captura de Screenshots
            logger.info("📸 FASE 5: Capturando screenshots do conteúdo viral")
            if viral_content:
                screenshots = await self._capture_viral_screenshots(viral_content, session_id)
                search_results['screenshots_captured'] = screenshots
                self.session_stats['screenshots_captured'] = len(screenshots)

            # FASE 6: Análise Preditiva dos dados coletados
            logger.info("📊 FASE 6: Realizando análise preditiva dos dados coletados...")
            predictive_insights = await predictive_analytics_service.analyze_all_data(session_id)
            search_results['predictive_insights'] = predictive_insights
            logger.info(f"✅ Análise preditiva concluída e insights adicionados aos resultados da busca.")

            # Calcula estatísticas finais
            search_duration = time.time() - start_time
            all_results = search_results['web_results'] + search_results['social_results'] + search_results['youtube_results']
            unique_urls = list(set(r.get('url', '') for r in all_results if r.get('url')))

            search_results['statistics'].update({
                'total_sources': len(all_results),
                'unique_urls': len(unique_urls),
                'content_extracted': sum(len(r.get('content', '')) for r in all_results),
                'api_calls_made': sum(self.session_stats['api_rotations'].values()),
                'search_duration': search_duration
            })

            logger.info(f"✅ BUSCA REAL MASSIVA CONCLUÍDA em {search_duration:.2f}s")
            logger.info(f"📊 {len(all_results)} resultados de {len(search_results['providers_used'])} provedores")
            logger.info(f"📸 {len(search_results['screenshots_captured'])} screenshots capturados")

            return search_results

        except Exception as e:
            logger.error(f"❌ ERRO CRÍTICO na busca massiva: {e}")
            raise

    async def _search_alibaba_websailor(self, query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Busca REAL usando Alibaba WebSailor Agent"""
        try:
            # Importa o agente WebSailor
            from services.alibaba_websailor import alibaba_websailor
            
            if not alibaba_websailor or not alibaba_websailor.enabled:
                logger.warning("⚠️ Alibaba WebSailor não está habilitado")
                return {'success': False, 'error': 'Alibaba WebSailor não habilitado'}

            # Executa a pesquisa profunda - CORRIGIDO: chamando o método correto
            research_result = await alibaba_websailor.navigate_and_research_deep(
                query=query,
                context=context,
                max_pages=30,
                depth_levels=2,
                session_id=None # Ou passe session_id se o método aceitar
            )

            if not research_result or not research_result.get('conteudo_consolidado'):
                return {'success': False, 'error': 'Nenhum resultado da pesquisa WebSailor'}

            # Converte resultados do WebSailor para formato padrão
            results = []
            fontes_detalhadas = research_result.get('conteudo_consolidado', {}).get('fontes_detalhadas', [])
            
            for fonte in fontes_detalhadas:
                results.append({
                    'title': fonte.get('title', ''),
                    'url': fonte.get('url', ''),
                    'snippet': '',  # WebSailor não fornece snippet diretamente
                    'source': 'alibaba_websailor',
                    'relevance_score': fonte.get('quality_score', 0.7),
                    'content_length': fonte.get('content_length', 0)
                })

            logger.info(f"✅ Alibaba WebSailor processado com {len(results)} resultados")
            
            return {
                'success': True,
                'provider': 'ALIBABA_WEBSAILOR',
                'results': results,
                'raw_data': research_result
            }

        except ImportError:
            logger.warning("⚠️ Alibaba WebSailor não encontrado")
            return {'success': False, 'error': 'Alibaba WebSailor não disponível'}
        except Exception as e:
            logger.error(f"❌ Erro Alibaba WebSailor: {e}")
            return {'success': False, 'error': str(e)}

    async def _search_firecrawl(self, query: str) -> Dict[str, Any]:
        """Busca REAL usando Firecrawl"""
        try:
            api_key = self.get_next_api_key('FIRECRAWL')
            if not api_key:
                return {'success': False, 'error': 'Firecrawl API key não disponível'}

            # Busca no Google e extrai com Firecrawl
            search_url = f"https://www.google.com/search?q={quote_plus(query)}&hl=pt-BR&gl=BR"

            async with aiohttp.ClientSession() as session:
                headers = {
                    'Authorization': f'Bearer {api_key}',
                    'Content-Type': 'application/json'
                }

                payload = {
                    'url': search_url,
                    'formats': ['markdown', 'html'],
                    'onlyMainContent': True,
                    'includeTags': ['p', 'h1', 'h2', 'h3', 'article'],
                    'excludeTags': ['nav', 'footer', 'aside', 'script'],
                    'waitFor': 3000
                }

                async with session.post(
                    self.service_urls['FIRECRAWL'],
                    json=payload,
                    headers=headers,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        content = data.get('data', {}).get('markdown', '')

                        # Extrai resultados do conteúdo
                        results = self._extract_search_results_from_content(content, 'firecrawl')

                        return {
                            'success': True,
                            'provider': 'FIRECRAWL',
                            'results': results,
                            'raw_content': content[:2000]
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Firecrawl erro {response.status}: {error_text}")
                        return {'success': False, 'error': f'HTTP {response.status}'}

        except Exception as e:
            logger.error(f"❌ Erro Firecrawl: {e}")
            return {'success': False, 'error': str(e)}

    async def _search_jina(self, query: str) -> Dict[str, Any]:
        """Busca REAL usando Jina AI"""
        try:
            api_key = self.get_next_api_key('JINA')
            if not api_key:
                return {'success': False, 'error': 'Jina API key não disponível'}

            # Busca múltiplas URLs com Jina
            search_urls = [
                f"https://www.google.com/search?q={quote_plus(query)}&hl=pt-BR",
                f"https://www.bing.com/search?q={quote_plus(query)}&cc=br",
                f"https://search.yahoo.com/search?p={quote_plus(query)}&ei=UTF-8"
            ]

            results = []

            async with aiohttp.ClientSession() as session:
                for search_url in search_urls:
                    try:
                        jina_url = f"{self.service_urls['JINA']}{search_url}"
                        headers = {
                            'Authorization': f'Bearer {api_key}',
                            'Accept': 'text/plain'
                        }

                        async with session.get(
                            jina_url,
                            headers=headers,
                            timeout=30
                        ) as response:
                            if response.status == 200:
                                content = await response.text()
                                extracted_results = self._extract_search_results_from_content(content, 'jina')
                                results.extend(extracted_results)

                    except Exception as e:
                        logger.warning(f"⚠️ Erro em URL Jina {search_url}: {e}")
                        continue

            return {
                'success': True,
                'provider': 'JINA',
                'results': results[:20]  # Limita a 20 resultados
            }

        except Exception as e:
            logger.error(f"❌ Erro Jina: {e}")
            return {'success': False, 'error': str(e)}

    async def _search_google(self, query: str) -> Dict[str, Any]:
        """Busca REAL usando Google Custom Search API"""
        try:
            api_key = self.get_next_api_key('GOOGLE')
            cx = os.getenv('GOOGLE_CSE_ID')
            if not api_key or not cx:
                return {'success': False, 'error': 'Google API key ou CSE ID não disponível'}

            async with aiohttp.ClientSession() as session:
                params = {
                    'key': api_key,
                    'cx': cx,
                    'q': query,
                    'num': 10,  # Número de resultados
                    'hl': 'pt-BR', # Idioma da interface
                    'gl': 'BR' # País de pesquisa
                }
                async with session.get(self.service_urls['GOOGLE'], params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = []
                        for item in data.get('items', []):
                            results.append({
                                'title': item.get('title'),
                                'url': item.get('link'),
                                'snippet': item.get('snippet'),
                                'source': 'google'
                            })
                        return {
                            'success': True,
                            'provider': 'GOOGLE',
                            'results': results
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Google Search erro {response.status}: {error_text}")
                        return {'success': False, 'error': f'HTTP {response.status}'}
        except Exception as e:
            logger.error(f"❌ Erro Google Search: {e}")
            return {'success': False, 'error': str(e)}

    async def _search_exa(self, query: str) -> Dict[str, Any]:
        """Busca REAL usando Exa API"""
        try:
            api_key = self.get_next_api_key('EXA')
            if not api_key:
                return {'success': False, 'error': 'Exa API key não disponível'}

            async with aiohttp.ClientSession() as session:
                headers = {
                    'x-api-key': api_key,
                    'Content-Type': 'application/json'
                }
                payload = {
                    'query': query,
                    'num_results': 10,
                    'type': 'search' # Pode ser 'search' ou 'find_similar'
                }
                async with session.post(self.service_urls['EXA'], headers=headers, json=payload, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = []
                        for item in data.get('results', []):
                            results.append({
                                'title': item.get('title'),
                                'url': item.get('url'),
                                'snippet': item.get('text'), # Exa usa 'text' como snippet
                                'source': 'exa'
                            })
                        return {
                            'success': True,
                            'provider': 'EXA',
                            'results': results
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Exa API erro {response.status}: {error_text}")
                        return {'success': False, 'error': f'HTTP {response.status}'}
        except Exception as e:
            logger.error(f"❌ Erro Exa API: {e}")
            return {'success': False, 'error': str(e)}

    async def _search_serper(self, query: str) -> Dict[str, Any]:
        """Busca REAL usando Serper API"""
        try:
            api_key = self.get_next_api_key('SERPER')
            if not api_key:
                return {'success': False, 'error': 'Serper API key não disponível'}

            async with aiohttp.ClientSession() as session:
                headers = {
                    'X-API-KEY': api_key,
                    'Content-Type': 'application/json'
                }
                payload = {
                    'q': query,
                    'gl': 'br', # País
                    'hl': 'pt' # Idioma
                }
                async with session.post(self.service_urls['SERPER'], headers=headers, json=payload, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = []
                        for item in data.get('organic', []):
                            results.append({
                                'title': item.get('title'),
                                'url': item.get('link'),
                                'snippet': item.get('snippet'),
                                'source': 'serper'
                            })
                        return {
                            'success': True,
                            'provider': 'SERPER',
                            'results': results
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Serper API erro {response.status}: {error_text}")
                        return {'success': False, 'error': f'HTTP {response.status}'}
        except Exception as e:
            logger.error(f"❌ Erro Serper API: {e}")
            return {'success': False, 'error': str(e)}

    async def _search_youtube(self, query: str) -> Dict[str, Any]:
        """Busca REAL usando YouTube Data API"""
        try:
            api_key = self.get_next_api_key('YOUTUBE')
            if not api_key:
                return {'success': False, 'error': 'YouTube API key não disponível'}

            async with aiohttp.ClientSession() as session:
                params = {
                    'part': 'snippet',
                    'q': query,
                    'type': 'video',
                    'maxResults': 10,
                    'key': api_key,
                    'regionCode': 'BR',
                    'relevanceLanguage': 'pt'
                }
                async with session.get(self.service_urls['YOUTUBE'], params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = []
                        for item in data.get('items', []):
                            results.append({
                                'title': item['snippet']['title'],
                                'url': f"https://www.youtube.com/watch?v={item['id']['videoId']}",
                                'snippet': item['snippet']['description'],
                                'source': 'youtube',
                                'published_at': item['snippet']['publishedAt']
                            })
                        return {
                            'success': True,
                            'provider': 'YOUTUBE',
                            'platform': 'youtube',
                            'results': results
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ YouTube API erro {response.status}: {error_text}")
                        return {'success': False, 'error': f'HTTP {response.status}'}
        except Exception as e:
            logger.error(f"❌ Erro YouTube API: {e}")
            return {'success': False, 'error': str(e)}

    async def _search_supadata(self, query: str) -> Dict[str, Any]:
        """Busca REAL usando Supadata API para redes sociais (Instagram, Facebook, TikTok)"""
        try:
            api_key = self.get_next_api_key('SUPADATA')
            if not api_key:
                return {'success': False, 'error': 'Supadata API key não disponível'}

            async with aiohttp.ClientSession() as session:
                headers = {
                    'Authorization': f'Bearer {api_key}',
                    'Content-Type': 'application/json'
                }
                payload = {
                    'query': query,
                    'platforms': ['instagram', 'facebook', 'tiktok'], # Exemplo de plataformas
                    'num_results': 10
                }
                async with session.post(self.service_urls['SUPADATA'], headers=headers, json=payload, timeout=60) as response:
                    if response.status == 200:
                        data = await response.json()
                        results = []
                        for platform, posts in data.get('results', {}).items():
                            for post in posts:
                                results.append({
                                    'title': post.get('title', ''),
                                    'url': post.get('url', ''),
                                    'snippet': post.get('text', ''),
                                    'source': 'supadata',
                                    'platform': platform,
                                    'likes': post.get('likes', 0),
                                    'comments': post.get('comments', 0),
                                    'shares': post.get('shares', 0),
                                    'published_at': post.get('published_at')
                                })
                        return {
                            'success': True,
                            'provider': 'SUPADATA',
                            'results': results
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Supadata API erro {response.status}: {error_text}")
                        return {'success': False, 'error': f'HTTP {response.status}'}
        except Exception as e:
            logger.error(f"❌ Erro Supadata API: {e}")
            return {'success': False, 'error': str(e)}

    def _identify_viral_content(self, social_media_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Identifica conteúdo potencialmente viral com base em métricas de engajamento."""
        viral_threshold_likes = 1000  # Exemplo: mais de 1000 likes
        viral_threshold_comments = 100 # Exemplo: mais de 100 comentários
        viral_threshold_shares = 50   # Exemplo: mais de 50 compartilhamentos

        viral_content = []
        for item in social_media_results:
            likes = item.get('likes', 0)
            comments = item.get('comments', 0)
            shares = item.get('shares', 0)

            if (likes >= viral_threshold_likes or
                comments >= viral_threshold_comments or
                shares >= viral_threshold_shares):
                viral_content.append(item)
        
        logger.info(f"🔥 Identificados {len(viral_content)} itens de conteúdo potencialmente viral.")
        return viral_content

    async def _capture_viral_screenshots(self, viral_content: List[Dict[str, Any]], session_id: str) -> List[str]:
        """Captura screenshots de URLs de conteúdo viral."""
        from services.visual_content_capture import visual_content_capture
        
        captured_screenshots = []
        for item in viral_content:
            url = item.get('url')
            if url:
                try:
                    screenshot_path = await visual_content_capture.capture_screenshot_async(url, session_id)
                    if screenshot_path:
                        captured_screenshots.append(screenshot_path)
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao capturar screenshot para {url}: {e}")
        
        logger.info(f"📸 Capturados {len(captured_screenshots)} screenshots de conteúdo viral.")
        return captured_screenshots

    def _extract_search_results_from_content(self, content: str, source: str) -> List[Dict[str, Any]]:
        """Extrai títulos, URLs e snippets de conteúdo textual (ex: de Firecrawl ou Jina)."""
        results = []
        # Regex simplificado para encontrar URLs e títulos/snippets próximos
        # Isso é um placeholder e pode precisar de refinamento dependendo do formato do conteúdo real
        
        # Exemplo: busca por links Markdown [title](url)
        links = re.findall(r'\[(.*?)\]\((https?://[\w\d\./\-]+)\)', content)
        for title, url in links:
            results.append({
                'title': title,
                'url': url,
                'snippet': '', # Snippet pode ser mais difícil de extrair com regex simples
                'source': source
            })

        # Fallback: busca por URLs simples
        if not results:
            urls = re.findall(r'https?://[\w\d\./\-]+', content)
            for url in urls:
                results.append({
                    'title': f"Conteúdo de {url}",
                    'url': url,
                    'snippet': content[:200] + "..." if len(content) > 200 else content,
                    'source': source
                })

        return results

# Instância global
real_search_orchestrator = RealSearchOrchestrator()


