#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Enhanced Module Processor
Processador aprimorado de módulos com IA
"""

import os
import logging
import asyncio
import json
from typing import Dict, List, Any
from datetime import datetime
from pathlib import Path

# Import do Enhanced AI Manager
from services.enhanced_ai_manager import enhanced_ai_manager
from services.auto_save_manager import salvar_etapa, salvar_erro
from services.predictive_analytics_service import predictive_analytics_service # Import adicionado
from modules.cpl_creator import create_devastating_cpl_protocol

logger = logging.getLogger(__name__)

class EnhancedModuleProcessor:
    """Processador aprimorado de módulos"""

    def __init__(self):
        """Inicializa o processador"""
        self.ai_manager = enhanced_ai_manager

        # Lista completa dos módulos (incluindo o novo módulo CPL)
        self.modules_config = {
            "anti_objecao": {
                "title": "Sistema Anti-Objeção",
                "description": "Sistema completo para antecipar e neutralizar objeções",
                "use_active_search": False,
                "type": "standard"
            },
            "avatars": {
                "title": "Avatares do Público-Alvo",
                "description": "Personas detalhadas do público-alvo",
                "use_active_search": False,
                "type": "standard"
            },
            "concorrencia": {
                "title": "Análise Competitiva",
                "description": "Análise completa da concorrência",
                "use_active_search": True,
                "type": "standard"
            },
            "drivers_mentais": {
                "title": "Drivers Mentais",
                "description": "Gatilhos psicológicos e drivers de compra",
                "use_active_search": False,
                "type": "standard"
            },
            "funil_vendas": {
                "title": "Funil de Vendas",
                "description": "Estrutura completa do funil de vendas",
                "use_active_search": False,
                "type": "standard"
            },
            "insights_mercado": {
                "title": "Insights de Mercado",
                "description": "Insights profundos sobre o mercado",
                "use_active_search": True,
                "type": "standard"
            },
            "palavras_chave": {
                "title": "Estratégia de Palavras-Chave",
                "description": "Estratégia completa de SEO e palavras-chave",
                "use_active_search": False,
                "type": "standard"
            },
            "plano_acao": {
                "title": "Plano de Ação",
                "description": "Plano de ação detalhado e executável",
                "use_active_search": False,
                "type": "standard"
            },
            "posicionamento": {
                "title": "Estratégia de Posicionamento",
                "description": "Posicionamento estratégico no mercado",
                "use_active_search": False,
                "type": "standard"
            },
            "pre_pitch": {
                "title": "Estrutura de Pré-Pitch",
                "description": "Estrutura de pré-venda e engajamento",
                "use_active_search": False,
                "type": "standard"
            },
            "predicoes_futuro": {
                "title": "Predições de Mercado",
                "description": "Predições e tendências futuras",
                "use_active_search": True,
                "type": "standard"
            },
            "provas_visuais": {
                "title": "Sistema de Provas Visuais",
                "description": "Provas visuais e sociais",
                "use_active_search": False,
                "type": "standard"
            },
            "metricas_conversao": {
                "title": "Métricas de Conversão",
                "description": "KPIs e métricas de conversão",
                "use_active_search": False,
                "type": "standard"
            },
            "estrategia_preco": {
                "title": "Estratégia de Precificação",
                "description": "Estratégia de preços e monetização",
                "use_active_search": False,
                "type": "standard"
            },
            "canais_aquisicao": {
                "title": "Canais de Aquisição",
                "description": "Canais de aquisição de clientes",
                "use_active_search": False,
                "type": "standard"
            },
            "cronograma_lancamento": {
                "title": "Cronograma de Lançamento",
                "description": "Cronograma detalhado de lançamento",
                "use_active_search": False,
                "type": "standard"
            },
            "cpl_completo": {
                "title": "Protocolo Integrado de CPLs Devastadores",
                "description": "Protocolo completo para criação de sequência de 4 CPLs de alta performance",
                "use_active_search": True,
                "type": "specialized",
                "requires": ["sintese_master", "avatar_data", "contexto_estrategico", "dados_web"]
            }
        }

        logger.info("🚀 Enhanced Module Processor inicializado")

    async def generate_all_modules(self, session_id: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Gera todos os módulos (16 padrão + 1 especializado CPL)"""
        logger.info(f"🚀 Iniciando geração de todos os módulos para sessão: {session_id}")

        # Carrega dados base
        base_data = self._load_base_data(session_id)
        # Adiciona insights iniciais do contexto, se existirem
        if context and "initial_predictive_insights" in context:
            base_data["initial_predictive_insights"] = context["initial_predictive_insights"]
            logger.info("Insights preditivos iniciais adicionados ao base_data para módulos.")

        results = {
            "session_id": session_id,
            "successful_modules": 0,
            "failed_modules": 0,
            "modules_generated": [],
            "modules_failed": [],
            "total_modules": len(self.modules_config)
        }

        # Cria diretório de módulos
        modules_dir = Path(f"analyses_data/{session_id}/modules")
        modules_dir.mkdir(parents=True, exist_ok=True)

        # Gera cada módulo
        for module_name, config in self.modules_config.items():
            try:
                logger.info(f"📝 Gerando módulo: {module_name}")

                # Verifica se é o módulo especializado CPL
                if module_name == "cpl_completo":
                    cpl_content = await create_devastating_cpl_protocol(
                        sintese_master=base_data.get("sintese_master", {}),
                        avatar_data=base_data.get("avatar_data", {}),
                        contexto_estrategico=base_data.get("contexto_estrategico", {}),
                        dados_web=base_data.get("dados_web", {}),
                        session_id=session_id
                    )
                    
                    cpl_json_path = modules_dir / f"{module_name}.json"
                    with open(cpl_json_path, "w", encoding="utf-8") as f:
                        json.dump(cpl_content, f, ensure_ascii=False, indent=2)
                    
                    cpl_md_content = self._format_cpl_content_to_markdown(cpl_content)
                    cpl_md_path = modules_dir / f"{module_name}.md"
                    with open(cpl_md_path, "w", encoding="utf-8") as f:
                        f.write(cpl_md_content)
                else:
                    # Gera conteúdo do módulo padrão
                    prompt = self._get_module_prompt(module_name, config, base_data)
                    if config.get("use_active_search", False):
                        content = await self.ai_manager.generate_with_active_search(
                            prompt=prompt,
                            context=base_data.get("context", ""),
                            session_id=session_id
                        )
                    else:
                        content = await self.ai_manager.generate_text(
                            prompt=prompt
                        )

                    # Salva módulo padrão
                    module_path = modules_dir / f"{module_name}.md"
                    with open(module_path, "w", encoding="utf-8") as f:
                        f.write(content)

                results["successful_modules"] += 1
                results["modules_generated"].append(module_name)

                logger.info(f"✅ Módulo {module_name} gerado com sucesso")

            except Exception as e:
                logger.error(f"❌ Erro ao gerar módulo {module_name}: {e}")
                salvar_erro(f"modulo_{module_name}", e, contexto={"session_id": session_id})
                results["failed_modules"] += 1
                results["modules_failed"].append({
                    "module": module_name,
                    "error": str(e)
                })

        # Gera relatório consolidado
        await self._generate_consolidated_report(session_id, results)

        logger.info(f"✅ Geração concluída: {results["successful_modules"]}/{results["total_modules"]} módulos")

        return results

    def _load_base_data(self, session_id: str) -> Dict[str, Any]:
        """Carrega dados base da sessão"""
        try:
            session_dir = Path(f"analyses_data/{session_id}")

            # Carrega sínteses
            synthesis_data = {}
            for synthesis_file in session_dir.glob("sintese_*.json"):
                try:
                    with open(synthesis_file, "r", encoding="utf-8") as f:
                        synthesis_data[synthesis_file.stem] = json.load(f)
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao carregar síntese {synthesis_file}: {e}")

            # Carrega relatório de coleta
            coleta_content = ""
            coleta_file = session_dir / "relatorio_coleta.md"
            if coleta_file.exists():
                with open(coleta_file, "r", encoding="utf-8") as f:
                    coleta_content = f.read()

            # Carrega dados específicos para o módulo CPL
            sintese_master = {}
            avatar_data = {}
            contexto_estrategico = {}
            dados_web = {}
            
            # Tenta carregar a síntese master
            sintese_master_file = session_dir / "sintese_master_synthesis.json"
            if sintese_master_file.exists():
                try:
                    with open(sintese_master_file, "r", encoding="utf-8") as f:
                        sintese_master = json.load(f)
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao carregar síntese master: {e}")
            
            # Tenta carregar dados do avatar
            avatar_file = session_dir / "avatar_detalhado.json"
            if avatar_file.exists():
                try:
                    with open(avatar_file, "r", encoding="utf-8") as f:
                        avatar_data = json.load(f)
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao carregar dados do avatar: {e}")
            
            # Tenta carregar contexto estratégico
            contexto_file = session_dir / "contexto_estrategico.json"
            if contexto_file.exists():
                try:
                    with open(contexto_file, "r", encoding="utf-8") as f:
                        contexto_estrategico = json.load(f)
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao carregar contexto estratégico: {e}")
            
            # Tenta carregar dados da web
            web_data_file = session_dir / "dados_pesquisa_web.json"
            if web_data_file.exists():
                try:
                    with open(web_data_file, "r", encoding="utf-8") as f:
                        dados_web = json.load(f)
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao carregar dados da web: {e}")

            return {
                "synthesis_data": synthesis_data,
                "coleta_content": coleta_content,
                "context": f"Dados de síntese: {len(synthesis_data)} arquivos. Relatório de coleta: {len(coleta_content)} caracteres.",
                "sintese_master": sintese_master,
                "avatar_data": avatar_data,
                "contexto_estrategico": contexto_estrategico,
                "dados_web": dados_web
            }

        except Exception as e:
            logger.error(f"❌ Erro ao carregar dados base: {e}")
            return {
                "synthesis_data": {}, 
                "coleta_content": "", 
                "context": "",
                "sintese_master": {},
                "avatar_data": {},
                "contexto_estrategico": {},
                "dados_web": {}
            }

    def _get_module_prompt(self, module_name: str, config: Dict[str, Any], base_data: Dict[str, Any]) -> str:
        """Gera prompt para um módulo específico"""

        base_prompt = f"""# {config["title"]}

Você é um especialista em {config["description"].lower()}.

## DADOS DISPONÍVEIS:
{base_data.get("context", "Dados limitados")}

## CONTEXTO DOS DADOS COLETADOS:
{base_data.get("coleta_content", "")[:1000]}...

"""
        # Adiciona insights preditivos ao prompt, se disponíveis
        if "initial_predictive_insights" in base_data:
            base_prompt += f"\n## INSIGHTS PREDITIVOS INICIAIS:\n"
            for key, value in base_data["initial_predictive_insights"].items():
                base_prompt += f"- {key}: {value}\n"
            base_prompt += "\n"

        base_prompt += f"""## TAREFA:
Crie um módulo ultra-detalhado sobre {config["title"]} baseado nos dados coletados e nos insights preditivos.

## ESTRUTURA OBRIGATÓRIA:
1. **Resumo Executivo**
2. **Análise Detalhada**
3. **Estratégias Específicas**
4. **Implementação Prática**
5. **Métricas e KPIs**
6. **Cronograma de Execução**

## REQUISITOS:
- Mínimo 2000 palavras
- Dados específicos do mercado brasileiro
- Estratégias acionáveis
- Métricas mensuráveis
- Formato markdown profissional

Gere um conteúdo extremamente detalhado e prático.
"""

        return base_prompt

    def _format_cpl_content_to_markdown(self, cpl_content: Dict[str, Any]) -> str:
        """Formata o conteúdo do módulo CPL para Markdown"""
        try:
            markdown_content = f"""# {cpl_content.get("titulo", "Protocolo de CPLs Devastadores")}

{cpl_content.get("descricao", "")}

"""

            # Adiciona cada fase do protocolo
            fases = cpl_content.get("fases", {})
            for fase_key, fase_data in fases.items():
                markdown_content += f"## {fase_data.get("titulo", fase_key)}\n\n"
                markdown_content += f"**{fase_data.get("descricao", "")}**\n\n"
                
                # Adiciona seções específicas de cada fase
                if "estrategia" in fase_data:
                    markdown_content += f"### Estratégia\n{fase_data["estrategia"]}\n\n"
                
                if "versoes_evento" in fase_data:
                    markdown_content += "### Versões do Evento\n"
                    for versao in fase_data["versoes_evento"]:
                        markdown_content += f"- **{versao.get("nome_evento", "")}** ({versao.get("tipo", "")}): {versao.get("justificativa_psicologica", "")}\n"
                    markdown_content += "\n"
                
                if "teasers" in fase_data:
                    markdown_content += "### Teasers\n"
                    for teaser in fase_data["teasers"]:
                        markdown_content += f"- {teaser.get("texto", "")} (*{teaser.get("justificativa", "")}*)\n"
                    markdown_content += "\n"
                
                if "historia_transformacao" in fase_data:
                    ht = fase_data["historia_transformacao"]
                    markdown_content += "### História de Transformação\n"
                    markdown_content += f"- **Antes**: {ht.get("antes", "")}\n"
                    markdown_content += f"- **Durante**: {ht.get("durante", "")}\n"
                    markdown_content += f"- **Depois**: {ht.get("depois", "")}\n\n"

                if "elementos_chave" in fase_data:
                    markdown_content += "### Elementos Chave\n"
                    for elemento in fase_data["elementos_chave"]:
                        markdown_content += f"- **{elemento.get("nome", "")}**: {elemento.get("descricao", "")}\n"
                    markdown_content += "\n"

                if "gatilhos_mentais" in fase_data:
                    markdown_content += "### Gatilhos Mentais Aplicados\n"
                    for gatilho in fase_data["gatilhos_mentais"]:
                        markdown_content += f"- **{gatilho.get("nome", "")}**: {gatilho.get("aplicacao", "")}\n"
                    markdown_content += "\n"

                if "exemplos_copy" in fase_data:
                    markdown_content += "### Exemplos de Copy\n"
                    for exemplo in fase_data["exemplos_copy"]:
                        markdown_content += f"- **{exemplo.get("tipo", "")}**: {exemplo.get("copy", "")}\n"
                    markdown_content += "\n"

            markdown_content += f"""## Conclusão

{cpl_content.get("conclusao", "")}

"""
            return markdown_content
        except Exception as e:
            logger.error(f"❌ Erro ao formatar conteúdo CPL para Markdown: {e}")
            return f"Erro ao gerar CPL: {e}"

    async def _generate_consolidated_report(self, session_id: str, results: Dict[str, Any]):
        """Gera um relatório consolidado dos módulos gerados."""
        report_content = f"# Relatório Consolidado de Geração de Módulos\n\n"
        report_content += f"**Sessão ID**: {session_id}\n"
        report_content += f"**Data de Geração**: {datetime.now().isoformat()}\n\n"
        report_content += f"**Total de Módulos**: {results["total_modules"]}\n"
        report_content += f"**Módulos Gerados com Sucesso**: {results["successful_modules"]}\n"
        report_content += f"**Módulos com Falha**: {results["failed_modules"]}\n\n"

        if results["modules_generated"]:
            report_content += "## Módulos Gerados com Sucesso\n"
            for module_name in results["modules_generated"]:
                report_content += f"- {self.modules_config[module_name]["title"]} ({module_name})\n"
            report_content += "\n"

        if results["modules_failed"]:
            report_content += "## Módulos com Falha\n"
            for failed_module in results["modules_failed"]:
                report_content += f"- {self.modules_config[failed_module["module"]]["title"]} ({failed_module["module"]}) - Erro: {failed_module["error"]}\n"
            report_content += "\n"

        report_content += "## Detalhes dos Módulos\n"
        for module_name in results["modules_generated"]:
            module_path = Path(f"analyses_data/{session_id}/modules/{module_name}.md")
            if module_path.exists():
                report_content += f"### {self.modules_config[module_name]["title"]}\n"
                report_content += f"[Ver Módulo Completo]({module_path.name})\n\n"
            else:
                report_content += f"### {self.modules_config[module_name]["title"]} (Conteúdo não encontrado)\n\n"

        report_path = Path(f"analyses_data/{session_id}/relatorio_modulos_gerados.md")
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_content)
        logger.info(f"✅ Relatório consolidado de módulos salvo em: {report_path}")

# Instância global
enhanced_module_processor = EnhancedModuleProcessor()


