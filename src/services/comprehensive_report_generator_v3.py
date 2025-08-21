#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Comprehensive Report Generator V3
Compilador de relatório final a partir dos módulos gerados
"""

import os
import logging
import json
from typing import Dict, Any, List
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class ComprehensiveReportGeneratorV3:
    """Compilador de relatório final ultra robusto"""

    def __init__(self):
        """Inicializa o compilador"""
        # Ordem atualizada dos módulos, incluindo os novos módulos de CPL
        self.modules_order = [
            "anti_objecao",
            "avatars", 
            "concorrencia",
            "drivers_mentais",
            "funil_vendas",
            "insights_mercado",
            "palavras_chave",
            "plano_acao",
            "posicionamento",
            "pre_pitch",
            "predicoes_futuro",
            "provas_visuais",
            "metricas_conversao",
            "estrategia_preco",
            "canais_aquisicao",
            "cronograma_lancamento",
            # Novos módulos de CPL adicionados conforme instruções do CPL.txt
            "cpl_completo" # Alterado para o nome do módulo único
        ]

        # Títulos atualizados, incluindo os novos módulos de CPL
        self.module_titles = {
            "anti_objecao": "Sistema Anti-Objeção",
            "avatars": "Avatares do Público-Alvo",
            "concorrencia": "Análise Competitiva",
            "drivers_mentais": "Drivers Mentais",
            "funil_vendas": "Funil de Vendas",
            "insights_mercado": "Insights de Mercado",
            "palavras_chave": "Estratégia de Palavras-Chave",
            "plano_acao": "Plano de Ação",
            "posicionamento": "Estratégia de Posicionamento",
            "pre_pitch": "Estrutura de Pré-Pitch",
            "predicoes_futuro": "Predições de Mercado",
            "provas_visuais": "Sistema de Provas Visuais",
            "metricas_conversao": "Métricas de Conversão",
            "estrategia_preco": "Estratégia de Precificação",
            "canais_aquisicao": "Canais de Aquisição",
            "cronograma_lancamento": "Cronograma de Lançamento",
            "cpl_completo": "Protocolo Integrado de CPLs Devastadores"
        }

        logger.info("📋 Comprehensive Report Generator ULTRA ROBUSTO inicializado")

    def compile_final_markdown_report(self, session_id: str, predictive_insights: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Compila relatório final a partir dos módulos gerados e insights preditivos.

        Args:
            session_id: ID da sessão
            predictive_insights: Dicionário com os insights preditivos do PredictiveAnalyticsEngine.

        Returns:
            Dict com informações do relatório compilado
        """
        logger.info(f"📋 Compilando relatório final para sessão: {session_id}")

        try:
            # 1. Verifica estrutura de diretórios
            session_dir = Path(f"analyses_data/{session_id}")
            modules_dir = session_dir / "modules"
            files_dir = Path(f"analyses_data/files/{session_id}")

            if not session_dir.exists():
                raise Exception(f"Diretório da sessão não encontrado: {session_dir}")

            # 2. Carrega módulos disponíveis
            available_modules = self._load_available_modules(modules_dir)

            # 3. Carrega screenshots disponíveis
            screenshot_paths = self._load_screenshot_paths(files_dir)

            # 4. Compila relatório
            final_report = self._compile_report_content(
                session_id, 
                available_modules, 
                screenshot_paths,
                predictive_insights # Passa os insights preditivos
            )

            # 5. Salva relatório final
            report_path = self._save_final_report(session_id, final_report)

            # 6. Gera estatísticas
            statistics = self._generate_report_statistics(
                available_modules, 
                screenshot_paths, 
                final_report
            )

            logger.info(f"✅ Relatório final compilado: {report_path}")

            return {
                "success": True,
                "session_id": session_id,
                "report_path": report_path,
                "modules_compiled": len(available_modules),
                "screenshots_included": len(screenshot_paths),
                "estatisticas_relatorio": statistics,
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"❌ Erro na compilação: {e}")
            return {
                "success": False,
                "error": str(e),
                "session_id": session_id,
                "timestamp": datetime.now().isoformat()
            }

    def _load_available_modules(self, modules_dir: Path) -> Dict[str, str]:
        """Carrega módulos disponíveis"""
        available_modules = {}

        try:
            if not modules_dir.exists():
                logger.warning(f"⚠️ Diretório de módulos não existe: {modules_dir}")
                return available_modules

            for module_name in self.modules_order:
                # Primeiro tenta carregar arquivo .md
                module_file = modules_dir / f"{module_name}.md"
                if module_file.exists():
                    with open(module_file, "r", encoding="utf-8") as f:
                        content = f.read()
                        if content.strip():
                            available_modules[module_name] = content
                            logger.debug(f"✅ Módulo carregado: {module_name}")
                        else:
                            logger.warning(f"⚠️ Módulo vazio: {module_name}")
                else:
                    # Se não encontrar .md, tenta carregar arquivo .json (para módulos CPL)
                    module_file_json = modules_dir / f"{module_name}.json"
                    if module_file_json.exists():
                        try:
                            with open(module_file_json, "r", encoding="utf-8") as f:
                                json_content = json.load(f)
                                # Converte o conteúdo JSON em uma representação em texto
                                content = json.dumps(json_content, indent=2, ensure_ascii=False)
                                available_modules[module_name] = content
                        except Exception as e:
                            logger.warning(f"⚠️ Erro ao carregar módulo JSON {module_name}: {e}")
                    else:
                        logger.warning(f"⚠️ Módulo não encontrado: {module_name}")

            logger.info(f"📊 {len(available_modules)}/{len(self.modules_order)} módulos carregados")
            return available_modules

        except Exception as e:
            logger.error(f"❌ Erro ao carregar módulos: {e}")
            return available_modules

    def _load_screenshot_paths(self, files_dir: Path) -> List[str]:
        """Carrega caminhos dos screenshots"""
        screenshot_paths = []

        try:
            if not files_dir.exists():
                logger.warning(f"⚠️ Diretório de arquivos não existe: {files_dir}")
                return screenshot_paths

            # Busca por arquivos PNG (screenshots)
            for screenshot_file in files_dir.glob("*.png"):
                relative_path = f"files/{files_dir.name}/{screenshot_file.name}"
                screenshot_paths.append(relative_path)
                logger.debug(f"📸 Screenshot encontrado: {screenshot_file.name}")

            logger.info(f"📸 {len(screenshot_paths)} screenshots encontrados")
            return screenshot_paths

        except Exception as e:
            logger.error(f"❌ Erro ao carregar screenshots: {e}")
            return screenshot_paths

    def _compile_report_content(
        self, 
        session_id: str, 
        modules: Dict[str, str], 
        screenshots: List[str],
        predictive_insights: Dict[str, Any] = None
    ) -> str:
        """Compila conteúdo do relatório final"""

        # Cabeçalho do relatório
        report = f"""# RELATÓRIO FINAL - ARQV30 Enhanced v3.0

**Sessão:** {session_id}  
**Gerado em:** {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}  
**Módulos Compilados:** {len(modules)}/{len(self.modules_order)}  
**Screenshots Incluídos:** {len(screenshots)}

---

## SUMÁRIO EXECUTIVO

Este relatório consolida a análise ultra-detalhada realizada pelo sistema ARQV30 Enhanced v3.0, contemplando {len(modules)} módulos especializados de análise estratégica.

### Módulos Incluídos:
"""

        # Lista de módulos
        for i, module_name in enumerate(self.modules_order, 1):
            title = self.module_titles.get(module_name, module_name.replace("_", " ").title())
            status = "✅" if module_name in modules else "❌"
            report += f"{i}. {status} {title}\n"

        report += "\n---\n\n"

        # Adiciona insights preditivos se disponíveis
        if predictive_insights:
            report += "## INSIGHTS PREDITIVOS E CENÁRIOS FUTUROS\n\n"
            report += "Esta seção apresenta as análises preditivas e os cenários futuros gerados pelo Predictive Analytics Engine, oferecendo uma visão antecipada de tendências e oportunidades.\n\n"
            
            # Adiciona insights textuais
            if predictive_insights.get("textual_insights"):
                report += "### Análise Textual Ultra-Profunda\n"
                report += f"Total de palavras: {predictive_insights['textual_insights'].get('total_words', 'N/A')}\n"
                report += f"Palavras únicas: {predictive_insights['textual_insights'].get('unique_words', 'N/A')}\n"
                report += f"Score de legibilidade: {predictive_insights['textual_insights'].get('readability_score', 'N/A'):.2f}\n"
                report += f"Distribuição de sentimento: {json.dumps(predictive_insights['textual_insights'].get('sentiment_distribution', {}), indent=2, ensure_ascii=False)}\n"
                report += f"Palavras-chave principais: {', '.join(predictive_insights['textual_insights'].get('top_keywords', []))}\n"
                report += f"Entidades principais: {json.dumps(predictive_insights['textual_insights'].get('top_entities', {}), indent=2, ensure_ascii=False)}\n"
                if predictive_insights['textual_insights'].get('topic_modeling', {}).get('topics'):
                    report += "Modelagem de Tópicos:\n"
                    for topic in predictive_insights['textual_insights']['topic_modeling']['topics']:
                        report += f"  - Tópico {topic['id']}: {topic['keywords']}\n"
                report += "\n"

            # Adiciona tendências temporais
            if predictive_insights.get("temporal_trends"):
                report += "### Tendências Temporais e Projeções\n"
                report += f"Volume de conteúdo futuro (90 dias): {predictive_insights['temporal_trends'].get('future_projections', {}).get('content_volume', {}).get('value_in_90_days', 'N/A'):.2f}\n"
                report += f"Tendência de sentimento futuro (90 dias): {predictive_insights['temporal_trends'].get('future_projections', {}).get('average_sentiment', {}).get('value_in_90_days', 'N/A'):.2f}\n"
                report += "\n"

            # Adiciona análise de qualidade dos dados
            if predictive_insights.get("data_quality_assessment"):
                report += "### Avaliação da Qualidade dos Dados\n"
                report += f"Score de completude: {predictive_insights['data_quality_assessment'].get('completeness_score', 'N/A'):.2f}%\n"
                report += f"Score de consistência: {predictive_insights['data_quality_assessment'].get('consistency_score', 'N/A'):.2f}%\n"
                report += f"Score de temporalidade: {predictive_insights['data_quality_assessment'].get('timeliness_score', 'N/A'):.2f}%\n"
                report += f"Score de relevância: {predictive_insights['data_quality_assessment'].get('relevance_score', 'N/A'):.2f}%\n"
                report += f"Score de qualidade geral: {predictive_insights['data_quality_assessment'].get('overall_quality_score', 'N/A'):.2f}%\n"
                if predictive_insights['data_quality_assessment'].get('issues_detected'):
                    report += "Problemas detectados: " + ", ".join(predictive_insights['data_quality_assessment']['issues_detected']) + "\n"
                report += "\n"

            # Adiciona previsões
            if predictive_insights.get("predictions"):
                report += "### Previsões Ultra-Avançadas\n"
                report += f"Previsão de tendência de mercado: {predictive_insights['predictions'].get('market_trend_forecast', {}).get('overall_trend', 'N/A')}\n"
                report += f"Potencial de conteúdo viral: {predictive_insights['predictions'].get('viral_content_potential', {}).get('highest_engagement_score', 'N/A')}\n"
                report += f"Outlook geral do mercado: {predictive_insights['predictions'].get('overall_market_outlook', 'N/A')}\n"
                report += "\n"

            # Adiciona cenários
            if predictive_insights.get("scenarios"):
                report += "### Modelagem de Cenários Complexos\n"
                report += "**Cenário Otimista:**\n"
                report += f"  - Crescimento de Mercado: {predictive_insights['scenarios'].get('optimistic', {}).get('market_growth', 'N/A')}\n"
                report += f"  - Sentimento: {predictive_insights['scenarios'].get('optimistic', {}).get('sentiment', 'N/A')}\n"
                report += f"  - Recomendação: {predictive_insights['scenarios'].get('optimistic', {}).get('recommendation', 'N/A')}\n"
                report += "**Cenário Realista:**\n"
                report += f"  - Crescimento de Mercado: {predictive_insights['scenarios'].get('realistic', {}).get('market_growth', 'N/A')}\n"
                report += f"  - Sentimento: {predictive_insights['scenarios'].get('realistic', {}).get('sentiment', 'N/A')}\n"
                report += f"  - Recomendação: {predictive_insights['scenarios'].get('realistic', {}).get('recommendation', 'N/A')}\n"
                report += "**Cenário Pessimista:**\n"
                report += f"  - Crescimento de Mercado: {predictive_insights['scenarios'].get('pessimistic', {}).get('market_growth', 'N/A')}\n"
                report += f"  - Sentimento: {predictive_insights['scenarios'].get('pessimistic', {}).get('sentiment', 'N/A')}\n"
                report += f"  - Recomendação: {predictive_insights['scenarios'].get('pessimistic', {}).get('recommendation', 'N/A')}\n"
                report += "\n"

            # Adiciona avaliação de riscos e oportunidades
            if predictive_insights.get("risk_assessment"):
                report += "### Avaliação de Riscos e Oportunidades\n"
                report += f"Score de Risco: {predictive_insights['risk_assessment'].get('risk_score', 'N/A'):.2f}\n"
                report += f"Score de Oportunidade: {predictive_insights['risk_assessment'].get('opportunity_score', 'N/A'):.2f}\n"
                if predictive_insights['risk_assessment'].get('identified_risks'):
                    report += "Riscos Identificados:\n"
                    for risk in predictive_insights['risk_assessment']['identified_risks']:
                        report += f"  - Tipo: {risk.get('type', 'N/A')}, Descrição: {risk.get('description', 'N/A')}, Severidade: {risk.get('severity', 'N/A')}\n"
                if predictive_insights['risk_assessment'].get('identified_opportunities'):
                    report += "Oportunidades Identificadas:\n"
                    for opp in predictive_insights['risk_assessment']['identified_opportunities']:
                        report += f"  - Tipo: {opp.get('type', 'N/A')}, Descrição: {opp.get('description', 'N/A')}, Impacto: {opp.get('impact', 'N/A')}\n"
                report += "\n"

            # Adiciona mapeamento de oportunidades estratégicas
            if predictive_insights.get("opportunity_mapping"):
                report += "### Mapeamento de Oportunidades Estratégicas\n"
                if predictive_insights['opportunity_mapping'].get('product_development'):
                    report += "Desenvolvimento de Produto:\n"
                    for item in predictive_insights['opportunity_mapping']['product_development']:
                        report += f"  - {item.get('description', 'N/A')}\n"
                if predictive_insights['opportunity_mapping'].get('marketing_campaigns'):
                    report += "Campanhas de Marketing:\n"
                    for item in predictive_insights['opportunity_mapping']['marketing_campaigns']:
                        report += f"  - {item.get('description', 'N/A')}\n"
                if predictive_insights['opportunity_mapping'].get('content_strategy'):
                    report += "Estratégia de Conteúdo:\n"
                    for item in predictive_insights['opportunity_mapping']['content_strategy']:
                        report += f"  - {item.get('description', 'N/A')}\n"
                report += "\n"

            # Adiciona métricas de confiança
            if predictive_insights.get("confidence_metrics"):
                report += "### Métricas de Confiança\n"
                report += f"Score de Cobertura de Dados: {predictive_insights['confidence_metrics'].get('data_coverage_score', 'N/A'):.2f}%\n"
                report += f"Score de Precisão do Modelo: {predictive_insights['confidence_metrics'].get('model_accuracy_score', 'N/A'):.2f}%\n"
                report += f"Score de Estabilidade da Previsão: {predictive_insights['confidence_metrics'].get('prediction_stability_score', 'N/A'):.2f}%\n"
                report += f"Score de Confiança Geral: {predictive_insights['confidence_metrics'].get('overall_confidence_score', 'N/A'):.2f}%\n"
                report += "\n"

            # Adiciona recomendações estratégicas
            if predictive_insights.get("strategic_recommendations"):
                report += "### Recomendações Estratégicas\n"
                if predictive_insights['strategic_recommendations'].get('short_term'):
                    report += "Curto Prazo:\n"
                    for rec in predictive_insights['strategic_recommendations']['short_term']:
                        report += f"  - Ação: {rec.get('action', 'N/A')}, Prioridade: {rec.get('priority', 'N/A')}\n"
                if predictive_insights['strategic_recommendations'].get('medium_term'):
                    report += "Médio Prazo:\n"
                    for rec in predictive_insights['strategic_recommendations']['medium_term']:
                        report += f"  - Ação: {rec.get('action', 'N/A')}, Prioridade: {rec.get('priority', 'N/A')}\n"
                if predictive_insights['strategic_recommendations'].get('long_term'):
                    report += "Longo Prazo:\n"
                    for rec in predictive_insights['strategic_recommendations']['long_term']:
                        report += f"  - Ação: {rec.get('action', 'N/A')}, Prioridade: {rec.get('priority', 'N/A')}\n"
                report += "\n"

            # Adiciona priorização de ações
            if predictive_insights.get("action_priorities"):
                report += "### Priorização de Ações\n"
                for priority_level, actions in predictive_insights['action_priorities'].items():
                    if actions:
                        report += f"{priority_level.replace('_', ' ').title()}:\n"
                        for action in actions:
                            report += f"  - {action.get('action', 'N/A')}\n"
                report += "\n"

            report += "---\n\n"

        # Adiciona screenshots se disponíveis
        if screenshots:
            report += "## EVIDÊNCIAS VISUAIS\n\n"
            for i, screenshot in enumerate(screenshots, 1):
                report += f"### Screenshot {i}\n"
                report += f"![Screenshot {i}]({screenshot})\n\n"
            report += "---\n\n"

        # Compila módulos na ordem definida
        for module_name in self.modules_order:
            if module_name in modules:
                title = self.module_titles.get(module_name, module_name.replace("_", " ").title())
                report += f"## {title}\n\n"
                
                # Trata módulos CPL de forma especial (JSON)
                if module_name == "cpl_completo": # Alterado para o nome do módulo único
                    try:
                        # Tenta parsear o conteúdo como JSON
                        module_content = json.loads(modules[module_name])
                        report += self._format_cpl_module_content(module_content)
                    except json.JSONDecodeError:
                        # Se não for JSON válido, adiciona o conteúdo como está
                        report += modules[module_name]
                else:
                    # Módulos normais em Markdown
                    report += modules[module_name]
                
                report += "\n\n---\n\n"

        # Rodapé
        report += f"""
## INFORMAÇÕES TÉCNICAS

**Sistema:** ARQV30 Enhanced v3.0  
**Sessão:** {session_id}  
**Data de Compilação:** {datetime.now().strftime("%d/%m/%Y %H:%M:%S")}  
**Módulos Processados:** {len(modules)}/{len(self.modules_order)}  
**Status:** {"Completo" if len(modules) == len(self.modules_order) else "Parcial"}

### Estatísticas de Compilação:
- ✅ Sucessos: {len(modules)}
- ❌ Falhas: {len(self.modules_order) - len(modules)}
- 📊 Taxa de Sucesso: {(len(modules)/len(self.modules_order)*100):.1f}%

---

*Relatório compilado automaticamente pelo ARQV30 Enhanced v3.0*
"""

        return report

    def _format_cpl_module_content(self, cpl_content: Dict[str, Any]) -> str:
        """Formata o conteúdo de um módulo CPL para exibição no relatório"""
        formatted_content = ""
        
        # Adiciona título e descrição se disponíveis
        if "titulo" in cpl_content:
            formatted_content += f"**{cpl_content["titulo"]}**\n\n"
        
        if "descricao" in cpl_content:
            formatted_content += f"{cpl_content["descricao"]}\n\n"
        
        # Adiciona fases se disponíveis
        if "fases" in cpl_content:
            for fase_key, fase_data in cpl_content["fases"].items():
                if isinstance(fase_data, dict):
                    # Título da fase
                    if "titulo" in fase_data:
                        formatted_content += f"### {fase_data["titulo"]}\n\n"
                    
                    # Descrição da fase
                    if "descricao" in fase_data:
                        formatted_content += f"{fase_data["descricao"]}\n\n"
                    
                    # Outros campos da fase
                    for key, value in fase_data.items():
                        if key not in ["titulo", "descricao"]:
                            if isinstance(value, str):
                                formatted_content += f"**{key.replace("_", " ").title()}:** {value}\n\n"
                            elif isinstance(value, list):
                                formatted_content += f"**{key.replace("_", " ").title()}:**\n"
                                for item in value:
                                    if isinstance(item, str):
                                        formatted_content += f"- {item}\n"
                                    elif isinstance(item, dict):
                                        formatted_content += f"- {json.dumps(item, ensure_ascii=False)}\n"
                                formatted_content += "\n"
                            elif isinstance(value, dict):
                                formatted_content += f"**{key.replace("_", " ").title()}:**\n"
                                for sub_key, sub_value in value.items():
                                    formatted_content += f"  - {sub_key}: {sub_value}\n"
                                formatted_content += "\n"
                
        # Adiciona considerações finais se disponíveis
        if "consideracoes_finais" in cpl_content:
            formatted_content += "### Considerações Finais\n\n"
            for key, value in cpl_content["consideracoes_finais"].items():
                if isinstance(value, str):
                    formatted_content += f"**{key.replace("_", " ").title()}:** {value}\n\n"
                elif isinstance(value, list):
                    formatted_content += f"**{key.replace("_", " ").title()}:**\n"
                    for item in value:
                        formatted_content += f"- {item}\n"
                    formatted_content += "\n"
        
        return formatted_content

    def _save_final_report(self, session_id: str, report_content: str) -> str:
        """Salva o relatório final em um arquivo Markdown."""
        report_dir = Path(f"analyses_data/{session_id}")
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / "relatorio_final_completo.md"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_content)
        logger.info(f"✅ Relatório final salvo em: {report_path}")
        return str(report_path)

    def _generate_report_statistics(self, modules: Dict[str, str], screenshots: List[str], final_report_content: str) -> Dict[str, Any]:
        """Gera estatísticas sobre o relatório compilado."""
        stats = {
            "total_modules_expected": len(self.modules_order),
            "total_modules_compiled": len(modules),
            "total_screenshots_included": len(screenshots),
            "report_length_chars": len(final_report_content),
            "report_length_words": len(final_report_content.split()),
            "compilation_timestamp": datetime.now().isoformat()
        }
        return stats

# Instância global
comprehensive_report_generator_v3 = ComprehensiveReportGeneratorV3()


