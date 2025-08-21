#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Enhanced Workflow Routes
Rotas para o workflow aprimorado em 3 etapas
"""

import logging
import time
import uuid
import asyncio
import os
import glob
from datetime import datetime
from typing import Dict, Any  # Import necessário para Dict e Any
from flask import Blueprint, request, jsonify, send_file
from services.real_search_orchestrator import real_search_orchestrator
from services.viral_content_analyzer import viral_content_analyzer
from services.enhanced_synthesis_engine import enhanced_synthesis_engine
from services.enhanced_module_processor import enhanced_module_processor
from services.comprehensive_report_generator_v3 import comprehensive_report_generator_v3
from services.auto_save_manager import salvar_etapa
from services.predictive_analytics_service import predictive_analytics_service # Import adicionado

logger = logging.getLogger(__name__)

enhanced_workflow_bp = Blueprint("enhanced_workflow", __name__)

@enhanced_workflow_bp.route("/workflow/step1/start", methods=["POST"])
def start_step1_collection():
    """ETAPA 1: Coleta Massiva de Dados com Screenshots"""
    try:
        data = request.get_json()

        # Gera session_id único
        session_id = f"session_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

        # Extrai parâmetros
        segmento = data.get("segmento", "").strip()
        produto = data.get("produto", "").strip()
        publico = data.get("publico", "").strip()

        # Validação
        if not segmento:
            return jsonify({"error": "Segmento é obrigatório"}), 400

        # Constrói query de pesquisa
        query_parts = [segmento]
        if produto:
            query_parts.append(produto)
        query_parts.extend(["Brasil", "2024", "mercado"])

        query = " ".join(query_parts)

        # Contexto da análise
        context = {
            "segmento": segmento,
            "produto": produto,
            "publico": publico,
            "query_original": query,
            "etapa": 1,
            "workflow_type": "enhanced_v3"
        }

        logger.info(f"🚀 ETAPA 1 INICIADA - Sessão: {session_id}")
        logger.info(f"🔍 Query: {query}")

        # Salva início da etapa 1
        salvar_etapa("etapa1_iniciada", {
            "session_id": session_id,
            "query": query,
            "context": context,
            "timestamp": datetime.now().isoformat()
        }, categoria="workflow")

        # Executa coleta massiva em thread separada
        def execute_collection():
            try:
                # Executa busca massiva real
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                try:
                    search_results = loop.run_until_complete(
                        real_search_orchestrator.execute_massive_real_search(
                            query=query,
                            context=context,
                            session_id=session_id
                        )
                    )

                    # Analisa e captura conteúdo viral
                    viral_analysis = loop.run_until_complete(
                        viral_content_analyzer.analyze_and_capture_viral_content(
                            search_results=search_results,
                            session_id=session_id,
                            max_captures=15
                        )
                    )

                finally:
                    loop.close()

                # Gera relatório de coleta
                collection_report = _generate_collection_report(
                    search_results, viral_analysis, session_id, context
                )

                # Salva relatório
                _save_collection_report(collection_report, session_id)

                # Salva resultado da etapa 1
                salvar_etapa("etapa1_concluida", {
                    "session_id": session_id,
                    "search_results": search_results,
                    "viral_analysis": viral_analysis,
                    "collection_report_generated": True,
                    "timestamp": datetime.now().isoformat()
                }, categoria="workflow")

                logger.info(f"✅ ETAPA 1 CONCLUÍDA - Sessão: {session_id}")

            except Exception as e:
                logger.error(f"❌ Erro na execução da Etapa 1: {e}")
                salvar_etapa("etapa1_erro", {
                    "session_id": session_id,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }, categoria="workflow")

        # Inicia execução em background
        import threading
        thread = threading.Thread(target=execute_collection, daemon=True)
        thread.start()

        return jsonify({
            "success": True,
            "session_id": session_id,
            "message": "Etapa 1 iniciada: Coleta massiva de dados",
            "query": query,
            "estimated_duration": "3-5 minutos",
            "next_step": "/api/workflow/step2/start",
            "status_endpoint": f"/api/workflow/status/{session_id}"
        }), 200

    except Exception as e:
        logger.error(f"❌ Erro ao iniciar Etapa 1: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "message": "Falha ao iniciar coleta de dados"
        }), 500

@enhanced_workflow_bp.route("/workflow/step2/start", methods=["POST"])
def start_step2_synthesis():
    """ETAPA 2: Síntese com IA e Busca Ativa"""
    try:
        data = request.get_json()
        session_id = data.get("session_id")

        if not session_id:
            return jsonify({"error": "session_id é obrigatório"}), 400

        logger.info(f"🧠 ETAPA 2 INICIADA - Síntese para sessão: {session_id}")

        # Salva início da etapa 2
        salvar_etapa("etapa2_iniciada", {
            "session_id": session_id,
            "timestamp": datetime.now().isoformat()
        }, categoria="workflow")

        # Executa síntese em thread separada
        def execute_synthesis():
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                try:
                    # Executa síntese master com busca ativa
                    synthesis_result = loop.run_until_complete(
                        enhanced_synthesis_engine.execute_enhanced_synthesis(
                            session_id=session_id,
                            synthesis_type="master_synthesis"
                        )
                    )

                    # Executa síntese comportamental
                    behavioral_result = loop.run_until_complete(
                        enhanced_synthesis_engine.execute_behavioral_synthesis(session_id)
                    )

                    # Executa síntese de mercado
                    market_result = loop.run_until_complete(
                        enhanced_synthesis_engine.execute_market_synthesis(session_id)
                    )

                finally:
                    loop.close()

                # Salva resultado da etapa 2
                salvar_etapa("etapa2_concluida", {
                    "session_id": session_id,
                    "synthesis_result": synthesis_result,
                    "behavioral_result": behavioral_result,
                    "market_result": market_result,
                    "timestamp": datetime.now().isoformat()
                }, categoria="workflow")

                logger.info(f"✅ ETAPA 2 CONCLUÍDA - Sessão: {session_id}")

            except Exception as e:
                logger.error(f"❌ Erro na execução da Etapa 2: {e}")
                salvar_etapa("etapa2_erro", {
                    "session_id": session_id,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }, categoria="workflow")

        # Inicia execução em background
        import threading
        thread = threading.Thread(target=execute_synthesis, daemon=True)
        thread.start()

        return jsonify({
            "success": True,
            "session_id": session_id,
            "message": "Etapa 2 iniciada: Síntese com IA e busca ativa",
            "estimated_duration": "2-4 minutos",
            "next_step": "/api/workflow/step3/start",
            "status_endpoint": f"/api/workflow/status/{session_id}"
        }), 200

    except Exception as e:
        logger.error(f"❌ Erro ao iniciar Etapa 2: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "message": "Falha ao iniciar síntese"
        }), 500

@enhanced_workflow_bp.route("/workflow/step3/start", methods=["POST"])
def start_step3_generation():
    """ETAPA 3: Geração dos 16 Módulos e Relatório Final"""
    try:
        data = request.get_json()
        session_id = data.get("session_id")

        if not session_id:
            return jsonify({"error": "session_id é obrigatório"}), 400

        logger.info(f"📝 ETAPA 3 INICIADA - Geração para sessão: {session_id}")

        # Salva início da etapa 3
        salvar_etapa("etapa3_iniciada", {
            "session_id": session_id,
            "timestamp": datetime.now().isoformat()
        }, categoria="workflow")

        # Executa geração em thread separada
        def execute_generation():
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                try:
                    # Realiza análise preditiva antes de gerar módulos
                    predictive_insights = loop.run_until_complete(
                        predictive_analytics_service.analyze_all_data(session_id)
                    )
                    logger.info(f"📊 Insights preditivos gerados: {predictive_insights}")

                    # Gera todos os 16 módulos, passando os insights preditivos
                    modules_result = loop.run_until_complete(
                        enhanced_module_processor.generate_all_modules(session_id, context={"initial_predictive_insights": predictive_insights})
                    )

                    # Compila relatório final, passando os insights preditivos
                    final_report = comprehensive_report_generator_v3.compile_final_markdown_report(session_id, predictive_insights)

                finally:
                    loop.close()

                # Salva resultado da etapa 3
                salvar_etapa("etapa3_concluida", {
                    "session_id": session_id,
                    "modules_result": modules_result,
                    "final_report": final_report,
                    "predictive_insights": predictive_insights, # Salva os insights preditivos
                    "timestamp": datetime.now().isoformat()
                }, categoria="workflow")

                logger.info(f"✅ ETAPA 3 CONCLUÍDA - Sessão: {session_id}")
                logger.info(f"📊 {modules_result.get("successful_modules", 0)}/16 módulos gerados")

            except Exception as e:
                logger.error(f"❌ Erro na execução da Etapa 3: {e}")
                salvar_etapa("etapa3_erro", {
                    "session_id": session_id,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }, categoria="workflow")

        # Inicia execução em background
        import threading
        thread = threading.Thread(target=execute_generation, daemon=True)
        thread.start()

        return jsonify({
            "success": True,
            "session_id": session_id,
            "message": "Etapa 3 iniciada: Geração de 16 módulos",
            "estimated_duration": "4-6 minutos",
            "modules_to_generate": 16,
            "status_endpoint": f"/api/workflow/status/{session_id}"
        }), 200

    except Exception as e:
        logger.error(f"❌ Erro ao iniciar Etapa 3: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "message": "Falha ao iniciar geração de módulos"
        }), 500

@enhanced_workflow_bp.route("/workflow/complete", methods=["POST"])
def execute_complete_workflow():
    """Executa workflow completo em sequência"""
    try:
        data = request.get_json()

        # Gera session_id único
        session_id = f"session_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

        logger.info(f"🚀 WORKFLOW COMPLETO INICIADO - Sessão: {session_id}")

        # Executa workflow completo em thread separada
        def execute_full_workflow():
            try:
                # ETAPA 1: Coleta
                logger.info("🌊 Executando Etapa 1: Coleta massiva")

                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                try:
                    # Constrói query
                    segmento = data.get("segmento", "").strip()
                    produto = data.get("produto", "").strip()
                    query = f"{segmento} {produto} Brasil 2024 mercado".strip()                 
                    context = {
                        "segmento": segmento,
                        "produto": produto,
                        "publico": data.get("publico", ""),
                        "preco": data.get("preco", ""),
                        "objetivo_receita": data.get("objetivo_receita", ""),
                        "workflow_type": "complete"
                    }

                    # Executa busca massiva
                    search_results = loop.run_until_complete(
                        real_search_orchestrator.execute_massive_real_search(
                            query=query,
                            context=context,
                            session_id=session_id
                        )
                    )

                    # Analisa conteúdo viral
                    viral_analysis = loop.run_until_complete(
                        viral_content_analyzer.analyze_and_capture_viral_content(
                            search_results=search_results,
                            session_id=session_id
                        )
                    )

                    # Gera relatório de coleta
                    collection_report = _generate_collection_report(
                        search_results, viral_analysis, session_id, context
                    )
                    _save_collection_report(collection_report, session_id)

                    # ETAPA 2: Síntese
                    logger.info("🧠 Executando Etapa 2: Síntese com IA")

                    synthesis_result = loop.run_until_complete(
                        enhanced_synthesis_engine.execute_enhanced_synthesis(session_id)
                    )

                    # ETAPA 2.5: Análise Preditiva (Nova Etapa)
                    logger.info("📊 Executando Análise Preditiva...")
                    predictive_insights = loop.run_until_complete(
                        predictive_analytics_service.analyze_all_data(session_id)
                    )
                    logger.info(f"📊 Insights preditivos gerados: {predictive_insights}")

                    # ETAPA 3: Geração de módulos
                    logger.info("📝 Executando Etapa 3: Geração de módulos")

                    modules_result = loop.run_until_complete(
                        enhanced_module_processor.generate_all_modules(session_id, context={"initial_predictive_insights": predictive_insights})
                    )

                    # Compila relatório final
                    final_report = comprehensive_report_generator_v3.compile_final_markdown_report(session_id, predictive_insights)

                finally:
                    loop.close()

                # Salva resultado final
                salvar_etapa("workflow_completo", {
                    "session_id": session_id,
                    "search_results": search_results,
                    "viral_analysis": viral_analysis,
                    "synthesis_result": synthesis_result,
                    "predictive_insights": predictive_insights, # Salva os insights preditivos
                    "modules_result": modules_result,
                    "final_report": final_report,
                    "timestamp": datetime.now().isoformat()
                }, categoria="workflow")

                logger.info(f"✅ WORKFLOW COMPLETO CONCLUÍDO - Sessão: {session_id}")

            except Exception as e:
                logger.error(f"❌ Erro na execução do Workflow Completo: {e}")
                salvar_etapa("workflow_completo_erro", {
                    "session_id": session_id,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }, categoria="workflow")

        # Inicia execução em background
        import threading
        thread = threading.Thread(target=execute_full_workflow, daemon=True)
        thread.start()

        return jsonify({
            "success": True,
            "session_id": session_id,
            "message": "Workflow completo iniciado",
            "estimated_duration": "10-15 minutos",
            "status_endpoint": f"/api/workflow/status/{session_id}"
        }), 200

    except Exception as e:
        logger.error(f"❌ Erro ao iniciar Workflow Completo: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "message": "Falha ao iniciar workflow completo"
        }), 500

def _generate_collection_report(search_results: Dict[str, Any], viral_analysis: Dict[str, Any], session_id: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """Gera um relatório de coleta de dados (simplificado para demonstração)."""
    report = {
        "title": "Relatório de Coleta de Dados",
        "session_id": session_id,
        "query": search_results.get("query", "N/A"),
        "timestamp": datetime.now().isoformat(),
        "statistics": search_results.get("statistics", {}),
        "viral_content_summary": viral_analysis,
        "context": context
    }
    return report

def _save_collection_report(report: Dict[str, Any], session_id: str):
    """Salva o relatório de coleta em um arquivo JSON."""
    report_dir = os.path.join("analyses_data", session_id)
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, "collection_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logger.info(f"✅ Relatório de coleta salvo em: {report_path}")


