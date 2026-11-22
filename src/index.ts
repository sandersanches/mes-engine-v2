// mes-engine\src\index.ts

import dotenv from 'dotenv';
import { getActiveTenants } from './cache.js';
import { processTenantWorkCenters } from './processor.js';
import { processTenantProcessVariables } from './guardian-processor.js';
import { processTenantScheduledTasks } from './paperless-scheduler.js';
import { logger } from './logger.js';

dotenv.config();

// Configurações do Motor Principal (OEE / Produção / Paradas)
const LOOP_INTERVAL_MS =
  (Number(process.env.LOOP_INTERVAL_SECONDS) || 10) * 1000;
const BATCH_LIMIT = Number(process.env.BATCH_POINTS_LIMIT) || 500;

// Configurações do Motor Paralelo 1 (Nexus Guardian - Variáveis de Processo)
const GUARDIAN_LOOP_INTERVAL_MS =
  (Number(process.env.GUARDIAN_LOOP_INTERVAL_SECONDS) || 5) * 1000;
const GUARDIAN_BATCH_LIMIT =
  Number(process.env.GUARDIAN_BATCH_POINTS_LIMIT) || 300;

// Configurações do Motor Paralelo 2 (Paperless & CEP - Agendamentos e Pendências)
const PAPERLESS_LOOP_INTERVAL_MS =
  (Number(process.env.PAPERLESS_LOOP_INTERVAL_SECONDS) || 30) * 1000;

let isProcessing = false;
let isGuardianProcessing = false;
let isSchedulerProcessing = false;

async function executeEngineCycle(): Promise<void> {
  // Trava de concorrência: nunca inicia novo ciclo enquanto o anterior estiver rodando
  if (isProcessing) {
    logger.warn(
      '⚠️ [MES Engine] Ciclo anterior ainda em execução. Aguardando finalização...',
    );
    return;
  }

  isProcessing = true;
  const cycleStart = Date.now();

  try {
    const tenants = await getActiveTenants();

    for (const tenant of tenants) {
      try {
        await processTenantWorkCenters(tenant, BATCH_LIMIT);
      } catch (tenantErr: unknown) {
        const errorMsg =
          tenantErr instanceof Error ? tenantErr.message : String(tenantErr);
        logger.error(
          `❌ [MES Engine] Erro ao processar tenant "${tenant.subdomain}":`,
          errorMsg,
        );
      }
    }
  } catch (err: unknown) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    logger.error('❌ [MES Engine] Erro fatal no ciclo do motor MES:', errorMsg);
  } finally {
    isProcessing = false;
    const elapsedMs = Date.now() - cycleStart;

    const nextDelay = Math.max(0, LOOP_INTERVAL_MS - elapsedMs);
    setTimeout(executeEngineCycle, nextDelay);
  }
}

async function executeGuardianCycle(): Promise<void> {
  // Trava de concorrência exclusiva do Guardian: não bloqueia nem é bloqueada pelo motor MES
  if (isGuardianProcessing) {
    logger.warn(
      '⚠️ [Guardian Engine] Ciclo anterior ainda em execução. Aguardando finalização...',
    );
    return;
  }

  isGuardianProcessing = true;
  const cycleStart = Date.now();

  try {
    const tenants = await getActiveTenants();

    for (const tenant of tenants) {
      try {
        await processTenantProcessVariables(tenant, GUARDIAN_BATCH_LIMIT);
      } catch (tenantErr: unknown) {
        const errorMsg =
          tenantErr instanceof Error ? tenantErr.message : String(tenantErr);
        logger.error(
          `❌ [Guardian Engine] Erro ao processar variáveis do tenant "${tenant.subdomain}":`,
          errorMsg,
        );
      }
    }
  } catch (err: unknown) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    logger.error(
      '❌ [Guardian Engine] Erro fatal no ciclo do motor Guardian:',
      errorMsg,
    );
  } finally {
    isGuardianProcessing = false;
    const elapsedMs = Date.now() - cycleStart;

    const nextDelay = Math.max(0, GUARDIAN_LOOP_INTERVAL_MS - elapsedMs);
    setTimeout(executeGuardianCycle, nextDelay);
  }
}

async function executePaperlessSchedulerCycle(): Promise<void> {
  // Trava de concorrência exclusiva do Agendador Paperless/CEP
  if (isSchedulerProcessing) {
    logger.warn(
      '⚠️ [Paperless Scheduler] Ciclo anterior ainda em execução. Aguardando finalização...',
    );
    return;
  }

  isSchedulerProcessing = true;
  const cycleStart = Date.now();

  try {
    const tenants = await getActiveTenants();

    for (const tenant of tenants) {
      try {
        await processTenantScheduledTasks(tenant);
      } catch (tenantErr: unknown) {
        const errorMsg =
          tenantErr instanceof Error ? tenantErr.message : String(tenantErr);
        logger.error(
          `❌ [Paperless Scheduler] Erro ao processar agendamentos do tenant "${tenant.subdomain}":`,
          errorMsg,
        );
      }
    }
  } catch (err: unknown) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    logger.error(
      '❌ [Paperless Scheduler] Erro fatal no ciclo do agendador:',
      errorMsg,
    );
  } finally {
    isSchedulerProcessing = false;
    const elapsedMs = Date.now() - cycleStart;

    const nextDelay = Math.max(0, PAPERLESS_LOOP_INTERVAL_MS - elapsedMs);
    setTimeout(executePaperlessSchedulerCycle, nextDelay);
  }
}

async function main(): Promise<void> {
  logger.info('🚀 Inicializando Nexus MES Engine (Workers Autônomos)...');
  logger.info(
    `⏱️ [MES] Ciclo: ${LOOP_INTERVAL_MS / 1000}s | Lote máx: ${BATCH_LIMIT} pts`,
  );
  logger.info(
    `🛡️ [Guardian] Ciclo: ${GUARDIAN_LOOP_INTERVAL_MS / 1000}s | Lote máx: ${GUARDIAN_BATCH_LIMIT} pts`,
  );
  logger.info(
    `📋 [Paperless/CEP] Ciclo de Agendamentos: ${PAPERLESS_LOOP_INTERVAL_MS / 1000}s`,
  );

  // Inicia os três motores em paralelo de forma totalmente independente
  void executeEngineCycle();
  void executeGuardianCycle();
  void executePaperlessSchedulerCycle();
}

void main();
