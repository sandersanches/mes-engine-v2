import dotenv from 'dotenv';
import { getActiveTenants } from './cache.js';
import { processTenantWorkCenters } from './processor.js';
import { logger } from './logger.js';

dotenv.config();

const LOOP_INTERVAL_MS =
  (Number(process.env.LOOP_INTERVAL_SECONDS) || 10) * 1000;
const BATCH_LIMIT = Number(process.env.BATCH_POINTS_LIMIT) || 500;

let isProcessing = false;

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

    // Agenda a próxima execução garantindo que respeite o tempo restante
    const nextDelay = Math.max(0, LOOP_INTERVAL_MS - elapsedMs);
    setTimeout(executeEngineCycle, nextDelay);
  }
}

async function main(): Promise<void> {
  logger.info('🚀 Inicializando Nexus MES Engine (Worker Autônomo)...');
  logger.info(
    `⏱️ Frequência de ciclo configurada: ${LOOP_INTERVAL_MS / 1000}s`,
  );
  logger.info(`📦 Limite por lote InfluxDB: ${BATCH_LIMIT} pontos`);

  // Inicia o primeiro ciclo imediatamente
  void executeEngineCycle();
}

void main();
