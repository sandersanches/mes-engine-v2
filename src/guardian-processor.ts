// mes-engine\src\guardian-processor.ts
import { getTenantClient } from './db.js';
import { CachedTenant } from './cache.js';
import {
  fetchProcessVariablePointsAfter,
  TelemetryFeedPoint,
} from './influx.js';
import { logger } from './logger.js';

interface ActiveAlertRecord {
  id: string;
  startTime: Date;
  endTime: Date | null;
  peakValue: number;
  limitType: 'MIN' | 'MAX';
}

interface ProcessVariableRecord {
  id: string;
  name: string;
  signalNature: 'ANALOG' | 'DIGITAL';
  deviceId: string | null;
  channel: string | null;
  measurement: string | null;
  field: string | null;
  isSimulated: boolean;
  simulatedDeviceId: string | null;
  simulatedChannel: string | null;
  lastvalue: number;
  lastUpdate: Date | null;
  isMinLimitMonitoring: boolean;
  isMaxLimitMonitoring: boolean;
  minValueLimit: number | null;
  maxValueLimit: number | null;
  currentAlertLogId: string | null;
  currentAlertLog: ActiveAlertRecord | null;
}

interface AlertInMemory {
  id: string | null; // null se ainda não existe no PostgreSQL
  startTime: Date;
  endTime: Date | null;
  peakValue: number;
  limitType: 'MIN' | 'MAX';
  isDirty: boolean;
}

// function evaluateViolation(
//   v: ProcessVariableRecord,
//   value: number,
// ): 'MIN' | 'MAX' | null {
//   if (
//     v.isMaxLimitMonitoring &&
//     v.maxValueLimit !== null &&
//     v.maxValueLimit !== undefined &&
//     value > v.maxValueLimit
//   ) {
//     return 'MAX';
//   }

//   if (
//     v.isMinLimitMonitoring &&
//     v.minValueLimit !== null &&
//     v.minValueLimit !== undefined &&
//     value < v.minValueLimit
//   ) {
//     return 'MIN';
//   }

//   return null;
// }

function evaluateViolation(
  v: ProcessVariableRecord & {
    isAlarmEnabled?: boolean;
    expectedState?: boolean;
  },
  value: number,
): 'MIN' | 'MAX' | null {
  if (v.isAlarmEnabled === false) return null;

  if (v.signalNature === 'DIGITAL') {
    const currentBool = value >= 0.5;
    const expected = v.expectedState ?? true;
    if (currentBool !== expected) {
      return expected ? 'MIN' : 'MAX';
    }
    return null;
  }

  if (
    v.isMaxLimitMonitoring &&
    v.maxValueLimit !== null &&
    v.maxValueLimit !== undefined &&
    value > v.maxValueLimit
  ) {
    return 'MAX';
  }

  if (
    v.isMinLimitMonitoring &&
    v.minValueLimit !== null &&
    v.minValueLimit !== undefined &&
    value < v.minValueLimit
  ) {
    return 'MIN';
  }

  return null;
}

export async function processTenantProcessVariables(
  tenant: CachedTenant,
  batchLimit: number,
): Promise<void> {
  // Ignora tenants que não têm o módulo PROCESS_GUARDIAN ativo
  if (!tenant.activeModules.includes('PROCESS_GUARDIAN')) {
    return;
  }

  const tenantDb = await getTenantClient<any>(tenant.subdomain);

  const variables: ProcessVariableRecord[] =
    await tenantDb.processVariable.findMany({
      where: { deletedAt: null },
      include: {
        currentAlertLog: true,
      },
    });

  if (variables.length === 0) return;

  for (const v of variables) {
    const deviceId = v.isSimulated
      ? v.simulatedDeviceId?.trim() || 'SIMULADOR-IOT-001'
      : v.deviceId?.trim();

    const channel = v.isSimulated
      ? v.simulatedChannel?.trim() || 'ch1_user_range'
      : v.channel?.trim() || 'ch1_user_range';

    // if (!deviceId || !channel) continue;

    // //Nota: O coletor MQTT grava com measurement "device_telemetry" e field "value".
    // // Caso a variável utilize os padrões legados ("telemetry"), normalizamos para "device_telemetry"
    // // se não houver customização específica.
    // const measurement =
    //   !v.measurement || v.measurement === 'telemetry'
    //     ? 'device_telemetry'
    //     : v.measurement;
    // const field = v.field || 'value';

    // const afterTimestampMs = v.lastUpdate
    //   ? new Date(v.lastUpdate).getTime()
    //   : Date.now() - 2 * 3600 * 1000;

    if (!deviceId || !channel) continue;

    // O mqtt-collector sempre grava no InfluxDB com measurement="device_telemetry",
    // tag channel="<canal>" e field="value"
    const measurement = 'device_telemetry';
    const field = 'value';

    const afterTimestampMs = v.lastUpdate
      ? new Date(v.lastUpdate).getTime()
      : Date.now() - 2 * 3600 * 1000;

    // 1. Busca em lote ordenado do mais antigo para o mais recente, limitado a `batchLimit`
    const points: TelemetryFeedPoint[] = await fetchProcessVariablePointsAfter({
      bucket: tenant.influxBucket,
      deviceId,
      channel,
      measurement,
      field,
      afterTimestampMs,
      limit: batchLimit,
    });

    if (points.length === 0) continue;

    // Garante ordenação cronológica estrita
    points.sort((a, b) => a.timestampMs - b.timestampMs);

    // 2. Processamento 100% em memória de todo o lote antes de tocar no banco
    let latestValue = v.lastvalue;
    let latestTimestampMs = afterTimestampMs;

    let activeAlert: AlertInMemory | null =
      v.currentAlertLog && !v.currentAlertLog.endTime
        ? {
            id: v.currentAlertLog.id,
            startTime: new Date(v.currentAlertLog.startTime),
            endTime: null,
            peakValue: v.currentAlertLog.peakValue,
            limitType: v.currentAlertLog.limitType,
            isDirty: false,
          }
        : null;

    const closedAlertsInBatch: AlertInMemory[] = [];

    for (const pt of points) {
      const pointTime = new Date(pt.timestampMs);
      latestValue = pt.value;
      latestTimestampMs = pt.timestampMs;

      const violationType = evaluateViolation(v, pt.value);

      if (violationType) {
        if (!activeAlert) {
          // Abertura de novo alarme
          activeAlert = {
            id: null,
            startTime: pointTime,
            endTime: null,
            peakValue: pt.value,
            limitType: violationType,
            isDirty: true,
          };
        } else if (activeAlert.limitType === violationType) {
          // Continuação do mesmo alarme: atualiza o valor de pico (peakValue)
          if (violationType === 'MAX' && pt.value > activeAlert.peakValue) {
            activeAlert.peakValue = pt.value;
            activeAlert.isDirty = true;
          } else if (
            violationType === 'MIN' &&
            pt.value < activeAlert.peakValue
          ) {
            activeAlert.peakValue = pt.value;
            activeAlert.isDirty = true;
          }
        } else {
          // Inversão direta de limite (ex: saiu de MIN direto para MAX): fecha o anterior e abre novo
          activeAlert.endTime = pointTime;
          activeAlert.isDirty = true;
          closedAlertsInBatch.push({ ...activeAlert });

          activeAlert = {
            id: null,
            startTime: pointTime,
            endTime: null,
            peakValue: pt.value,
            limitType: violationType,
            isDirty: true,
          };
        }
      } else {
        // Leitura dentro da faixa normal
        if (activeAlert) {
          activeAlert.endTime = pointTime;
          activeAlert.isDirty = true;
          closedAlertsInBatch.push({ ...activeAlert });
          activeAlert = null;
        }
      }
    }

    // 3. Persistência consolidada única no banco relacional do tenant
    await tenantDb.$transaction(async (tx: any) => {
      // 3.1. Persiste todos os alertas que foram encerrados durante este lote
      for (const closed of closedAlertsInBatch) {
        if (closed.id) {
          await tx.processVariableAlertLog.update({
            where: { id: closed.id },
            data: {
              endTime: closed.endTime,
              peakValue: closed.peakValue,
            },
          });
        } else {
          // Alerta que abriu e fechou dentro do mesmo lote
          await tx.processVariableAlertLog.create({
            data: {
              processVariableId: v.id,
              startTime: closed.startTime,
              endTime: closed.endTime,
              peakValue: closed.peakValue,
              limitType: closed.limitType,
            },
          });
        }
      }

      // 3.2. Persiste ou atualiza o alerta que permanece ativo ao final do lote
      let finalCurrentAlertId: string | null = null;

      if (activeAlert) {
        if (activeAlert.id) {
          if (activeAlert.isDirty) {
            await tx.processVariableAlertLog.update({
              where: { id: activeAlert.id },
              data: {
                peakValue: activeAlert.peakValue,
              },
            });
          }
          finalCurrentAlertId = activeAlert.id;
        } else {
          const createdAlert = await tx.processVariableAlertLog.create({
            data: {
              processVariableId: v.id,
              startTime: activeAlert.startTime,
              endTime: null,
              peakValue: activeAlert.peakValue,
              limitType: activeAlert.limitType,
            },
          });
          finalCurrentAlertId = createdAlert.id;
        }
      }

      // 3.3. Atualiza ponteiro de leitura (lastUpdate), último valor (lastvalue) e alerta ativo
      await tx.processVariable.update({
        where: { id: v.id },
        data: {
          lastvalue: latestValue,
          lastUpdate: new Date(latestTimestampMs),
          currentAlertLogId: finalCurrentAlertId,
        },
      });
    });

    const hasMoreBacklog = points.length >= batchLimit;
    logger.debug(
      `🛡️ [Guardian Engine] "${v.name}" (${tenant.subdomain}): ${points.length} ponto(s) processado(s) | Último: ${latestValue}${hasMoreBacklog ? ' (mais registros pendentes para o próximo ciclo)' : ''}`,
    );
  }
}
