import { getTenantClient } from './db.js';
import {
  CachedTenant,
  getTenantShifts,
  resolveShiftForTimestamp,
  CachedShift,
} from './cache.js';
import { fetchPointsAfterDirect, TelemetryFeedPoint } from './influx.js';
import { logger } from './logger.js';

interface WorkCenterRecord {
  id: string;
  name: string;
  status: 'STOPPED' | 'PRODUCTION' | 'NO_PROGRAM' | 'PLANNED_STOP';
  productionDeviceId: string | null;
  productionChannel: string | null;
  simulatedProductionDeviceId: string | null;
  simulatedProductionChannel: string | null;
  isSimulatedProduction: boolean;
  counterState: { value: bigint; timestamp: Date } | null;
  processedState: { lastProcessed: Date | null } | null;
  downtimeState: {
    downtimeId: string;
    intervalDowntimeId: string;
    startTime: Date;
    productionMetricsId: string;
  } | null;
  orders: Array<{
    id: string;
    status: string;
    producedQuantity: bigint;
    initialProductionCount: bigint;
    finalProductionCount: bigint;
    counterStarted: boolean;
  }>;
}

// Obtém a data arredondada para o início da hora cheia (ex: 14:00:00.000)
function getHourBucket(date: Date): Date {
  const d = new Date(date);
  d.setMinutes(0, 0, 0);
  return d;
}

export async function processTenantWorkCenters(
  tenant: CachedTenant,
  batchLimit: number,
): Promise<void> {
  const tenantDb = await getTenantClient<any>(tenant.subdomain);
  const shifts = await getTenantShifts(tenant.subdomain);

  // 1. Carrega os centros com seus estados e a OP ativa (se houver)
  const workCenters: WorkCenterRecord[] = await tenantDb.workCenter.findMany({
    where: { deletedAt: null },
    include: {
      counterState: true,
      processedState: true,
      downtimeState: true,
      orders: {
        where: { status: 'RUNNING', deletedAt: null },
        take: 1,
      },
    },
  });

  for (const wc of workCenters) {
    // Define qual dispositivo e canal ler (Simulador ou Hardware Real)
    const effectiveDeviceId = wc.isSimulatedProduction
      ? wc.simulatedProductionDeviceId || 'SIMULADOR-IOT-001'
      : wc.productionDeviceId;

    const effectiveChannel = wc.isSimulatedProduction
      ? wc.simulatedProductionChannel || 'chd1_value'
      : wc.productionChannel || 'chd1_value';

    if (!effectiveDeviceId) continue;

    // Timestamp do último ponto processado
    let lastProcessedTimestampSeconds = 0;
    if (wc.processedState?.lastProcessed) {
      lastProcessedTimestampSeconds = Math.floor(
        new Date(wc.processedState.lastProcessed).getTime() / 1000,
      );
    } else {
      // Padrão: últimas 2 horas caso seja a primeira execução
      lastProcessedTimestampSeconds = Math.floor(Date.now() / 1000) - 2 * 3600;
    }

    // 2. Busca fatias cronológicas paginadas no InfluxDB
    const points: TelemetryFeedPoint[] = await fetchPointsAfterDirect({
      bucket: tenant.influxBucket,
      deviceId: effectiveDeviceId,
      channel: effectiveChannel,
      afterTimestampSeconds: lastProcessedTimestampSeconds,
      limit: batchLimit,
    });

    if (points.length === 0) continue;

    logger.debug(
      `⚙️ [MES Engine] Processando ${points.length} ponto(s) para linha "${wc.name}" (${tenant.subdomain}).`,
    );

    // =========================================================================
    // 3. EXECUÇÃO DA MÁQUINA DE ESTADOS EM MEMÓRIA
    // =========================================================================
    let currentCounter = wc.counterState ? wc.counterState.value : 0n;
    let lastPulseTime = wc.counterState
      ? wc.counterState.timestamp
      : new Date(points[0].timestampMs);
    let latestProcessedPointTimestampMs = 0;

    let activeOrder = wc.orders.length > 0 ? wc.orders[0] : null;
    let workCenterStatus = wc.status;

    // Estado da parada em andamento
    let activeDowntimeId = wc.downtimeState?.downtimeId || null;
    let activeIntervalDowntimeId = wc.downtimeState?.intervalDowntimeId || null;
    let downtimeStartTime = wc.downtimeState
      ? wc.downtimeState.startTime
      : null;
    let currentShiftForDowntime: CachedShift | null = null;
    let currentHourForInterval: Date | null = null;

    if (downtimeStartTime) {
      currentShiftForDowntime = resolveShiftForTimestamp(
        shifts,
        downtimeStartTime.getTime(),
      );
      currentHourForInterval = getHourBucket(downtimeStartTime);
    }

    // Mapa agregador de métricas para persistência em lote ao final:
    // Chave composta: hourIso_shiftId_orderId
    const metricAccumulator = new Map<
      string,
      {
        hour: Date;
        shiftId: string | null;
        orderId: string | null;
        filterOrder: string;
        producedQuantity: bigint;
        initialCount: bigint;
        finalCount: bigint;
      }
    >();

    // Operações de banco coletadas para a transação final
    const downtimesToCreate: any[] = [];
    const downtimesToUpdate: Array<{
      id: string;
      endTime: Date;
      durationSeconds: bigint;
    }> = [];

    const intervalsToCreate: any[] = [];
    const intervalsToUpdate: Array<{
      id: string;
      endTime: Date;
      durationSeconds: bigint;
    }> = [];

    for (const point of points) {
      const pointTime = new Date(point.timestampMs);
      const pointValueBigInt = BigInt(Math.max(0, Math.floor(point.value)));
      latestProcessedPointTimestampMs = point.timestampMs;

      // Cálculo do delta
      let delta = 0n;
      if (pointValueBigInt >= currentCounter) {
        delta = pointValueBigInt - currentCounter;
      } else {
        // Reset de hardware ou overflow
        delta = pointValueBigInt;
      }

      currentCounter = pointValueBigInt;
      const currentShift = resolveShiftForTimestamp(shifts, point.timestampMs);
      const currentHour = getHourBucket(pointTime);

      // -------------------------------------------------------------
      // REGRA A: Linha com Ordem de Produção (`RUNNING`)
      // -------------------------------------------------------------
      if (activeOrder) {
        if (delta > 0n) {
          // Houve avanço de peças: máquina produzindo
          lastPulseTime = pointTime;
          workCenterStatus = 'PRODUCTION';
          activeOrder.producedQuantity += delta;
          activeOrder.finalProductionCount = currentCounter;
          if (!activeOrder.counterStarted) {
            activeOrder.counterStarted = true;
            activeOrder.initialProductionCount = currentCounter;
          }

          // Se a linha estava parada, encerra a parada imediatamente
          if (
            activeDowntimeId &&
            activeIntervalDowntimeId &&
            downtimeStartTime
          ) {
            const durationSec = BigInt(
              Math.max(
                0,
                Math.floor(
                  (pointTime.getTime() - downtimeStartTime.getTime()) / 1000,
                ),
              ),
            );
            downtimesToUpdate.push({
              id: activeDowntimeId,
              endTime: pointTime,
              durationSeconds: durationSec,
            });
            intervalsToUpdate.push({
              id: activeIntervalDowntimeId,
              endTime: pointTime,
              durationSeconds: durationSec,
            });

            activeDowntimeId = null;
            activeIntervalDowntimeId = null;
            downtimeStartTime = null;
          }
        } else {
          // Delta == 0: sem avanço
          const secondsSinceLastPulse = Math.floor(
            (pointTime.getTime() - lastPulseTime.getTime()) / 1000,
          );

          if (secondsSinceLastPulse >= tenant.stopToleranceSeconds) {
            // Estourou a tolerância: linha entra em parada (STOPPED)
            workCenterStatus = 'STOPPED';

            if (!activeDowntimeId) {
              // Abre novo Downtime mestre e seu primeiro IntervalDowntime
              downtimeStartTime = lastPulseTime;
              currentShiftForDowntime = currentShift;
              currentHourForInterval = currentHour;

              const newDowntimeId = `dt_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
              const newIntervalId = `idt_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;

              activeDowntimeId = newDowntimeId;
              activeIntervalDowntimeId = newIntervalId;

              downtimesToCreate.push({
                id: newDowntimeId,
                workCenterId: wc.id,
                orderId: activeOrder.id,
                shiftId: currentShift?.id || null,
                startTime: downtimeStartTime,
                endTime: pointTime,
                durationSeconds: BigInt(secondsSinceLastPulse),
              });

              intervalsToCreate.push({
                id: newIntervalId,
                downtimeId: newDowntimeId,
                workCenterId: wc.id,
                startTime: downtimeStartTime,
                endTime: pointTime,
                durationSeconds: BigInt(secondsSinceLastPulse),
                hour: currentHour,
              });
            } else {
              // Parada contínua:
              // Verifica se houve troca de turno
              const shiftChanged =
                currentShift?.id !== currentShiftForDowntime?.id;
              // Verifica se virou a hora
              const hourChanged =
                currentHour.getTime() !== currentHourForInterval?.getTime();

              if (shiftChanged) {
                // Fecha a parada anterior na virada do turno e abre outra para o novo turno
                if (downtimeStartTime) {
                  const durationSec = BigInt(
                    Math.max(
                      0,
                      Math.floor(
                        (pointTime.getTime() - downtimeStartTime.getTime()) /
                          1000,
                      ),
                    ),
                  );
                  downtimesToUpdate.push({
                    id: activeDowntimeId,
                    endTime: pointTime,
                    durationSeconds: durationSec,
                  });
                  intervalsToUpdate.push({
                    id: activeIntervalDowntimeId!,
                    endTime: pointTime,
                    durationSeconds: durationSec,
                  });
                }

                // Inicia nova parada vinculada ao novo turno
                downtimeStartTime = pointTime;
                currentShiftForDowntime = currentShift;
                currentHourForInterval = currentHour;

                const newDowntimeId = `dt_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                const newIntervalId = `idt_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                activeDowntimeId = newDowntimeId;
                activeIntervalDowntimeId = newIntervalId;

                downtimesToCreate.push({
                  id: newDowntimeId,
                  workCenterId: wc.id,
                  orderId: activeOrder.id,
                  shiftId: currentShift?.id || null,
                  startTime: downtimeStartTime,
                  endTime: pointTime,
                  durationSeconds: 0n,
                });
                intervalsToCreate.push({
                  id: newIntervalId,
                  downtimeId: newDowntimeId,
                  workCenterId: wc.id,
                  startTime: downtimeStartTime,
                  endTime: pointTime,
                  durationSeconds: 0n,
                  hour: currentHour,
                });
              } else if (hourChanged) {
                // Virou a hora: fecha apenas o IntervalDowntime anterior e abre outro no mesmo Downtime pai
                if (downtimeStartTime) {
                  const durSec = BigInt(
                    Math.max(
                      0,
                      Math.floor(
                        (pointTime.getTime() - downtimeStartTime.getTime()) /
                          1000,
                      ),
                    ),
                  );
                  intervalsToUpdate.push({
                    id: activeIntervalDowntimeId!,
                    endTime: pointTime,
                    durationSeconds: durSec,
                  });
                }

                currentHourForInterval = currentHour;
                const newIntervalId = `idt_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                activeIntervalDowntimeId = newIntervalId;

                intervalsToCreate.push({
                  id: newIntervalId,
                  downtimeId: activeDowntimeId,
                  workCenterId: wc.id,
                  startTime: pointTime,
                  endTime: pointTime,
                  durationSeconds: 0n,
                  hour: currentHour,
                });
              } else {
                // Apenas atualiza o endTime corrente e a duração em aberto
                if (downtimeStartTime) {
                  const totalDurSec = BigInt(
                    Math.max(
                      0,
                      Math.floor(
                        (pointTime.getTime() - downtimeStartTime.getTime()) /
                          1000,
                      ),
                    ),
                  );
                  downtimesToUpdate.push({
                    id: activeDowntimeId,
                    endTime: pointTime,
                    durationSeconds: totalDurSec,
                  });
                  intervalsToUpdate.push({
                    id: activeIntervalDowntimeId!,
                    endTime: pointTime,
                    durationSeconds: totalDurSec,
                  });
                }
              }
            }
          }
        }
      } else {
        // -------------------------------------------------------------
        // REGRA B: Linha Sem Programação (`NO_PROGRAM`)
        // -------------------------------------------------------------
        workCenterStatus = 'NO_PROGRAM';
        // Mesmo com pulsos (delta > 0), a parada de "Sem Programação" não é finalizada.
        // As peças continuam sendo computadas em métricas desvinculadas de OP.
      }

      // -------------------------------------------------------------
      // REGRA C: Agrupamento em ProductionMetrics
      // -------------------------------------------------------------
      const shiftKey = currentShift?.id || 'NO_SHIFT';
      const orderKey = activeOrder?.id || 'NO_ORDER';
      const hourKey = currentHour.toISOString();
      const compKey = `${hourKey}_${shiftKey}_${orderKey}`;

      if (!metricAccumulator.has(compKey)) {
        metricAccumulator.set(compKey, {
          hour: currentHour,
          shiftId: currentShift?.id || null,
          orderId: activeOrder?.id || null,
          filterOrder: `${wc.id}_${shiftKey}_${orderKey}`,
          producedQuantity: 0n,
          initialCount: pointValueBigInt,
          finalCount: pointValueBigInt,
        });
      }

      const m = metricAccumulator.get(compKey)!;
      m.producedQuantity += delta;
      m.finalCount = pointValueBigInt;
    }

    // =========================================================================
    // 4. TRANSAÇÃO ATÔMICA FINAL NO POSTGRESQL (PERSISTÊNCIA EM LOTE)
    // =========================================================================
    await tenantDb.$transaction(async (tx: any) => {
      // 4.1 Persistência de ProductionMetrics
      for (const m of metricAccumulator.values()) {
        const initialTime = new Date(m.hour);
        const finalTime = new Date(m.hour.getTime() + 3600 * 1000);

        // Busca se já existe métrica para essa janela
        const existing = await tx.productionMetrics.findFirst({
          where: {
            hour: m.hour,
            workCenterId: wc.id,
            shiftId: m.shiftId,
            filterOrder: m.filterOrder,
          },
        });

        if (existing) {
          await tx.productionMetrics.update({
            where: { id: existing.id },
            data: {
              producedQuantity: { increment: m.producedQuantity },
              finalProductionCount: m.finalCount,
            },
          });
        } else {
          await tx.productionMetrics.create({
            data: {
              hour: m.hour,
              initialTime,
              finalTime,
              workCenterId: wc.id,
              shiftId: m.shiftId,
              orderId: m.orderId,
              filterOrder: m.filterOrder,
              producedQuantity: m.producedQuantity,
              initialProductionCount: m.initialCount,
              finalProductionCount: m.finalCount,
            },
          });
        }
      }

      // 4.2 Criação e Atualização de Downtimes
      for (const dt of downtimesToCreate) {
        // Encontra uma métrica correspondente para vincular
        const pm = await tx.productionMetrics.findFirst({
          where: { workCenterId: wc.id },
          orderBy: { hour: 'desc' },
        });
        if (pm) {
          await tx.downtime.create({
            data: {
              id: dt.id,
              workCenterId: dt.workCenterId,
              orderId: dt.orderId,
              shiftId: dt.shiftId,
              startTime: dt.startTime,
              endTime: dt.endTime,
              durationSeconds: dt.durationSeconds,
              productionMetricsId: pm.id,
            },
          });
        }
      }

      for (const dtu of downtimesToUpdate) {
        await tx.downtime.update({
          where: { id: dtu.id },
          data: { endTime: dtu.endTime, durationSeconds: dtu.durationSeconds },
        });
      }

      // 4.3 Criação e Atualização de IntervalDowntimes
      for (const idt of intervalsToCreate) {
        const pm = await tx.productionMetrics.findFirst({
          where: { workCenterId: wc.id, hour: idt.hour },
        });
        if (pm) {
          await tx.intervalDowntime.create({
            data: {
              id: idt.id,
              downtimeId: idt.downtimeId,
              workCenterId: idt.workCenterId,
              startTime: idt.startTime,
              endTime: idt.endTime,
              durationSeconds: idt.durationSeconds,
              productionMetricsId: pm.id,
            },
          });
        }
      }

      for (const idtu of intervalsToUpdate) {
        await tx.intervalDowntime.update({
          where: { id: idtu.id },
          data: {
            endTime: idtu.endTime,
            durationSeconds: idtu.durationSeconds,
          },
        });
      }

      // 4.4 Atualiza o status da linha fabril
      await tx.workCenter.update({
        where: { id: wc.id },
        data: { status: workCenterStatus },
      });

      // 4.5 Se houver OP ativa, atualiza seus acumuladores
      if (activeOrder) {
        await tx.order.update({
          where: { id: activeOrder.id },
          data: {
            producedQuantity: activeOrder.producedQuantity,
            finalProductionCount: activeOrder.finalProductionCount,
            counterStarted: activeOrder.counterStarted,
          },
        });
      }

      // 4.6 Atualiza CounterState (último valor absoluto)
      await tx.counterState.upsert({
        where: { workCenterId: wc.id },
        update: {
          value: currentCounter,
          timestamp: lastPulseTime,
        },
        create: {
          workCenterId: wc.id,
          value: currentCounter,
          timestamp: lastPulseTime,
        },
      });

      // 4.7 Atualiza DowntimeState com o ponteiro da parada aberta (ou limpa se retomou)
      if (activeDowntimeId && activeIntervalDowntimeId && downtimeStartTime) {
        const pm = await tx.productionMetrics.findFirst({
          where: { workCenterId: wc.id },
          orderBy: { hour: 'desc' },
        });
        if (pm) {
          await tx.downtimeState.upsert({
            where: { workCenterId: wc.id },
            update: {
              downtimeId: activeDowntimeId,
              intervalDowntimeId: activeIntervalDowntimeId,
              startTime: downtimeStartTime,
              productionMetricsId: pm.id,
            },
            create: {
              workCenterId: wc.id,
              downtimeId: activeDowntimeId,
              intervalDowntimeId: activeIntervalDowntimeId,
              startTime: downtimeStartTime,
              productionMetricsId: pm.id,
            },
          });
        }
      } else {
        await tx.downtimeState.deleteMany({
          where: { workCenterId: wc.id },
        });
      }

      // 4.8 Atualiza o ponteiro de processamento do InfluxDB (ProcessedState)
      if (latestProcessedPointTimestampMs > 0) {
        await tx.processedState.upsert({
          where: { workCenterId: wc.id },
          update: {
            lastProcessed: new Date(latestProcessedPointTimestampMs),
          },
          create: {
            workCenterId: wc.id,
            lastProcessed: new Date(latestProcessedPointTimestampMs),
          },
        });
      }
    });

    logger.debug(`💾 [MES Engine] Linha "${wc.name}" persistida com sucesso.`);
  }
}
