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
  rejectDeviceId: string | null;
  rejectChannel: string | null;
  isRejectEnabled: boolean;
  simulatedProductionDeviceId: string | null;
  simulatedProductionChannel: string | null;
  simulatedRejectDeviceId: string | null;
  simulatedRejectChannel: string | null;
  isSimulatedProduction: boolean;
  isSimulatedReject: boolean;
  isSimulatedRejectEnabled: boolean;
  counterState: { value: bigint; timestamp: Date } | null;
  processedState: { lastProcessed: Date | null } | null;
  metricState: {
    lastMetricId: string;
    productionMetrics: {
      id: string;
      hour: Date;
      initialTime: Date;
      finalTime: Date;
      shiftId: string | null;
      orderId: string | null;
      filterOrder: string;
      initialProductionCount: bigint;
      finalProductionCount: bigint;
      initialRejectCount: bigint;
      finalRejectCount: bigint;
      producedQuantity: bigint;
      autoRejectedQuantity: bigint;
      manualRejectedQuantity: number;
      totalRejectedQuantity: bigint;
    } | null;
  } | null;
  downtimeState: {
    downtimeId: string;
    intervalDowntimeId: string;
    startTime: Date;
    productionMetricsId: string | null;
    orderId: string | null;
  } | null;
  orders: Array<{
    id: string;
    status: string;
    producedQuantity: bigint;
    autoRejectedQuantity: bigint;
    initialProductionCount: bigint;
    finalProductionCount: bigint;
    initialRejectCount: bigint;
    finalRejectCount: bigint;
    counterStarted: boolean;
  }>;
}

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

  const workCenters: WorkCenterRecord[] = await tenantDb.workCenter.findMany({
    where: { deletedAt: null },
    include: {
      counterState: true,
      processedState: true,
      metricState: {
        include: {
          productionMetrics: true,
        },
      },
      downtimeState: true,
      orders: {
        where: { status: 'RUNNING', deletedAt: null },
        take: 1,
      },
    },
  });

  for (const wc of workCenters) {
    const isSimulated = wc.isSimulatedProduction;
    const prodDeviceId = isSimulated
      ? wc.simulatedProductionDeviceId || 'SIMULADOR-IOT-001'
      : wc.productionDeviceId;
    const prodChannel = isSimulated
      ? wc.simulatedProductionChannel || 'chd1_value'
      : wc.productionChannel || 'chd1_value';

    const isRejectActive = isSimulated
      ? wc.isSimulatedReject && wc.isSimulatedRejectEnabled
      : wc.isRejectEnabled;
    const rejChannel = isSimulated
      ? wc.simulatedRejectChannel || 'chd2_value'
      : wc.rejectChannel || 'chd2_value';

    if (!prodDeviceId) continue;

    const channelsToQuery = [prodChannel];
    if (isRejectActive && rejChannel && rejChannel !== prodChannel) {
      channelsToQuery.push(rejChannel);
    }

    let lastProcessedTimestampSeconds = 0;
    if (wc.processedState?.lastProcessed) {
      lastProcessedTimestampSeconds = Math.floor(
        new Date(wc.processedState.lastProcessed).getTime() / 1000,
      );
    } else {
      lastProcessedTimestampSeconds = Math.floor(Date.now() / 1000) - 2 * 3600;
    }

    const points: TelemetryFeedPoint[] = await fetchPointsAfterDirect({
      bucket: tenant.influxBucket,
      deviceId: prodDeviceId,
      channels: channelsToQuery,
      afterTimestampSeconds: lastProcessedTimestampSeconds,
      limit: batchLimit,
    });

    if (points.length === 0) continue;

    logger.debug(
      `⚙️ [MES Engine] Processando ${points.length} ponto(s) para "${wc.name}" (${tenant.subdomain}).`,
    );

    // =========================================================================
    // RECUPERA ESTADOS ANTERIORES
    // =========================================================================
    let currentProdCounter = wc.counterState ? wc.counterState.value : 0n;
    let currentRejectCounter = 0n;
    let lastPulseTime = wc.counterState
      ? wc.counterState.timestamp
      : new Date(points[0].timestampMs);
    let latestProcessedPointTimestampMs = 0;

    const activeOrder = wc.orders.length > 0 ? wc.orders[0] : null;
    let workCenterStatus = activeOrder ? wc.status : 'NO_PROGRAM';

    // Estados de Downtime
    let activeDowntimeId = wc.downtimeState?.downtimeId || null;
    let activeIntervalDowntimeId = wc.downtimeState?.intervalDowntimeId || null;
    let downtimeStartTime = wc.downtimeState
      ? wc.downtimeState.startTime
      : null;

    let intervalStartTime: Date | null = null;
    if (activeIntervalDowntimeId) {
      const existingInterval = await tenantDb.intervalDowntime.findUnique({
        where: { id: activeIntervalDowntimeId },
      });
      if (existingInterval) {
        intervalStartTime = existingInterval.startTime;
      }
    }

    let currentShiftForDowntime: CachedShift | null = null;
    let currentHourForInterval: Date | null = null;

    if (downtimeStartTime) {
      currentShiftForDowntime = resolveShiftForTimestamp(
        shifts,
        downtimeStartTime.getTime(),
      );
      currentHourForInterval = getHourBucket(
        intervalStartTime || downtimeStartTime,
      );
    }

    const downtimesToCreate: any[] = [];
    const downtimesToUpdate = new Map<
      string,
      { endTime: Date; durationSeconds: bigint }
    >();

    const intervalsToCreate: any[] = [];
    const intervalsToUpdate = new Map<
      string,
      { endTime: Date; durationSeconds: bigint }
    >();

    // =========================================================================
    // INICIALIZAÇÃO DA MÉTRICA ATIVA VIA METRIC_STATE
    // =========================================================================
    interface ActiveMetricMemory {
      id: string | null; // Se nulo, precisa ser criada no banco
      hour: Date;
      initialTime: Date;
      finalTime: Date;
      shiftId: string | null;
      orderId: string | null;
      filterOrder: string;
      initialProdCount: bigint;
      finalProdCount: bigint;
      initialRejCount: bigint;
      finalRejCount: bigint;
      producedQuantity: bigint;
      autoRejectedQuantity: bigint;
      manualRejectedQuantity: number;
      totalRejectedQuantity: bigint;
      isClosed: boolean; // Indica se foi encerrada e não aceita novos pontos
    }

    let currentActiveMetric: ActiveMetricMemory | null = null;

    if (wc.metricState && wc.metricState.productionMetrics) {
      const pm = wc.metricState.productionMetrics;
      currentActiveMetric = {
        id: pm.id,
        hour: new Date(pm.hour),
        initialTime: new Date(pm.initialTime),
        finalTime: new Date(pm.finalTime),
        shiftId: pm.shiftId,
        orderId: pm.orderId,
        filterOrder: pm.filterOrder,
        initialProdCount: BigInt(pm.initialProductionCount ?? 0n),
        finalProdCount: BigInt(pm.finalProductionCount ?? 0n),
        initialRejCount: BigInt(pm.initialRejectCount ?? 0n),
        finalRejCount: BigInt(pm.finalRejectCount ?? 0n),
        producedQuantity: BigInt(pm.producedQuantity ?? 0n),
        autoRejectedQuantity: BigInt(pm.autoRejectedQuantity ?? 0n),
        manualRejectedQuantity: pm.manualRejectedQuantity ?? 0,
        totalRejectedQuantity: BigInt(pm.totalRejectedQuantity ?? 0n),
        isClosed: false,
      };
    }

    // Lista de métricas que foram finalizadas/congeladas durante este lote
    const closedMetricsToPersist: ActiveMetricMemory[] = [];

    // Agrupa e ordena pontos cronologicamente
    const timestampMap = new Map<number, { prod?: number; rej?: number }>();
    for (const p of points) {
      if (!timestampMap.has(p.timestampMs)) {
        timestampMap.set(p.timestampMs, {});
      }
      const entry = timestampMap.get(p.timestampMs)!;
      if (p.channel === prodChannel) entry.prod = p.value;
      if (p.channel === rejChannel) entry.rej = p.value;
    }

    const sortedTimestamps = Array.from(timestampMap.keys()).sort(
      (a, b) => a - b,
    );

    for (const ts of sortedTimestamps) {
      const pointTime = new Date(ts);
      latestProcessedPointTimestampMs = ts;
      const data = timestampMap.get(ts)!;

      let prodDelta = 0n;
      if (data.prod !== undefined) {
        const prodVal = BigInt(Math.max(0, Math.floor(data.prod)));
        if (prodVal >= currentProdCounter) {
          prodDelta = prodVal - currentProdCounter;
        } else {
          prodDelta = prodVal;
        }
        currentProdCounter = prodVal;
      }

      if (data.rej !== undefined) {
        currentRejectCounter = BigInt(Math.max(0, Math.floor(data.rej)));
      }

      const currentShift = resolveShiftForTimestamp(shifts, ts);
      const currentHour = getHourBucket(pointTime);

      // =======================================================================
      // CASO 1: COM ORDEM DE PRODUÇÃO ATIVA (`RUNNING`)
      // =======================================================================
      if (activeOrder) {
        if (prodDelta > 0n) {
          lastPulseTime = pointTime;
          workCenterStatus = 'PRODUCTION';

          activeOrder.finalProductionCount = currentProdCounter;
          activeOrder.finalRejectCount = currentRejectCounter;
          if (!activeOrder.counterStarted) {
            activeOrder.counterStarted = true;
            activeOrder.initialProductionCount = currentProdCounter;
            activeOrder.initialRejectCount = currentRejectCounter;
          }
          activeOrder.producedQuantity =
            activeOrder.finalProductionCount >=
            activeOrder.initialProductionCount
              ? activeOrder.finalProductionCount -
                activeOrder.initialProductionCount
              : activeOrder.finalProductionCount;

          activeOrder.autoRejectedQuantity =
            activeOrder.finalRejectCount >= activeOrder.initialRejectCount
              ? activeOrder.finalRejectCount - activeOrder.initialRejectCount
              : activeOrder.finalRejectCount;

          if (
            activeDowntimeId &&
            activeIntervalDowntimeId &&
            downtimeStartTime &&
            intervalStartTime
          ) {
            const dtDur = BigInt(
              Math.max(
                0,
                Math.floor(
                  (pointTime.getTime() - downtimeStartTime.getTime()) / 1000,
                ),
              ),
            );
            const idtDur = BigInt(
              Math.max(
                0,
                Math.floor(
                  (pointTime.getTime() - intervalStartTime.getTime()) / 1000,
                ),
              ),
            );

            downtimesToUpdate.set(activeDowntimeId, {
              endTime: pointTime,
              durationSeconds: dtDur,
            });
            intervalsToUpdate.set(activeIntervalDowntimeId, {
              endTime: pointTime,
              durationSeconds: idtDur,
            });

            activeDowntimeId = null;
            activeIntervalDowntimeId = null;
            downtimeStartTime = null;
            intervalStartTime = null;
          }
        } else {
          const secondsSincePulse = Math.floor(
            (pointTime.getTime() - lastPulseTime.getTime()) / 1000,
          );

          if (secondsSincePulse >= tenant.stopToleranceSeconds) {
            workCenterStatus = 'STOPPED';

            if (!activeDowntimeId) {
              downtimeStartTime = lastPulseTime;
              intervalStartTime = lastPulseTime;
              currentShiftForDowntime = currentShift;
              currentHourForInterval = currentHour;

              const newDowntimeId = `dt_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
              const newIntervalId = `idt_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
              activeDowntimeId = newDowntimeId;
              activeIntervalDowntimeId = newIntervalId;

              const dur = BigInt(
                Math.max(
                  0,
                  Math.floor(
                    (pointTime.getTime() - lastPulseTime.getTime()) / 1000,
                  ),
                ),
              );

              downtimesToCreate.push({
                id: newDowntimeId,
                workCenterId: wc.id,
                orderId: activeOrder.id,
                shiftId: currentShift?.id || null,
                startTime: downtimeStartTime,
                endTime: pointTime,
                durationSeconds: dur,
              });

              intervalsToCreate.push({
                id: newIntervalId,
                downtimeId: newDowntimeId,
                workCenterId: wc.id,
                startTime: intervalStartTime,
                endTime: pointTime,
                durationSeconds: dur,
              });
            } else {
              const shiftChanged =
                currentShift?.id !== currentShiftForDowntime?.id;
              const hourChanged =
                currentHour.getTime() !== currentHourForInterval?.getTime();

              if (shiftChanged) {
                if (downtimeStartTime && intervalStartTime) {
                  const dtDur = BigInt(
                    Math.max(
                      0,
                      Math.floor(
                        (pointTime.getTime() - downtimeStartTime.getTime()) /
                          1000,
                      ),
                    ),
                  );
                  const idtDur = BigInt(
                    Math.max(
                      0,
                      Math.floor(
                        (pointTime.getTime() - intervalStartTime.getTime()) /
                          1000,
                      ),
                    ),
                  );

                  downtimesToUpdate.set(activeDowntimeId, {
                    endTime: pointTime,
                    durationSeconds: dtDur,
                  });
                  intervalsToUpdate.set(activeIntervalDowntimeId!, {
                    endTime: pointTime,
                    durationSeconds: idtDur,
                  });
                }

                downtimeStartTime = pointTime;
                intervalStartTime = pointTime;
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
                  startTime: intervalStartTime,
                  endTime: pointTime,
                  durationSeconds: 0n,
                });
              } else if (hourChanged) {
                if (intervalStartTime) {
                  const idtDur = BigInt(
                    Math.max(
                      0,
                      Math.floor(
                        (pointTime.getTime() - intervalStartTime.getTime()) /
                          1000,
                      ),
                    ),
                  );
                  intervalsToUpdate.set(activeIntervalDowntimeId!, {
                    endTime: pointTime,
                    durationSeconds: idtDur,
                  });
                }

                intervalStartTime = pointTime;
                currentHourForInterval = currentHour;

                const newIntervalId = `idt_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
                activeIntervalDowntimeId = newIntervalId;

                intervalsToCreate.push({
                  id: newIntervalId,
                  downtimeId: activeDowntimeId,
                  workCenterId: wc.id,
                  startTime: intervalStartTime,
                  endTime: pointTime,
                  durationSeconds: 0n,
                });

                if (downtimeStartTime) {
                  const dtDur = BigInt(
                    Math.max(
                      0,
                      Math.floor(
                        (pointTime.getTime() - downtimeStartTime.getTime()) /
                          1000,
                      ),
                    ),
                  );
                  downtimesToUpdate.set(activeDowntimeId, {
                    endTime: pointTime,
                    durationSeconds: dtDur,
                  });
                }
              } else {
                if (downtimeStartTime && intervalStartTime) {
                  const dtDur = BigInt(
                    Math.max(
                      0,
                      Math.floor(
                        (pointTime.getTime() - downtimeStartTime.getTime()) /
                          1000,
                      ),
                    ),
                  );
                  const idtDur = BigInt(
                    Math.max(
                      0,
                      Math.floor(
                        (pointTime.getTime() - intervalStartTime.getTime()) /
                          1000,
                      ),
                    ),
                  );

                  downtimesToUpdate.set(activeDowntimeId, {
                    endTime: pointTime,
                    durationSeconds: dtDur,
                  });
                  intervalsToUpdate.set(activeIntervalDowntimeId!, {
                    endTime: pointTime,
                    durationSeconds: idtDur,
                  });
                }
              }
            }
          }
        }
      } else {
        // =======================================================================
        // CASO 2: SEM ORDEM DE PRODUÇÃO (`NO_PROGRAM`)
        // =======================================================================
        workCenterStatus = 'NO_PROGRAM';

        if (!activeDowntimeId) {
          downtimeStartTime = pointTime;
          intervalStartTime = pointTime;
          currentShiftForDowntime = currentShift;
          currentHourForInterval = currentHour;

          const newDowntimeId = `dt_np_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
          const newIntervalId = `idt_np_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
          activeDowntimeId = newDowntimeId;
          activeIntervalDowntimeId = newIntervalId;

          downtimesToCreate.push({
            id: newDowntimeId,
            workCenterId: wc.id,
            orderId: null,
            shiftId: currentShift?.id || null,
            startTime: downtimeStartTime,
            endTime: pointTime,
            durationSeconds: 0n,
            description: 'Linha sem programação de ordem de produção',
          });

          intervalsToCreate.push({
            id: newIntervalId,
            downtimeId: newDowntimeId,
            workCenterId: wc.id,
            startTime: intervalStartTime,
            endTime: pointTime,
            durationSeconds: 0n,
          });
        } else {
          const shiftChanged = currentShift?.id !== currentShiftForDowntime?.id;
          const hourChanged =
            currentHour.getTime() !== currentHourForInterval?.getTime();

          if (shiftChanged) {
            if (downtimeStartTime && intervalStartTime) {
              const dtDur = BigInt(
                Math.max(
                  0,
                  Math.floor(
                    (pointTime.getTime() - downtimeStartTime.getTime()) / 1000,
                  ),
                ),
              );
              const idtDur = BigInt(
                Math.max(
                  0,
                  Math.floor(
                    (pointTime.getTime() - intervalStartTime.getTime()) / 1000,
                  ),
                ),
              );

              downtimesToUpdate.set(activeDowntimeId, {
                endTime: pointTime,
                durationSeconds: dtDur,
              });
              intervalsToUpdate.set(activeIntervalDowntimeId!, {
                endTime: pointTime,
                durationSeconds: idtDur,
              });
            }

            downtimeStartTime = pointTime;
            intervalStartTime = pointTime;
            currentShiftForDowntime = currentShift;
            currentHourForInterval = currentHour;

            const newDowntimeId = `dt_np_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
            const newIntervalId = `idt_np_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
            activeDowntimeId = newDowntimeId;
            activeIntervalDowntimeId = newIntervalId;

            downtimesToCreate.push({
              id: newDowntimeId,
              workCenterId: wc.id,
              orderId: null,
              shiftId: currentShift?.id || null,
              startTime: downtimeStartTime,
              endTime: pointTime,
              durationSeconds: 0n,
              description: 'Linha sem programação de ordem de produção',
            });

            intervalsToCreate.push({
              id: newIntervalId,
              downtimeId: newDowntimeId,
              workCenterId: wc.id,
              startTime: intervalStartTime,
              endTime: pointTime,
              durationSeconds: 0n,
            });
          } else if (hourChanged) {
            if (intervalStartTime) {
              const idtDur = BigInt(
                Math.max(
                  0,
                  Math.floor(
                    (pointTime.getTime() - intervalStartTime.getTime()) / 1000,
                  ),
                ),
              );
              intervalsToUpdate.set(activeIntervalDowntimeId!, {
                endTime: pointTime,
                durationSeconds: idtDur,
              });
            }

            intervalStartTime = pointTime;
            currentHourForInterval = currentHour;

            const newIntervalId = `idt_np_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
            activeIntervalDowntimeId = newIntervalId;

            intervalsToCreate.push({
              id: newIntervalId,
              downtimeId: activeDowntimeId,
              workCenterId: wc.id,
              startTime: intervalStartTime,
              endTime: pointTime,
              durationSeconds: 0n,
            });

            if (downtimeStartTime) {
              const dtDur = BigInt(
                Math.max(
                  0,
                  Math.floor(
                    (pointTime.getTime() - downtimeStartTime.getTime()) / 1000,
                  ),
                ),
              );
              downtimesToUpdate.set(activeDowntimeId, {
                endTime: pointTime,
                durationSeconds: dtDur,
              });
            }
          } else {
            if (downtimeStartTime && intervalStartTime) {
              const dtDur = BigInt(
                Math.max(
                  0,
                  Math.floor(
                    (pointTime.getTime() - downtimeStartTime.getTime()) / 1000,
                  ),
                ),
              );
              const idtDur = BigInt(
                Math.max(
                  0,
                  Math.floor(
                    (pointTime.getTime() - intervalStartTime.getTime()) / 1000,
                  ),
                ),
              );

              downtimesToUpdate.set(activeDowntimeId, {
                endTime: pointTime,
                durationSeconds: dtDur,
              });
              intervalsToUpdate.set(activeIntervalDowntimeId!, {
                endTime: pointTime,
                durationSeconds: idtDur,
              });
            }
          }
        }
      }

      // =======================================================================
      // CONTROLE ESTRITO DA PRODUCTION METRICS USANDO METRIC_STATE
      // =======================================================================
      const targetShiftId = currentShift?.id || null;
      const targetOrderId = activeOrder?.id || null;
      const targetFilterOrder = `${wc.id}_${targetShiftId || 'NO_SHIFT'}_${targetOrderId || 'NO_ORDER'}`;

      // Condição de Troca: hora virou, turno trocou ou OP mudou
      const isShiftChanged =
        currentActiveMetric && currentActiveMetric.shiftId !== targetShiftId;
      const isHourChanged =
        currentActiveMetric &&
        currentActiveMetric.hour.getTime() !== currentHour.getTime();
      const isOrderChanged =
        currentActiveMetric && currentActiveMetric.orderId !== targetOrderId;

      if (
        currentActiveMetric &&
        (isShiftChanged || isHourChanged || isOrderChanged)
      ) {
        // 1. FECHA a métrica anterior com os contadores e o timestamp exato do ponto da virada
        currentActiveMetric.finalTime = pointTime;
        currentActiveMetric.finalProdCount = currentProdCounter;
        currentActiveMetric.finalRejCount = currentRejectCounter;
        currentActiveMetric.producedQuantity =
          currentActiveMetric.finalProdCount >=
          currentActiveMetric.initialProdCount
            ? currentActiveMetric.finalProdCount -
              currentActiveMetric.initialProdCount
            : currentActiveMetric.finalProdCount;
        currentActiveMetric.autoRejectedQuantity =
          currentActiveMetric.finalRejCount >=
          currentActiveMetric.initialRejCount
            ? currentActiveMetric.finalRejCount -
              currentActiveMetric.initialRejCount
            : currentActiveMetric.finalRejCount;
        currentActiveMetric.totalRejectedQuantity =
          currentActiveMetric.autoRejectedQuantity +
          BigInt(currentActiveMetric.manualRejectedQuantity || 0);

        currentActiveMetric.isClosed = true;
        closedMetricsToPersist.push({ ...currentActiveMetric });

        // 2. ABRE a nova métrica com contadores idênticos aos finais da anterior (producedQuantity = 0)
        currentActiveMetric = {
          id: null, // Será gerado na transação
          hour: currentHour,
          initialTime: pointTime,
          finalTime: pointTime,
          shiftId: targetShiftId,
          orderId: targetOrderId,
          filterOrder: targetFilterOrder,
          initialProdCount: currentProdCounter,
          finalProdCount: currentProdCounter,
          initialRejCount: currentRejectCounter,
          finalRejCount: currentRejectCounter,
          producedQuantity: 0n,
          autoRejectedQuantity: 0n,
          manualRejectedQuantity: 0,
          totalRejectedQuantity: 0n,
          isClosed: false,
        };
      } else if (!currentActiveMetric) {
        // Primeira métrica do posto
        currentActiveMetric = {
          id: null,
          hour: currentHour,
          initialTime: pointTime,
          finalTime: pointTime,
          shiftId: targetShiftId,
          orderId: targetOrderId,
          filterOrder: targetFilterOrder,
          initialProdCount: currentProdCounter,
          finalProdCount: currentProdCounter,
          initialRejCount: currentRejectCounter,
          finalRejCount: currentRejectCounter,
          producedQuantity: 0n,
          autoRejectedQuantity: 0n,
          manualRejectedQuantity: 0,
          totalRejectedQuantity: 0n,
          isClosed: false,
        };
      } else {
        // Ponto dentro da mesma janela ativa: apenas atualiza o finalTime e contadores finais
        currentActiveMetric.finalTime = pointTime;
        currentActiveMetric.finalProdCount = currentProdCounter;
        currentActiveMetric.finalRejCount = currentRejectCounter;
        currentActiveMetric.producedQuantity =
          currentActiveMetric.finalProdCount >=
          currentActiveMetric.initialProdCount
            ? currentActiveMetric.finalProdCount -
              currentActiveMetric.initialProdCount
            : currentActiveMetric.finalProdCount;
        currentActiveMetric.autoRejectedQuantity =
          currentActiveMetric.finalRejCount >=
          currentActiveMetric.initialRejCount
            ? currentActiveMetric.finalRejCount -
              currentActiveMetric.initialRejCount
            : currentActiveMetric.finalRejCount;
        currentActiveMetric.totalRejectedQuantity =
          currentActiveMetric.autoRejectedQuantity +
          BigInt(currentActiveMetric.manualRejectedQuantity || 0);
      }
    }

    // =========================================================================
    // PERSISTÊNCIA ATÔMICA NO BANCO DO TENANT
    // =========================================================================
    await tenantDb.$transaction(async (tx: any) => {
      // 1. Salva e congela definitivamente as métricas que foram encerradas durante este ciclo
      for (const cm of closedMetricsToPersist) {
        if (cm.id) {
          await tx.productionMetrics.update({
            where: { id: cm.id },
            data: {
              finalTime: cm.finalTime,
              finalProductionCount: cm.finalProdCount,
              producedQuantity: cm.producedQuantity,
              finalRejectCount: cm.finalRejCount,
              autoRejectedQuantity: cm.autoRejectedQuantity,
              totalRejectedQuantity: cm.totalRejectedQuantity,
            },
          });
        } else {
          // Caso tenha sido aberta e fechada dentro do mesmo lote
          const created = await tx.productionMetrics.create({
            data: {
              hour: cm.hour,
              initialTime: cm.initialTime,
              finalTime: cm.finalTime,
              workCenterId: wc.id,
              shiftId: cm.shiftId,
              orderId: cm.orderId,
              filterOrder: cm.filterOrder,
              initialProductionCount: cm.initialProdCount,
              finalProductionCount: cm.finalProdCount,
              producedQuantity: cm.producedQuantity,
              initialRejectCount: cm.initialRejCount,
              finalRejectCount: cm.finalRejCount,
              autoRejectedQuantity: cm.autoRejectedQuantity,
              manualRejectedQuantity: cm.manualRejectedQuantity,
              totalRejectedQuantity: cm.totalRejectedQuantity,
            },
          });
          cm.id = created.id;
        }
      }

      // 2. Salva a métrica que permanece ativa ao final do lote
      let finalActiveMetricId: string | null = null;
      if (currentActiveMetric) {
        if (currentActiveMetric.id) {
          await tx.productionMetrics.update({
            where: { id: currentActiveMetric.id },
            data: {
              finalTime: currentActiveMetric.finalTime,
              finalProductionCount: currentActiveMetric.finalProdCount,
              producedQuantity: currentActiveMetric.producedQuantity,
              finalRejectCount: currentActiveMetric.finalRejCount,
              autoRejectedQuantity: currentActiveMetric.autoRejectedQuantity,
              totalRejectedQuantity: currentActiveMetric.totalRejectedQuantity,
            },
          });
          finalActiveMetricId = currentActiveMetric.id;
        } else {
          // Busca se já existe no banco por unicidade para não duplicar
          const existing = await tx.productionMetrics.findFirst({
            where: {
              hour: currentActiveMetric.hour,
              workCenterId: wc.id,
              shiftId: currentActiveMetric.shiftId,
              filterOrder: currentActiveMetric.filterOrder,
            },
          });

          if (existing) {
            await tx.productionMetrics.update({
              where: { id: existing.id },
              data: {
                finalTime: currentActiveMetric.finalTime,
                finalProductionCount: currentActiveMetric.finalProdCount,
                producedQuantity: currentActiveMetric.producedQuantity,
                finalRejectCount: currentActiveMetric.finalRejCount,
                autoRejectedQuantity: currentActiveMetric.autoRejectedQuantity,
                totalRejectedQuantity:
                  currentActiveMetric.totalRejectedQuantity,
              },
            });
            finalActiveMetricId = existing.id;
          } else {
            const created = await tx.productionMetrics.create({
              data: {
                hour: currentActiveMetric.hour,
                initialTime: currentActiveMetric.initialTime,
                finalTime: currentActiveMetric.finalTime,
                workCenterId: wc.id,
                shiftId: currentActiveMetric.shiftId,
                orderId: currentActiveMetric.orderId,
                filterOrder: currentActiveMetric.filterOrder,
                initialProductionCount: currentActiveMetric.initialProdCount,
                finalProductionCount: currentActiveMetric.finalProdCount,
                producedQuantity: currentActiveMetric.producedQuantity,
                initialRejectCount: currentActiveMetric.initialRejCount,
                finalRejectCount: currentActiveMetric.finalRejCount,
                autoRejectedQuantity: currentActiveMetric.autoRejectedQuantity,
                manualRejectedQuantity:
                  currentActiveMetric.manualRejectedQuantity,
                totalRejectedQuantity:
                  currentActiveMetric.totalRejectedQuantity,
              },
            });
            finalActiveMetricId = created.id;
          }
        }
      }

      // 3. ATUALIZA O METRIC_STATE (Ponteiro da métrica ativa para o próximo ciclo ou reinício)
      if (finalActiveMetricId) {
        await tx.metricState.upsert({
          where: { workCenterId: wc.id },
          update: { lastMetricId: finalActiveMetricId },
          create: { workCenterId: wc.id, lastMetricId: finalActiveMetricId },
        });
      }

      // 4. Criação de Downtimes
      for (const dt of downtimesToCreate) {
        await tx.downtime.create({
          data: {
            id: dt.id,
            workCenterId: dt.workCenterId,
            orderId: dt.orderId,
            shiftId: dt.shiftId,
            startTime: dt.startTime,
            endTime: dt.endTime,
            durationSeconds: dt.durationSeconds,
            description: dt.description || null,
          },
        });
      }

      // 5. Atualização de Downtimes em Aberto
      for (const [id, data] of downtimesToUpdate.entries()) {
        await tx.downtime.update({
          where: { id },
          data: {
            endTime: data.endTime,
            durationSeconds: data.durationSeconds,
          },
        });
      }

      // 6. Criação de IntervalDowntimes
      for (const idt of intervalsToCreate) {
        await tx.intervalDowntime.create({
          data: {
            id: idt.id,
            downtimeId: idt.downtimeId,
            workCenterId: idt.workCenterId,
            startTime: idt.startTime,
            endTime: idt.endTime,
            durationSeconds: idt.durationSeconds,
          },
        });
      }

      // 7. Atualização de IntervalDowntimes em Aberto
      for (const [id, data] of intervalsToUpdate.entries()) {
        await tx.intervalDowntime.update({
          where: { id },
          data: {
            endTime: data.endTime,
            durationSeconds: data.durationSeconds,
          },
        });
      }

      // 8. Atualização de Status do Posto
      await tx.workCenter.update({
        where: { id: wc.id },
        data: { status: workCenterStatus },
      });

      // 9. Atualização da Ordem Ativa (se houver)
      if (activeOrder) {
        await tx.order.update({
          where: { id: activeOrder.id },
          data: {
            producedQuantity: activeOrder.producedQuantity,
            autoRejectedQuantity: activeOrder.autoRejectedQuantity,
            initialProductionCount: activeOrder.initialProductionCount,
            finalProductionCount: activeOrder.finalProductionCount,
            initialRejectCount: activeOrder.initialRejectCount,
            finalRejectCount: activeOrder.finalRejectCount,
            totalRejectedQuantity: activeOrder.autoRejectedQuantity,
            counterStarted: activeOrder.counterStarted,
          },
        });
      }

      // 10. Atualiza CounterState
      await tx.counterState.upsert({
        where: { workCenterId: wc.id },
        update: { value: currentProdCounter, timestamp: lastPulseTime },
        create: {
          workCenterId: wc.id,
          value: currentProdCounter,
          timestamp: lastPulseTime,
        },
      });

      // 11. Atualiza DowntimeState
      if (activeDowntimeId && activeIntervalDowntimeId && downtimeStartTime) {
        await tx.downtimeState.upsert({
          where: { workCenterId: wc.id },
          update: {
            downtimeId: activeDowntimeId,
            orderId: activeOrder ? activeOrder.id : null,
            intervalDowntimeId: activeIntervalDowntimeId,
            startTime: downtimeStartTime,
            productionMetricsId: finalActiveMetricId,
          },
          create: {
            workCenterId: wc.id,
            downtimeId: activeDowntimeId,
            orderId: activeOrder ? activeOrder.id : null,
            intervalDowntimeId: activeIntervalDowntimeId,
            startTime: downtimeStartTime,
            productionMetricsId: finalActiveMetricId,
          },
        });
      } else {
        await tx.downtimeState.deleteMany({
          where: { workCenterId: wc.id },
        });
      }

      // 12. Atualiza ProcessedState
      if (latestProcessedPointTimestampMs > 0) {
        await tx.processedState.upsert({
          where: { workCenterId: wc.id },
          update: { lastProcessed: new Date(latestProcessedPointTimestampMs) },
          create: {
            workCenterId: wc.id,
            lastProcessed: new Date(latestProcessedPointTimestampMs),
          },
        });
      }
    });

    logger.debug(
      `💾 [MES Engine] Linha "${wc.name}" sincronizada com corte estrito de turnos e MetricState.`,
    );
  }
}
