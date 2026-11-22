// mes-engine\src\paperless-scheduler.ts
import { getTenantClient } from './db.js';
import {
  CachedTenant,
  getTenantShifts,
  resolveShiftForTimestamp,
} from './cache.js';
import { logger } from './logger.js';

export async function processTenantScheduledTasks(
  tenant: CachedTenant,
): Promise<void> {
  const hasPaperless = tenant.activeModules.includes('PAPERLESS');
  const hasCep = tenant.activeModules.includes('QUALITY_CEP');
  if (!hasPaperless && !hasCep) return;

  const tenantDb = await getTenantClient<any>(tenant.subdomain);
  const now = new Date();
  const nowMs = now.getTime();

  // 1. Atualiza tarefas vencidas de PENDING para OVERDUE
  await tenantDb.scheduledTask.updateMany({
    where: {
      status: 'PENDING',
      dueAt: { lt: now },
    },
    data: { status: 'OVERDUE' },
  });

  // 2. Busca regras contínuas ativas (Tempo, Quantidade e Turno)
  const rules = await tenantDb.scheduleRule.findMany({
    where: {
      isActive: true,
      deletedAt: null,
      triggerType: {
        in: ['RECURRING_TIME', 'QUANTITY_INTERVAL', 'SHIFT_CHANGE'],
      },
    },
  });

  if (rules.length === 0) return;

  // 3. Busca todas as Ordens de Produção em andamento (RUNNING) no tenant
  const runningOrders = await tenantDb.order.findMany({
    where: { status: 'RUNNING', deletedAt: null },
    include: {
      workCenter: {
        include: {
          machines: {
            where: { deletedAt: null },
            select: { id: true },
          },
        },
      },
    },
  });

  if (runningOrders.length === 0) return;

  const shifts = await getTenantShifts(tenant.subdomain);
  const currentShift = resolveShiftForTimestamp(shifts, nowMs);

  for (const order of runningOrders) {
    if (!order.workCenter || !order.startDate) continue;
    const machineIds = new Set<string>(
      order.workCenter.machines.map((m: { id: string }) => m.id),
    );

    for (const rule of rules) {
      // Verifica se a regra se aplica dinamicamente a esta OP / Centro / Máquina / Produto
      const matchesDept =
        rule.departmentId &&
        rule.departmentId === order.workCenter.departmentId;
      const matchesWc =
        rule.workCenterId && rule.workCenterId === order.workCenterId;
      const matchesMachine = rule.machineId && machineIds.has(rule.machineId);
      const matchesProduct =
        rule.productId && rule.productId === order.productId;

      if (!matchesDept && !matchesWc && !matchesMachine && !matchesProduct) {
        continue;
      }

      const targetMachineId = matchesMachine ? rule.machineId : null;
      const toleranceMs = Math.max(1, rule.toleranceMinutes || 15) * 60 * 1000;

      // GATILHO A: RECORRENTE POR TEMPO (ex: a cada 60 min desde o início da OP)
      if (rule.triggerType === 'RECURRING_TIME' && rule.intervalMinutes) {
        const intervalMs = rule.intervalMinutes * 60 * 1000;
        const elapsedMs = nowMs - new Date(order.startDate).getTime();
        const cycleIndex = Math.floor(elapsedMs / intervalMs);

        if (cycleIndex >= 1) {
          const refKey = `TIME_${order.id}_C${cycleIndex}_${targetMachineId || 'WC'}`;
          const dueAt = new Date(nowMs + toleranceMs);
          const hideFromOperatorAt = new Date(
            dueAt.getTime() + 24 * 60 * 60 * 1000,
          );

          await tenantDb.scheduledTask.upsert({
            where: {
              scheduleRuleId_orderId_workCenterId_triggerReferenceKey: {
                scheduleRuleId: rule.id,
                orderId: order.id,
                workCenterId: order.workCenterId,
                triggerReferenceKey: refKey,
              },
            },
            update: {},
            create: {
              scheduleRuleId: rule.id,
              status: 'PENDING',
              triggerReferenceKey: refKey,
              workCenterId: order.workCenterId,
              orderId: order.id,
              machineId: targetMachineId,
              scheduledFor: now,
              dueAt,
              hideFromOperatorAt,
            },
          });
        }
      }

      // GATILHO B: POR QUANTIDADE PRODUZIDA OU META ATINGIDA
      if (rule.triggerType === 'QUANTITY_INTERVAL') {
        const produced = Number(order.producedQuantity || 0n);

        if (rule.quantityInterval && rule.quantityInterval > 0) {
          const batchIndex = Math.floor(produced / rule.quantityInterval);
          if (batchIndex >= 1) {
            const refKey = `QTY_${order.id}_B${batchIndex}_${targetMachineId || 'WC'}`;
            const dueAt = new Date(nowMs + toleranceMs);
            const hideFromOperatorAt = new Date(
              dueAt.getTime() + 24 * 60 * 60 * 1000,
            );

            await tenantDb.scheduledTask.upsert({
              where: {
                scheduleRuleId_orderId_workCenterId_triggerReferenceKey: {
                  scheduleRuleId: rule.id,
                  orderId: order.id,
                  workCenterId: order.workCenterId,
                  triggerReferenceKey: refKey,
                },
              },
              update: {},
              create: {
                scheduleRuleId: rule.id,
                status: 'PENDING',
                triggerReferenceKey: refKey,
                workCenterId: order.workCenterId,
                orderId: order.id,
                machineId: targetMachineId,
                scheduledFor: now,
                dueAt,
                hideFromOperatorAt,
              },
            });
          }
        }

        if (
          rule.triggerAtOrderTarget &&
          order.targetQuantity > 0 &&
          produced >= order.targetQuantity
        ) {
          const refKey = `TARGET_${order.id}_${targetMachineId || 'WC'}`;
          const dueAt = new Date(nowMs + toleranceMs);
          const hideFromOperatorAt = new Date(
            dueAt.getTime() + 24 * 60 * 60 * 1000,
          );

          await tenantDb.scheduledTask.upsert({
            where: {
              scheduleRuleId_orderId_workCenterId_triggerReferenceKey: {
                scheduleRuleId: rule.id,
                orderId: order.id,
                workCenterId: order.workCenterId,
                triggerReferenceKey: refKey,
              },
            },
            update: {},
            create: {
              scheduleRuleId: rule.id,
              status: 'PENDING',
              triggerReferenceKey: refKey,
              workCenterId: order.workCenterId,
              orderId: order.id,
              machineId: targetMachineId,
              scheduledFor: now,
              dueAt,
              hideFromOperatorAt,
            },
          });
        }
      }

      // GATILHO C: VIRADA DE TURNO
      if (rule.triggerType === 'SHIFT_CHANGE' && currentShift) {
        const dateTag = now.toISOString().slice(0, 10);
        const refKey = `SHIFT_${order.id}_${dateTag}_${currentShift.id}_${targetMachineId || 'WC'}`;
        const dueAt = new Date(nowMs + toleranceMs);
        const hideFromOperatorAt = new Date(
          dueAt.getTime() + 24 * 60 * 60 * 1000,
        );

        await tenantDb.scheduledTask.upsert({
          where: {
            scheduleRuleId_orderId_workCenterId_triggerReferenceKey: {
              scheduleRuleId: rule.id,
              orderId: order.id,
              workCenterId: order.workCenterId,
              triggerReferenceKey: refKey,
            },
          },
          update: {},
          create: {
            scheduleRuleId: rule.id,
            status: 'PENDING',
            triggerReferenceKey: refKey,
            workCenterId: order.workCenterId,
            orderId: order.id,
            machineId: targetMachineId,
            scheduledFor: now,
            dueAt,
            hideFromOperatorAt,
          },
        });
      }
    }
  }

  logger.debug(
    `📋 [Paperless Scheduler] Ciclo de agendamentos concluído para ${tenant.subdomain}.`,
  );
}
