/*
 *
 *  Encom is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Encom is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser Public License
 *  along with Encom.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aionemu.gameserver.ai2.manager;

import com.aionemu.gameserver.ai2.AI2Logger;
import com.aionemu.gameserver.ai2.AIState;
import com.aionemu.gameserver.ai2.AbstractAI;
import com.aionemu.gameserver.ai2.NpcAI2;
import com.aionemu.gameserver.ai2.event.AIEventType;
import com.aionemu.gameserver.model.gameobjects.Creature;
import com.aionemu.gameserver.model.gameobjects.Npc;
import com.aionemu.gameserver.utils.MathUtil;
import com.aionemu.gameserver.utils.ThreadPoolManager;
import com.aionemu.gameserver.world.geo.GeoService;

/**
 * @author ATracer Rework: Angry Catster
 */
public class SimpleAttackManager {

	/**
	 * @param npcAI
	 * @param delay
	 */
	public static void performAttack(NpcAI2 npcAI, int delay) {
		if (npcAI.isLogging()) {
			AI2Logger.info(npcAI, "performAttack");
		}
		if (npcAI.getOwner().getGameStats().isNextAttackScheduled()) {
			if (npcAI.isLogging()) {
				AI2Logger.info(npcAI, "Attack already sheduled");
			}
			scheduleCheckedAttackAction(npcAI, delay);
			return;
		}

		if (!isTargetInAttackRange(npcAI.getOwner())) {
			if (npcAI.isLogging()) {
				AI2Logger.info(npcAI, "Attack will not be scheduled because of range");
			}
			npcAI.onGeneralEvent(AIEventType.TARGET_TOOFAR);
			return;
		}
		npcAI.getOwner().getGameStats().setNextAttackTime(System.currentTimeMillis() + delay);
		if (delay > 0) {
			ThreadPoolManager.getInstance().schedule(new SimpleAttackAction(npcAI), delay);
		} else {
			attackAction(npcAI);
		}
	}

	/**
	 * @param npcAI
	 * @param delay
	 */
	private static void scheduleCheckedAttackAction(NpcAI2 npcAI, int delay) {
		if (npcAI.isLogging()) {
			AI2Logger.info(npcAI, "Scheduling checked attack " + delay);
		}
		ThreadPoolManager.getInstance().schedule(new SimpleCheckedAttackAction(npcAI), delay);
	}
	
    public static boolean isTargetInAttackRange(Npc npc) {
    if (npc == null) {
        return false;
    }
    
    if (npc.getTarget() == null) {
        return false;
    }
    
    if (!(npc.getTarget() instanceof Creature)) {
        return false;
    }
    
    Creature target = (Creature) npc.getTarget();
    
    if (!target.isSpawned() || target.getLifeStats() == null || target.getLifeStats().isAlreadyDead()) {
        return false;
    }
    
    if (npc.getAi2().isLogging()) {
        try {
            float distance = npc.getDistanceToTarget();
            AI2Logger.info((AbstractAI) npc.getAi2(), "isTargetInAttackRange: " + distance);
        } catch (Exception e) {
        }
    }
    
    try {
        if (npc.getTarget() == null) {
            return false;
        }
        if (!GeoService.getInstance().canSee(npc, npc.getTarget())) {
            return false;
        }
    } catch (NullPointerException e) {
        if (npc.getAi2().isLogging()) {
            AI2Logger.info((AbstractAI) npc.getAi2(), "GeoService.canSee NPE: " + e.getMessage());
        }
        return false;
    }
    
    try {
        return MathUtil.isInAttackRange(npc, target, npc.getGameStats().getAttackRange().getCurrent() / 1000f);
    } catch (NullPointerException e) {
        if (npc.getAi2().isLogging()) {
            AI2Logger.info((AbstractAI) npc.getAi2(), "MathUtil.isInAttackRange NPE: " + e.getMessage());
        }
        return false;
        }
    }

	/**
	 * @param npcAI
	 */
    protected static void attackAction(final NpcAI2 npcAI) {
    if (!npcAI.isInState(AIState.FIGHT)) {
        return;
    }
    if (npcAI.isLogging()) {
        AI2Logger.info(npcAI, "attackAction");
    }
    
    Npc npc = npcAI.getOwner();
    
    if (npc == null) {
        npcAI.onGeneralEvent(AIEventType.TARGET_GIVEUP);
        return;
    }
    
    Creature target = (Creature) npc.getTarget();
    
    if (target == null) {
        npcAI.onGeneralEvent(AIEventType.TARGET_GIVEUP);
        return;
    }
    
    if (target.getLifeStats() == null || target.getLifeStats().isAlreadyDead() || !target.isSpawned()) {
        npcAI.onGeneralEvent(AIEventType.TARGET_GIVEUP);
        return;
    }
    
    if (isTargetInAttackRange(npc)) {
        try {
            if (npc.canSee(target)) {
                npc.getController().attackTarget(target, 0);
                npcAI.onGeneralEvent(AIEventType.ATTACK_COMPLETE);
                return;
            }
        } catch (NullPointerException e) {
            if (npcAI.isLogging()) {
                AI2Logger.info(npcAI, "Error in canSee check: " + e.getMessage());
            }
        }
    }
    
    npcAI.onGeneralEvent(AIEventType.TARGET_TOOFAR);
    }

	private final static class SimpleAttackAction implements Runnable {
		private NpcAI2 npcAI;
		SimpleAttackAction(NpcAI2 npcAI) {
			this.npcAI = npcAI;
		}

		@Override
		public void run() {
			attackAction(npcAI);
			npcAI = null;
		}
	}

	private final static class SimpleCheckedAttackAction implements Runnable {
		private NpcAI2 npcAI;
		SimpleCheckedAttackAction(NpcAI2 npcAI) {
			this.npcAI = npcAI;
		}

		@Override
		public void run() {
			if (!npcAI.getOwner().getGameStats().isNextAttackScheduled()) {
				attackAction(npcAI);
			} else {
				if (npcAI.isLogging()) {
					AI2Logger.info(npcAI, "Scheduled checked attacked confirmed");
				}
			}
			npcAI = null;
		}
	}
}