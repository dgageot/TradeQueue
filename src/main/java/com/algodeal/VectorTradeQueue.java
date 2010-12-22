package com.algodeal;

import java.util.*;

/**
 * Implementation proposed by Jose Paumard. Hacked by David Gageot. Introduced
 * the FINAL_POISON_PILL to pass most tests.
 */
public class VectorTradeQueue implements TradeQueue {
	static final Trade POISON_PILL = new Trade(-1);
	static final Trade FINAL_POISON_PILL = new Trade(-1);

	private final Object key = new Object();
	final Vector<Vector<Trade>> tradeListList = new Vector<Vector<Trade>>();
	private Vector<Trade> tradeList;

	public VectorTradeQueue() {
		tradeList = new Vector<Trade>();
		tradeListList.add(tradeList);
	}

	@Override
	public void receive(Trade trade) {
		tradeList.add(trade);
	}

	@Override
	public void stop() {
		tradeList.add(FINAL_POISON_PILL);
	}

	@Override
	public void clear() {
		synchronized (key) {
			tradeList.add(POISON_PILL);
			tradeList = new Vector<Trade>();
			tradeListList.add(tradeList);
		}
	}

	@Override
	public Iterator<Trade> iterator() {
		synchronized (key) {
			return new TradeQueueIterator();
		}
	}

	class TradeQueueIterator implements Iterator<Trade> {
		private int tradeListListIndex = 0;
		private int tradeListIndex = 0;
		private Trade nextTrade;
		private Vector<Trade> currentTradeList = tradeListList.get(tradeListListIndex);

		@Override
		public boolean hasNext() {
			while (true) {
				try {
					nextTrade = currentTradeList.get(tradeListIndex);
					break;
				} catch (ArrayIndexOutOfBoundsException e) {
					// la file ne comporte pas d'element, on attend qu'il en arrive
					Thread.yield();
				}
			}
			if (nextTrade == FINAL_POISON_PILL) {
				return false;
			}
			// ici on a le dernier element ajoute a la liste
			// si c'est la poison pill, on doit passer a la liste suivante
			if (nextTrade == POISON_PILL) {
				// on doit passer a la liste suivante
				tradeListListIndex++;
				tradeListIndex = 0;
				while (true) {
					try {
						currentTradeList = tradeListList.get(tradeListListIndex);
						return hasNext();
					} catch (ArrayIndexOutOfBoundsException e) {
						// la file ne comporte pas d'autre liste, on attend 
						// qu'il en arrive une nouvelle
						Thread.yield();
					}
				}
			}

			// le dernier element n'est pas une poison pill, on
			// le garde pour le retour de next(), et on retourne true
			tradeListIndex++;
			return true;
		}

		@Override
		public Trade next() {
			return nextTrade;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
