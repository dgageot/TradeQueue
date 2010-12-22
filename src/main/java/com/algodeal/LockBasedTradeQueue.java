package com.algodeal;

import java.util.*;
import java.util.concurrent.locks.*;
import com.google.common.collect.*;

/**
 * Typical use is: A thread will send trades into a {@link LockBasedTradeQueue}
 * through {@link #receive(Trade)} Lots of different threads iterate on the
 * trades received until the queue is stopped.
 */
public class LockBasedTradeQueue implements TradeQueue {
	static final Object NO_MORE_TRADE = new Object();

	final Lock lock = new ReentrantLock();
	final Condition condition = lock.newCondition();
	private List<Object> trades;

	public LockBasedTradeQueue() {
		trades = createEmptyTradeList();
	}

	private static List<Object> createEmptyTradeList() {
		return Lists.newArrayList();
	}

	@Override
	public void receive(Trade trade) {
		add(trade);
	}

	@Override
	public void stop() {
		add(NO_MORE_TRADE);
	}

	@Override
	public void clear() {
		try {
			lock.lock();
			List<Object> newTrades = createEmptyTradeList();
			add(newTrades);
			trades = newTrades;
		} finally {
			lock.unlock();
		}
	}

	private void add(Object value) {
		try {
			lock.lock();
			trades.add(value);
			condition.signalAll();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Iterator<Trade> iterator() {
		try {
			lock.lock();
			return new TradeIterator(trades);
		} finally {
			lock.unlock();
		}
	}

	class TradeIterator extends AbstractIterator<Trade> {
		private List<Object> tradeList;
		private int currentValueIndex = 0;

		TradeIterator(List<Object> trades) {
			tradeList = trades;
		}

		@Override
		@SuppressWarnings("unchecked")
		protected Trade computeNext() {
			Object value;

			try {
				lock.lock();
				while (currentValueIndex >= tradeList.size()) {
					condition.awaitUninterruptibly();
				}

				value = tradeList.get(currentValueIndex++);
			} finally {
				lock.unlock();
			}

			if (value == NO_MORE_TRADE) {
				return endOfData();
			}
			if (value instanceof Trade) {
				return (Trade) value;
			}

			tradeList = (List<Object>) value;
			currentValueIndex = 0;

			return computeNext();
		}
	}
}