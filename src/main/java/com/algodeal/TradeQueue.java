package com.algodeal;

import java.util.*;
import java.util.concurrent.locks.*;
import com.google.common.collect.*;

/**
 * Typical use is: A thread will send trades into a {@link TradeQueue} through
 * {@link #receive(Trade)} Lots of different threads iterate on the trades
 * received until the queue is stopped.
 */
public class TradeQueue implements Iterable<Trade> {
	static final Object NO_MORE_TRADE = new Object();

	final ReadWriteLock lock = new ReentrantReadWriteLock();
	final Lock write = lock.writeLock();
	final Lock read = lock.readLock();
	final Condition condition = write.newCondition();
	private List<Object> trades;

	public TradeQueue() {
		trades = createEmptyTradeList();
	}

	private static List<Object> createEmptyTradeList() {
		return Lists.newArrayList();
	}

	public void receive(Trade trade) {
		add(trade);
	}

	public void stop() {
		add(NO_MORE_TRADE);
	}

	public void clear() {
		try {
			write.lock();
			List<Object> newTrades = createEmptyTradeList();
			add(newTrades);
			trades = newTrades;
		} finally {
			write.unlock();
		}
	}

	private void add(Object value) {
		try {
			write.lock();
			trades.add(value);
			condition.signalAll();
		} finally {
			write.unlock();
		}
	}

	@Override
	public Iterator<Trade> iterator() {
		try {
			read.lock();
			return new TradeIterator(trades);
		} finally {
			read.unlock();
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
				read.lock();
				if (currentValueIndex >= tradeList.size()) {
					read.unlock();
					write.lock();
					while (currentValueIndex >= tradeList.size()) {
						condition.awaitUninterruptibly();
					}
					write.unlock();
					read.lock();
				}

				value = tradeList.get(currentValueIndex++);
			} finally {
				read.unlock();
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