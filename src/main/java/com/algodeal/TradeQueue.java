package com.algodeal;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

/**
 * Typical use is:
 * A thread will send trades into a {@link TradeQueue}
 * through {@link #receive(Trade)}
 * Lots of different threads iterate on the trades received until
 * the queue is stopped.
 */
public class TradeQueue implements Iterable<Trade> {
	protected static final Object NO_MORE_TRADE = new Object();

	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock write = lock.writeLock();
	private final Lock read = lock.readLock();
	private final Condition condition = write.newCondition();
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
			stop();
			trades = createEmptyTradeList();
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

	public Iterator<Trade> iterator() {
		try {
			read.lock();
			return new TradeIterator(trades);
		} finally {
			read.unlock();
		}
	}

	class TradeIterator extends AbstractIterator<Trade> {
		private final List<Object> trades;
		private int currentValueIndex = 0;

		TradeIterator(List<Object> trades) {
			this.trades = trades;
		}

		@Override
		protected Trade computeNext() {
			Object value;

			try {
				read.lock();
				if (currentValueIndex >= trades.size()) {
					read.unlock();
					write.lock();
					while (currentValueIndex >= trades.size()) {
						condition.awaitUninterruptibly();
					}
					write.unlock();
					read.lock();
				}

				value = trades.get(currentValueIndex++);
			} finally {
				read.unlock();
			}

			return value == NO_MORE_TRADE ? endOfData() : (Trade) value;
		}
	}
}