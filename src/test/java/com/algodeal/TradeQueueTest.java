package com.algodeal;

import static org.fest.assertions.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.*;
import org.junit.rules.Timeout;
import com.google.common.collect.Lists;

public class TradeQueueTest {
	private final Trade mockTrade1 = mock(Trade.class);
	private final Trade mockTrade2 = mock(Trade.class);
	private final Trade mockTrade3 = mock(Trade.class);
	private final Trade mockTrade4 = mock(Trade.class);

	@Rule
	public Timeout timeout = new Timeout(5000);

	final TradeQueue queue = new TradeQueue();

	@Test
	public void canIterateTrades() {
		queue.receive(mockTrade1);
		queue.receive(mockTrade2);
		queue.stop();

		Iterator<Trade> tradeIterator = queue.iterator();

		assertThat(tradeIterator).containsOnly(mockTrade1, mockTrade2);
	}

	@Test
	public void canIterateMultipleTimes() {
		queue.receive(mockTrade1);
		queue.receive(mockTrade2);
		queue.stop();

		assertThat(queue.iterator()).containsOnly(mockTrade1, mockTrade2);
		assertThat(queue.iterator()).containsOnly(mockTrade1, mockTrade2);
		assertThat(queue.iterator()).containsOnly(mockTrade1, mockTrade2);
	}

	@Test
	public void canClear() {
		queue.receive(mockTrade1);
		queue.receive(mockTrade2);
		queue.clear();
		queue.stop();

		assertThat(queue.iterator()).isEmpty();
	}

	@Test
	public void canClearWhileIterating() {
		Iterator<Trade> tradeIterator = queue.iterator();
		queue.receive(mockTrade1);
		queue.receive(mockTrade2);
		queue.clear();
		queue.stop();

		assertThat(tradeIterator).containsOnly(mockTrade1, mockTrade2);
	}

	@Test
	public void canClearAndStillIterateOnAllTrades() {
		Iterator<Trade> tradeIterator = queue.iterator();
		queue.receive(mockTrade1);
		queue.receive(mockTrade2);
		queue.clear();
		queue.receive(mockTrade3);
		queue.receive(mockTrade4);
		queue.stop();

		assertThat(tradeIterator).containsOnly(mockTrade1, mockTrade2, mockTrade3, mockTrade4);
	}

	@Test
	public void hasToBeThreadSafe() throws InterruptedException {
		final int NB = 200000;
		final AtomicInteger errorCount = new AtomicInteger(0);

		List<Thread> threads = Lists.newArrayList();
		for (int t = 0; t < 20; t++) {
			threads.add(new Thread() {
				@Override
				public void run() {
					try {
						Iterator<Trade> iterator = queue.iterator();
						for (int i = 0; i < NB; i++) {
							if ((!iterator.hasNext()) || (iterator.next().getOrder() != i)) {
								errorCount.incrementAndGet();
								break;
							}
						}
						queue.clear();
					} catch (Throwable e) {
						errorCount.incrementAndGet();
					}
				}
			});
		}
		threads.add(new Thread() {
			@Override
			public void run() {
				try {
					for (int i = 0; i < NB; i++) {
						queue.receive(new Trade(i));
					}
					queue.clear();
					for (int i = 0; i < NB; i++) {
						queue.receive(new Trade(i));
					}
				} catch (Throwable e) {
					errorCount.incrementAndGet();
				}
			}
		});

		for (Thread thread : threads) {
			thread.start();
		}
		for (Thread thread : threads) {
			thread.join();
		}

		assertThat(errorCount.get()).isZero();
	}
}
