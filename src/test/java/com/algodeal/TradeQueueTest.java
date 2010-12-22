package com.algodeal;

import static org.fest.assertions.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import com.google.common.collect.Lists;

public class TradeQueueTest {
	private final Trade mockTrade1 = mock(Trade.class);
	private final Trade mockTrade2 = mock(Trade.class);
	private final Trade mockTrade3 = mock(Trade.class);

	//final TradeQueue queue = new RWBasedTradeQueue(); // 20x10timesx2.000.000 in 14.1s 

	final TradeQueue queue = new VectorTradeQueue(); // 20x10timesx2.000.000 in 13.9s

	//final TradeQueue queue = new LockBasedTradeQueue(); // 20x10timesx2.000.000 in 17.4s

	@Test(timeout = 1000)
	public void canIterateOnAllTradesReceivedUntilStop() {
		queue.receive(mockTrade1);
		queue.receive(mockTrade2);
		queue.stop();

		Iterator<Trade> tradeIterator = queue.iterator();

		assertThat(tradeIterator).containsOnly(mockTrade1, mockTrade2);
	}

	@Test(timeout = 1000)
	public void canIterateMultipleTimes() {
		queue.receive(mockTrade1);
		queue.receive(mockTrade2);
		queue.stop();

		assertThat(queue.iterator()).containsOnly(mockTrade1, mockTrade2);
		assertThat(queue.iterator()).containsOnly(mockTrade1, mockTrade2);
		assertThat(queue.iterator()).containsOnly(mockTrade1, mockTrade2);
	}

	@Test(timeout = 1000)
	public void iteratorsCreatedAfterAClearOnlyGetTradesReceivedAfterClear() {
		queue.receive(mockTrade1);
		queue.receive(mockTrade2);
		queue.clear();

		queue.receive(mockTrade3);
		queue.stop();

		assertThat(queue.iterator()).containsOnly(mockTrade3);
	}

	@Test(timeout = 1000)
	public void iteratorsCreatedBeforeAClearGetAllTradesReceivedBeforeAndAfterClear() {
		Iterator<Trade> tradeIterator = queue.iterator();
		queue.receive(mockTrade1);
		queue.receive(mockTrade2);
		queue.clear();

		queue.receive(mockTrade3);
		queue.stop();

		assertThat(tradeIterator).containsOnly(mockTrade1, mockTrade2, mockTrade3);
	}

	@Test(timeout = 20 * 1000)
	public void hasToBeThreadSafe() throws InterruptedException {
		//final int NB = 2000000;
		final int NB = 200000; // Performance test
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
					for (int times = 0; times < 10; times++) {
						for (int i = 0; i < NB; i++) {
							queue.receive(new Trade(i));
						}
						queue.clear();
					}
					queue.stop();
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
