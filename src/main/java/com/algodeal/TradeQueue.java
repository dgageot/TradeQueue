package com.algodeal;

public interface TradeQueue extends Iterable<Trade> {
	void receive(Trade trade);

	void stop();

	void clear();
}