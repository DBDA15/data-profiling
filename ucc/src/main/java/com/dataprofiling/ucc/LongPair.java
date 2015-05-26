package com.dataprofiling.ucc;

/**
 * A pair of long values.
 *
 * @author Jakob Zwiener
 */
public class LongPair implements Comparable<LongPair> {

	private long first;
	private long second;

	public LongPair(long first, long second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (first ^ (first >>> 32));
		result = prime * result + (int) (second ^ (second >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		LongPair other = (LongPair) obj;
		if (first != other.first) {
			return false;
		}
		return second == other.second;
	}

	public long getFirst() {
		return this.first;
	}

	public void setFirst(long first) {
		this.first = first;
	}

	public long getSecond() {
		return this.second;
	}

	public void setSecond(long second) {
		this.second = second;
	}

	@Override
	public int compareTo(LongPair other) {
		if (other == null) {
			return 1;
		}

		if (other.first == this.first) {
			return (int) (this.second - other.second);
		}
		return (int) (this.first - other.first);
	}

}