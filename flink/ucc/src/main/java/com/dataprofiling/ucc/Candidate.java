package com.dataprofiling.ucc;

import java.util.BitSet;

/**
 * in the first round candidate equals columnIndex
 * @author Jaqueline
 *
 */
public class Candidate extends BitSet implements Comparable<Object>{
    private static final long serialVersionUID = -2331780728569111267L;
    public Candidate(int length) {
        super(length);
    }
    
    public Candidate() {
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof BitSet)) {
            return -1;
        }
        BitSet bitSet = (BitSet) o;
        if (equals(bitSet)) {
            return 0;
        }
        return 1;
    }

}
