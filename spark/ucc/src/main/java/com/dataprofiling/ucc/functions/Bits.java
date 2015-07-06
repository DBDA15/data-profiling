package com.dataprofiling.ucc.functions;

import java.util.BitSet;

/*
 * http://stackoverflow.com/questions/2473597/bitset-to-and-from-integer-long
 */
public class Bits {

    public static BitSet convert(long value) {
        BitSet bits = new BitSet();
        int index = 0;
        while (value != 0L) {
            if (value % 2L != 0) {
                bits.set(index);
            }
            ++index;
            value = value >>> 1;
        }
        return bits;
    }

    public static long convert(BitSet bits) {
        long value = 0L;
        for (int i = 0; i < bits.length(); ++i) {
            value += bits.get(i) ? (1L << i) : 0L;
        }
        return value;
    }

    public static boolean getBitAt(long value, long i) {
        if ((value & (1L << i)) != 0) {
            // The bit was set
            return true;
        }
        return false;
    }
    
    public static Long createLong(int position, int length) {
        BitSet bs = new BitSet(length);
        bs.set(position);
        return Bits.convert(bs);
    }
}
