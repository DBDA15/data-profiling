package com.dataprofiling.ucc.functions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class GenerateCandidates implements FlatMapFunction<Tuple2<Boolean, Iterable<Tuple2<Long, Boolean>>>, Long> {
    private static final long serialVersionUID = 4774484039697621395L;

    @Override
    public Iterable<Long> call(Tuple2<Boolean, Iterable<Tuple2<Long, Boolean>>> values) throws Exception {
        // ignore t._1 which was used to map all elements to one
        ArrayList<Long> out = new ArrayList<Long>();
        
        ArrayList<Long> uccs = new ArrayList<Long>();
        ArrayList<Long> candidates = new ArrayList<Long>();
        Iterator<Tuple2<Long, Boolean>> it = values._2.iterator();
        while (it.hasNext()) {
            Tuple2<Long, Boolean> obj = (Tuple2<Long, Boolean>) it.next();
            if (obj._2) {
                candidates.add(obj._1);
            } else {
                uccs.add(obj._1);
            }
        }

        // map to prefix and reduce
        HashMap<Long, long[]> prefixes = mapToPrefix(candidates);
        
        // map to candidate
        for (long[] tupleList : prefixes.values()) {
            for (int i = 0; i < tupleList.length - 1; i++) {
                for (int j = i + 1; j < tupleList.length; j++) {
                    Long newCandidate = tupleList[i] | tupleList[j];
                    if (!isSubsetUnique(newCandidate, uccs)){
                        out.add( tupleList[i] | tupleList[j]);
                    }
                }
            } 
        }
        
        return out;
        // Arrays.asList(out.asArray());
    }
    
    

    /**
     * Check if any of the minimal uniques is completely contained in the column
     * combination. If so, a subset is already unique.
     * 
     * TODO: inefficient due to comparisons to all minimal uniques and due to
     * convertion to BitSet
     * 
     * @param columnCombination
     * @param minUCC
     * @return true if columnCombination contains unique subset, false otherwise
     */
    private static boolean isSubsetUnique(Long columnCombinationL, Collection<Long> minUCC) {
        BitSet columnCombination = Bits.convert(columnCombinationL);
        for (Long bSetL : minUCC) {
            BitSet bSet = Bits.convert(bSetL);
            if (bSet.cardinality() > columnCombination.cardinality()) {
                continue;
            }
            BitSet copy = (BitSet) bSet.clone();
            copy.and(columnCombination);

            if (copy.cardinality() == bSet.cardinality()) {
                return true;
            }
        }
        return false;
    }


    private HashMap<Long, long[]> mapToPrefix(ArrayList<Long> candidates) {
        HashMap<Long, long[]> map = new HashMap<Long, long[]>();
        for (Long in : candidates) {
            // map to prefix
            BitSet inB = Bits.convert(in);
            BitSet bitSet = (BitSet) inB.clone();
            int highestBit = bitSet.previousSetBit(bitSet.length());
            bitSet.clear(highestBit);
            long[] inArray = { in };
            
            // groupBy(0).reduce to pair
            long newCandidate = Bits.convert(bitSet);
            if (map.containsKey(newCandidate)) {
                long[] combined = ArrayUtils.addAll(map.get(newCandidate), inArray);
                map.put(newCandidate, combined);
            } else {
                map.put(newCandidate, inArray);
            }
        }
        return map;
    }
}
