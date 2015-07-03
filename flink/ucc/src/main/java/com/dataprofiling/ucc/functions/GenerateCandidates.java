package com.dataprofiling.ucc.functions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import com.dataprofiling.ucc.helper.Bits;

public class GenerateCandidates implements GroupReduceFunction<Tuple2<Long, Boolean>, Long> {
    private static final long serialVersionUID = 1L;

    @Override
    public void reduce(Iterable<Tuple2<Long, Boolean>> values, Collector<Long> out) throws Exception {
        // TODO Auto-generated method stub

        // DataSet<Long> candidates = inital.filter(new
        // FilterCandidates()).map(new RemoveBoolean()).flatMap(new
        // MapToPrefix())
        // .name("Candidate generation I: Map from candidate to pair: Prefix, Candidates").groupBy(0)
        // .reduce(new
        // ReduceToPair()).name("candidate generation II").flatMap(new
        // MapToCandidates())
        // .name("Candidate generation III: Map from (prefix, candidates) to candidates");
        ArrayList<Long> uccs = new ArrayList<Long>();
        ArrayList<Long> candidates = new ArrayList<Long>();
        Iterator<Tuple2<Long, Boolean>> it = values.iterator();
        while (it.hasNext()) {
            Tuple2<Long, Boolean> obj = (Tuple2<Long, Boolean>) it.next();
            if (obj.f1) {
                candidates.add(obj.f0);
            } else {
                uccs.add(obj.f0);
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
                        out.collect( tupleList[i] | tupleList[j]);
                    }
                }
            } 
        }
        
        
        
        // subset check
        
        System.out.println();

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
