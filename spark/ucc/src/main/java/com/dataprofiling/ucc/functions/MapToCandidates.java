package com.dataprofiling.ucc.functions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class MapToCandidates implements FlatMapFunction<Tuple2<Long, Iterable<long[]>>, Long> {
        private static final long serialVersionUID = 2996101042290089153L;
        private Set<Long> minUCC;

        public MapToCandidates(Set<Long> localMinUcc) {
            this.minUCC = localMinUcc;
        }

        @Override
        public Iterable<Long> call(Tuple2<Long, Iterable<long[]>> t) throws Exception {
            List<Long> newCandidates = new ArrayList<Long>();

            List<Long> tupleList = new ArrayList<Long>();
            Iterator<long[]> it = t._2.iterator();

            while (it.hasNext()) {
                tupleList.add(it.next()[0]);
            }

            for (int i = 0; i < tupleList.size() - 1; i++) {
                for (int j = i + 1; j < tupleList.size(); j++) {
                    Long intersection = combine(tupleList.get(i), tupleList.get(j));
                    if (intersection != null) {
                        newCandidates.add(intersection);
                    }
                }
            }
            return newCandidates;
        }

        private Long combine(Long outer, Long inner) {
            Long newColumCombination = outer | inner;
            // do subset check
            if (!isSubsetUnique(newColumCombination, this.minUCC)) {
                return newColumCombination;
            }
            return null;
        }
        

        /**
         * Check if any of the minimal uniques is completely contained in the column
         * combination. If so, a subset is already unique.
         * 
         * TODO: inefficient due to comparisons to all minimal uniques
         * 
         * @param columnCombination
         * @param minUCC
         * @return true if columnCombination contains unique subset, false otherwise
         */
        private static boolean isSubsetUnique(Long columnCombinationL, Set<Long> minUCC) {
            BitSet columnCombination = Bits.convert(columnCombinationL);
            for (Long bSetL : minUCC) {
                BitSet bSet = Bits.convert(bSetL);
                // System.out.println("minUcc (in isSubsetUnique)" + minUCC);
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

}
