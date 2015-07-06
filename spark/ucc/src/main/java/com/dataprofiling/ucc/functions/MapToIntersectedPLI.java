package com.dataprofiling.ucc.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MapToIntersectedPLI implements PairFunction<Long, Long, Boolean> {
    private static final long serialVersionUID = 2149149028323431945L;
    private HashMap<Long, long[]> pliHashMap = new HashMap<Long, long[]>();


    public MapToIntersectedPLI(HashMap<Long, long[]> pliHashMap) {
        this.pliHashMap = pliHashMap;
    }

    @Override
    public Tuple2<Long, Boolean> call(Long in) throws Exception {
        List<Long> plis = determineSingleColumns(in);
        
        // 1. build maps for all other PLIs
        List<HashMap<Long, Long>> otherPLIs = new  ArrayList<HashMap<Long, Long>>();
        for (int j = 1; j < plis.size(); j++) {
            otherPLIs.add(buildHashMap(pliHashMap.get(plis.get(j))));
        }
        
        // 2. take first PLI and go through it
        long[] onePLI = pliHashMap.get(plis.get(0));
        for (int i = 0; i < onePLI.length;) {
            int sizeIndex = i;
            long sizeOfSameValue = onePLI[i];
            i++;

            List<List<Long>> values = new ArrayList<List<Long>>(otherPLIs.size());
            for (int j = 0; j < otherPLIs.size(); j++) {
                values.add(new ArrayList<Long>());
            }
            
            boolean[] areNonUnique = new boolean[otherPLIs.size()];
            for (; i <= sizeIndex + sizeOfSameValue; i++) {
                long rowIndex = onePLI[i];

                List<Long> addValues = new ArrayList<Long>();
                for (int j = 0; j < otherPLIs.size(); j++) {
                    HashMap<Long, Long> otherPLI = otherPLIs.get(j);
                    // find rowIndex in each other PLI
                    if (otherPLI.containsKey(rowIndex)) {
                        addValues.add(otherPLI.get(rowIndex));
                    } // end if
                } // end for
                if (addValues.size() == otherPLIs.size()) {
                    for (int j = 0; j < addValues.size(); j++) {
                        if (values.get(j).contains(otherPLIs.get(j).get(rowIndex))) {
                            areNonUnique[j] = true;
                        } else {
                            values.get(j).add(addValues.get(j));
                        }
                    }
                }
            }

            // if all plis list make it non unique, we can stop
            boolean result = areNonUnique[0];
            for (int j = 1; j < otherPLIs.size(); j++) {
                result = result && areNonUnique[j];
                if (!result)
                    break;
            }
            if (result) {
                return new Tuple2<Long, Boolean>(in, true);
            }
        }

        return new Tuple2<Long, Boolean>(in, false);
    }

    /**
     * 
     * @param in candidate
     */
    private List<Long> determineSingleColumns(Long in) {
        List<Long> plis = new ArrayList<Long>();
        long highestBit = (int) (Math.log(Long.highestOneBit(in)) / Math.log(2) + 1e-10);
        for (long i = 0; i < highestBit + 1; i++) {
            if (Bits.getBitAt(in, i)) {
                plis.add((long) Math.pow(2, (double) i));
            }
        }
        return plis;
    }
    

    private HashMap<Long, Long> buildHashMap(long[] pli) {
        HashMap<Long, Long> otherPli = new HashMap<Long, Long>();
        long distinctValues = 1;
        for (int i = 0; i < pli.length;) {
            long sizeOfSameValue = pli[i];
            i++;
            for (int j = 0; j < sizeOfSameValue; j++) {
                otherPli.put(pli[i], distinctValues);
                i++;
            }
            distinctValues++;
        }
        return otherPli;
    }
    
}
