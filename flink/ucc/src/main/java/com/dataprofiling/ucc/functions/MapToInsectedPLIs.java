package com.dataprofiling.ucc.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import com.dataprofiling.ucc.helper.Bits;

public class MapToInsectedPLIs extends RichMapFunction<Long, Tuple2<Long, Boolean>> {
    private static final long serialVersionUID = -5257555221859734116L;
    private HashMap<Long, long[]> pliHashMap = new HashMap<Long, long[]>();

    @Override
    public void open(Configuration params) {
        Collection<Tuple2<Long, long[]>> broadcastData = getRuntimeContext().getBroadcastVariable("singleColumnPLIs");
        // convert to hashMap
        for (Iterator<Tuple2<Long, long[]>> iterator = broadcastData.iterator(); iterator.hasNext();) {
            Tuple2<Long, long[]> tuple2 = (Tuple2<Long, long[]>) iterator.next();
            pliHashMap.put(tuple2.f0, tuple2.f1);
        }
    }
    
    @Override
    public Tuple2<Long, Boolean> map(Long in) throws Exception {
        List<Long> plis = new ArrayList<Long>();
        long highestBit = (int) (Math.log(Long.highestOneBit(in)) / Math.log(2) + 1e-10);
        for (long i = 0; i < highestBit + 1; i++) {
            if (Bits.getBitAt(in, i)) {
                plis.add((long) Math.pow(2, (double) i));
            }
        }

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
