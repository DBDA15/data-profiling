package com.dataprofiling.ucc;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class Parameters {
    /**
     * Create parameters from the given command line.
     */
    static Parameters parse(String... args) {
        try {
            Parameters parameters = new Parameters();
            new JCommander(parameters, args);
            return parameters;
        } catch (final ParameterException e) {
            System.err.println(e.getMessage());
            StringBuilder sb = new StringBuilder();
            new JCommander(new Parameters()).usage(sb);
            for (String line : sb.toString().split("\n")) {
                System.out.println(line);
            }
            System.exit(1);
            return null;
        }
    }
    

    @Parameter(names = "--input", description = "input CSV file", required = true)
    public String inputFile = "";
    
    @Parameter(names = "--delimiter", description = "delimiter for csv file", required = false)
    public String delimiter = ",";
    
    @Parameter(names = "--levelsToCheck", description = "levels after the program should terminate", required = false)
    public int levelsToCheck = Integer.MAX_VALUE;
}
