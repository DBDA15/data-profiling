package com.dataprofiling.ucc;

import java.util.List;

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

    @Parameter(names = "--parallelism", description = "degree of parallelism for the job execution")
    public int parallelism = -1;

    @Parameter(names = "--jars", description = "set of jars that are relevant to the execution of SINDY")
    public List<String> jars = null;

    @Parameter(names = "--executor", description = "<host name>:<port> of the Flink cluster")
    public String executor = null;

    @Parameter(names = "--distinct-attribute-groups", description = "whether to use only distinct attribute groups")
    public boolean isUseDistinctAttributeGroups = false;
}
