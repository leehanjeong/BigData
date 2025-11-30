package trianglecount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * WedgeCountDriver - Calculate Wedge Count using Degree Formula
 * 
 * Mathematical Background:
 * - A wedge is a path of length 2 (v1 - center - v2)
 * - For a vertex with degree d, it can be the center of C(d,2) = d*(d-1)/2 wedges
 * - Total wedges = Σ C(degree_v, 2) for all vertices v
 * 
 * This avoids generating actual wedges (which causes disk overflow)
 * 
 * Algorithm: 2 steps
 * - Step 1: Normalize edges (same as baseline)
 * - Step 2: Calculate degree and sum C(d,2) for each vertex
 * 
 * Usage: hadoop jar triangle_count.jar trianglecount.WedgeCountDriver <input>
 */
public class WedgeCountDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WedgeCountDriver(), args);
    }
    
    public int run(String[] args) throws Exception {
        
        String inputPath = args[0];
        String normalizedPath = inputPath + ".normalized_wc";
        String outputPath = inputPath + ".wedge_count";
        
        System.out.println("=".repeat(60));
        System.out.println("Wedge Count Calculator (using Degree Formula)");
        System.out.println("=".repeat(60));
        System.out.println("Input: " + inputPath);
        System.out.println("Formula: Total Wedges = Σ C(degree_v, 2) = Σ d*(d-1)/2");
        System.out.println();
        
        runStep1_Normalize(inputPath, normalizedPath);
        runStep2_CountWedges(normalizedPath, outputPath);
        
        return 0;
    }
    
    /**
     * Step 1: Normalize edges (u < v) and remove duplicates
     */
    private void runStep1_Normalize(String inputPath, String outputPath) throws Exception {
        
        Job job = Job.getInstance(getConf());
        job.setJobName("WedgeCount-Step1-Normalize");
        job.setJarByClass(WedgeCountDriver.class);
        
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);
        
        job.setMapOutputKeyClass(IntPairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setPartitionerClass(IntPairIntPartitioner.class);
        
        job.setOutputKeyClass(IntPairWritable.class);
        job.setOutputValueClass(Text.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);  // Text output for easier processing
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        job.waitForCompletion(true);
        
        long edgeCount = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue();
        System.out.println("[Step 1] Normalized edges: " + edgeCount);
    }
    
    /**
     * Step 2: Calculate degree for each vertex and compute wedge count
     * 
     * Mapper: For each edge (u, v), emit (u, 1) and (v, 1)
     * Reducer: Sum degrees, calculate C(d,2) = d*(d-1)/2, sum all
     */
    private void runStep2_CountWedges(String inputPath, String outputPath) throws Exception {
        
        Job job = Job.getInstance(getConf());
        job.setJobName("WedgeCount-Step2-CalculateWedges");
        job.setJarByClass(WedgeCountDriver.class);
        
        job.setMapperClass(DegreeCountMapper.class);
        job.setReducerClass(WedgeCountReducer.class);
        job.setCombinerClass(DegreeSumCombiner.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Use single reducer to get global sum
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        job.waitForCompletion(true);
    }
    
    /**
     * Mapper: Parse normalized edges and emit (vertex, 1) for each endpoint
     */
    public static class DegreeCountMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
        
        private IntWritable outKey = new IntWritable();
        private LongWritable one = new LongWritable(1);
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            
            // Handle both "u v" and "(u, v)" formats
            String[] parts;
            if (line.startsWith("(")) {
                // Format: (u, v)
                line = line.replaceAll("[()]", "").trim();
                parts = line.split("[,\\s]+");
            } else {
                // Format: u v or u\tv
                parts = line.split("[\\s,]+");
            }
            
            if (parts.length >= 2) {
                try {
                    int u = Integer.parseInt(parts[0].trim());
                    int v = Integer.parseInt(parts[1].trim());
                    
                    // Emit (u, 1) and (v, 1)
                    outKey.set(u);
                    context.write(outKey, one);
                    
                    outKey.set(v);
                    context.write(outKey, one);
                } catch (NumberFormatException e) {
                    // Skip invalid lines
                }
            }
        }
    }
    
    /**
     * Combiner: Sum degrees locally to reduce network traffic
     */
    public static class DegreeSumCombiner extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
        
        private LongWritable result = new LongWritable();
        
        @Override
        protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
    /**
     * Reducer: Calculate wedge count using formula C(d,2) = d*(d-1)/2
     * 
     * Output:
     * - vertex_count: number of vertices
     * - max_degree: maximum degree
     * - total_wedges: Σ C(degree_v, 2)
     */
    public static class WedgeCountReducer extends Reducer<IntWritable, LongWritable, Text, LongWritable> {
        
        private long vertexCount = 0;
        private long maxDegree = 0;
        private long totalWedges = 0;
        private long totalDegree = 0;
        
        @Override
        protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            
            // Sum all 1s to get degree for this vertex
            long degree = 0;
            for (LongWritable val : values) {
                degree += val.get();
            }
            
            // Update statistics
            vertexCount++;
            totalDegree += degree;
            if (degree > maxDegree) {
                maxDegree = degree;
            }
            
            // Calculate wedges centered at this vertex: C(d, 2) = d * (d-1) / 2
            if (degree >= 2) {
                long wedgesAtVertex = degree * (degree - 1) / 2;
                totalWedges += wedgesAtVertex;
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output final statistics
            context.write(new Text("vertex_count"), new LongWritable(vertexCount));
            context.write(new Text("edge_count"), new LongWritable(totalDegree / 2));  // each edge counted twice
            context.write(new Text("max_degree"), new LongWritable(maxDegree));
            context.write(new Text("avg_degree"), new LongWritable(totalDegree / vertexCount));
            context.write(new Text("total_wedges"), new LongWritable(totalWedges));
            
            // Also print to console
            System.out.println();
            System.out.println("=".repeat(60));
            System.out.println("RESULTS - Wedge Count (Baseline Formula)");
            System.out.println("=".repeat(60));
            System.out.println("Vertex count:  " + vertexCount);
            System.out.println("Edge count:    " + (totalDegree / 2));
            System.out.println("Max degree:    " + maxDegree);
            System.out.println("Avg degree:    " + (totalDegree / vertexCount));
            System.out.println("Total wedges:  " + totalWedges);
            System.out.println("=".repeat(60));
            System.out.println();
            System.out.println("Formula used: Total Wedges = Σ C(degree_v, 2) = Σ d*(d-1)/2");
            System.out.println("This is the BASELINE wedge count (before any optimization)");
        }
    }
}
