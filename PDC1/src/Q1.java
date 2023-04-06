import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    
        
    public static class Q1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	private Text journalYear = new Text();
        private final static IntWritable article = new IntWritable(1);
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	
            // Split the input line by tab delimiter and get the journal name and year
        
            String[] fields = value.toString().split(",");
        	if (fields.length != 4) {
                // Log an error message and skip the invalid input
                System.err.println("Invalid input line: " + value);
                return;
        	}
            String journal = fields[1];
            String year = fields[0];
            
            // Create the key-value pair and write it to the context
            journalYear.set(journal + "," + year);
            context.write(journalYear, article);
        }
    }
    public static class Q1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // counting the values that match our given key
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            
         
            // Write the output key-value pair to the context
            count=count-1;			//subtracting the extra count
            result.set(count);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
            System.err.println("Usage: JournalYearCounter <input path> <output path>");
            System.exit(-1);
        }
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Journal Year Counter");
            job.setJarByClass(Q1.class);
            job.setMapperClass(Q1Mapper.class);
            job.setReducerClass(Q1Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    
}