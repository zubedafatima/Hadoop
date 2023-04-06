import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("unused")
public class Q2 extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	public static class Q2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	
        
	private Text pair = new Text();
    private final static IntWritable article = new IntWritable(1);
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	//split first on comma and we'll get the comma separated but as authors are separated by: so second split on that
    	String[] line = value.toString().split(",");
    	//then taking the right side of that split
    	if (line.length != 4) {
            // Log an error message and skip the invalid input
            System.err.println("Invalid input line: " + value);
            return;
    	}
    	
    	//storing the list of authors as 
    	String[] authors = line[3].split(":");

    	//traversing the list of authors in that line 
        for (int i = 0; i < authors.length; i++) {
          for (int j = i + 1; j < authors.length; j++) {
            // Creating pairs
            pair.set(authors[i] + "," + authors[j]);
            
            //setting those pairs with each other as key and then sending with the value
            context.write(pair, article);
          }
        
        }
    }
    
	}
	
	public static class Q2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // counting the values that match our given key
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            
         
            // Write the output key-value pair to the context
            count=count-1;					//subtracting that extra count it gives due to the initialization	
            result.set(count);
            context.write(key, result);
        }
    }

	 public static void main(String[] args) throws Exception {
		    if (args.length != 2) {
		      System.err.println("Usage: CoAuthorshipGraph <input path> <output path>");
		      System.exit(-1);
		    }
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Co-Authorship Graph");
		    job.setJarByClass(Q2.class);
		    job.setMapperClass(Q2Mapper.class);
		    job.setReducerClass(Q2Reducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}