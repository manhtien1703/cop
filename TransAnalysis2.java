import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransAnalysis2 {

    // Mapper Class
    public static class OutdoorRecreationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line into fields
            String[] fields = value.toString().split(",");

            // Extract the necessary fields
            String category = fields[4];
            String customerId = fields[2];
            double amount = Double.parseDouble(fields[3]);

            // Filter by "Outdoor Recreation" category
            if (category.equals("Outdoor Recreation")) {
                // Emit the customer ID and amount
                context.write(new Text(customerId), new DoubleWritable(amount));
            }
        }
    }

    // Reducer Class
    public static class OutdoorRecreationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double totalExpense = 0;

            // Sum up all the amounts for each customer
            for (DoubleWritable value : values) {
                totalExpense += value.get();
            }

            // Emit the customer ID and total expense
            context.write(key, new DoubleWritable(totalExpense));
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Outdoor Recreation Expense");
        job.setJarByClass(TransAnalysis2.class);
        job.setMapperClass(OutdoorRecreationMapper.class);
        job.setReducerClass(OutdoorRecreationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
