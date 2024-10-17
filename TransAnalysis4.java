import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransAnalysis4 {

    public static class TransMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            String serviceType = fields[4];
            String customerId = fields[2];
            String amount = fields[3];

            context.write(new Text(serviceType), new Text(customerId + "," + amount));
        }
    }

    // Reducer Class
    public static class TransReducer extends Reducer<Text, Text, Text, Text> {
        private Text mostUsedServiceType = new Text();
        private int maxUsers = 0;
        private double highestRevenue = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashSet<String> uniqueUsers = new HashSet<>();
            double totalRevenue = 0;

            // Sum up all the amounts and count unique users for each service type
            for (Text value : values) {
                String[] fields = value.toString().split(",");
                String customerId = fields[0];
                double amount = Double.parseDouble(fields[1]);
                uniqueUsers.add(customerId);
                totalRevenue += amount;
            }

            int userCount = uniqueUsers.size();

            context.write(key,
                    new Text("Users: " + Integer.toString(userCount) + ", Revenue: " + Double.toString(totalRevenue)));

            if (userCount > maxUsers) {
                mostUsedServiceType.set(key);
                maxUsers = userCount;
                highestRevenue = totalRevenue;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit the most used service type and its total revenue
            context.write(new Text("Most used service type: " + mostUsedServiceType.toString()),
                    new Text("Users: " + Integer.toString(maxUsers) + ", Revenue: " + Double.toString(highestRevenue)));
        }

    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most used service type");
        job.setJarByClass(TransAnalysis4.class);
        job.setMapperClass(TransMapper.class);
        job.setReducerClass(TransReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
