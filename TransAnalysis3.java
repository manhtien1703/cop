import java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TransAnalysis3 {

    public static class TransMapper extends Mapper<Object, Text, Text, Text> {

        // user map to keep the userId-userName
        private Map<Integer, String> userMap = new HashMap<>();

        public void setup(Context context) throws IOException, InterruptedException {
            // Read cust.txt from the local filesystem
            try (BufferedReader br = new BufferedReader(new FileReader("cust.txt"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String columns[] = line.split(",");
                    String id = columns[0];
                    String name = columns[1];
                    userMap.put(Integer.parseInt(id), name);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            // Validate input length
            if (fields.length < 3) {
                System.err.println("Invalid input line: " + value.toString());
                return; // Skip this line if it's invalid
            }
            String gameType = fields[4];
            String custId = fields[2];

            String name = userMap.get(Integer.parseInt(custId));
            if (name != null && gameType != null) {
                context.write(new Text(gameType), new Text(name));
            } else {
                context.write(new Text("loi"), new Text("loi"));
            }
        }
    }

    public static class TransReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> userCount = new HashMap<>();
            int maxCount = 0;

            // Count the number of times each customer uses the service
            for (Text value : values) {
                String name = value.toString();
                userCount.put(name, userCount.getOrDefault(name, 0) + 1);
                maxCount = Math.max(maxCount, userCount.get(name));
            }

            // Identify the customer(s) who used the service the most
            List<String> vipCustomers = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : userCount.entrySet()) {
                if (entry.getValue() == maxCount) {
                    vipCustomers.add(entry.getKey() + "-" + entry.getValue());
                }
            }

            // Output the service type (key) and the VIP customer(s)
            context.write(key, new Text(String.join(", ", vipCustomers)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TransAnalysis3");
        job.setJarByClass(TransAnalysis3.class);
        job.setMapperClass(TransMapper.class);
        job.setReducerClass(TransReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        try {
            job.addCacheFile(new URI("hdfs://localhost:8020/mycache/cust.txt"));
        } catch (Exception e) {
            System.out.println("File Not Added");
            System.exit(1);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
