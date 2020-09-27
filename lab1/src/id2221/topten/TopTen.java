package id2221.topten;



import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import java.util.stream.Collectors;

public class TopTen {
	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}

	public static class TopTenMapper extends Mapper<Object, Text, IntWritable, Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		for ( String in : transformXmlToMap(value.toString()).keySet() ) {
			if( in.equals("Id") && !(transformXmlToMap(value.toString()).get(in)).equals("-1")) {
				repToRecordMap.put(Integer.parseInt(transformXmlToMap(value.toString()).get("Reputation")), new Text(transformXmlToMap(value.toString()).get("Id")));
			}
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
	    // Output our ten records to the reducers with a null key
	    //<FILL IN>
        TreeMap<Integer, Text> myNewMap = repToRecordMap.entrySet().stream()
                .limit(10)
                .collect(TreeMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

        for ( Integer in : myNewMap.keySet() ) {
            context.write(new IntWritable(in), myNewMap.get(in));
        }

	}
	}

	public static class TopTenReducer extends TableReducer<IntWritable, Text, IntWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text each : values) {
				repToRecordMap.put(key.get(), each);
				
        Put insHBase = new Put(Integer.toString(key.get()).getBytes());
		insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(Integer.toString(key.get())));
		insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(each.toString()));
        context.write(null, insHBase);
			}

	}

	}

	public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();

        // define scan and define column families to scan
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        Job job = Job.getInstance(conf, "top_rep");
        job.setJarByClass(TopTen.class);

        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
