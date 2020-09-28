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

    private static final int N = 10;

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

	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!transformXmlToMap(value.toString()).isEmpty() && transformXmlToMap(value.toString()).get("Id") != null ) {
                repToRecordMap.put(Integer.parseInt(transformXmlToMap(value.toString()).get("Reputation")), new Text(value));
            }
        }

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output our ten records to the reducers with a null key
            int i = 0;
            for (Map.Entry<Integer, Text> entry : repToRecordMap.descendingMap().entrySet()) {
                if (i++ < N) {
                    context.write(NullWritable.get(), entry.getValue() );
                }
            }
        }

	}

	public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				repToRecordMap.put( Integer.parseInt(transformXmlToMap(value.toString()).get("Reputation")) , new Text( transformXmlToMap(value.toString()).get("Id") ) );
			}

            int i = 0;
            for (Map.Entry<Integer, Text> entry : repToRecordMap.descendingMap().entrySet()) {
				Put insHBase = new Put(Integer.toString(i++).getBytes());
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(entry.getKey().toString()));
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(entry.getValue().toString()));
				context.write(NullWritable.get(), insHBase);
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
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
