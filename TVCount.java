import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.opencsv.CSVReader; 

public class TVCount {

    public static class TVCountMapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ///Mapper Code here
            StringReader stringReader = new StringReader(value.toString());
            CSVReader reader = new CSVReader(stringReader);
            String[] line = reader.readNext();

            String originCountry = line[1];
            String nameOfShow = line[3];
            int voteCount = Integer.parseInt(line[6]);

            if (originCountry.equals("JP") || originCountry.equals("US") || originCountry.equals("FR")) {
                switch(originCountry) {
                    case "JP":
                        originCountry = "Japan";
                        break;
                    case "US":
                        originCountry = "United States";
                        break;
                    case "FR":
                        originCountry = "France";
                        break;
                }
                context.write(new Text(originCountry+" - "+nameOfShow), new IntWritable(voteCount));
            }
        }
    }

    public static class TVReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            ///Reducer Code here
            for (IntWritable voteValue : values) {
                context.write(key, voteValue);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "tv count");
        job.addFileToClassPath(new Path("opencsv.jar"));
        job.setJarByClass(TVCount.class);
        job.setMapperClass(TVCountMapper.class);
        job.setReducerClass(TVReducer.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
