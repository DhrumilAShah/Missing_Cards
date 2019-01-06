import java.util.ArrayList;

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
public class MissingCards{
    public static class PMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        //Mapper Function
        @Override
        public void map(LongWritable lw, Text t, Context c){
          try{
            String[] stringArr = t.toString().split(" ");
            c.write(new Text(stringArr[0]),new IntWritable(Integer.parseInt(stringArr[1])));
          }catch(Exception e){
            e.printStackTrace();
          }
        }
    }
     public static class PReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        //Reducer Function
        @Override
        public void reduce(Text t, Iterable<IntWritable> iw, Context c){
          try{
            int iterator = 1,cardPosition = 0;
            ArrayList<Integer> suite = new ArrayList<Integer>();
            for(iterator=1; iterator<=13; iterator++)
              suite.add(iterator);
            for(IntWritable crd : iw){
              cardPosition = crd.get();
              if(suite.contains(cardPosition))
                suite.remove(suite.indexOf(cardPosition));
            }
            for(iterator=0; iterator<suite.size(); iterator++)
              c.write(t, new IntWritable(suite.get(iterator)));
          }catch(Exception e){
            e.printStackTrace();
          }
        }
    }
    public static void main(String[] args)throws Exception{
        Configuration config = new Configuration();
        Job job = new Job(config, "MissingCards");
        job.setJarByClass(MissingCards.class);
        job.setMapperClass(PMapper.class);
        job.setReducerClass(PReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}