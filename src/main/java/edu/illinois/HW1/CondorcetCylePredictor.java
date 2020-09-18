package edu.illinois.HW1;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class CondorcetCylePredictor {

  public static class VoteSplitterMapper extends Mapper<Object, Text, Text, Text> {

    private Text voteKey = new Text();
    private Text countValue = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] votes = value.toString().split(",");
      for (int i = 0; i < votes.length; i++) {
        for (int j = i + 1; j < votes.length; j++) {
          try {
            if (i != votes.length - 1) {
              char[] newKey = (votes[i].split("\\.")[0] + votes[j].split("\\.")[0]).toCharArray();
              Arrays.sort(newKey);
              voteKey.set(new String(newKey));
              countValue.set(votes[i].split("\\.")[0]);
              context.write(voteKey, countValue);
            }
          } catch (ArrayIndexOutOfBoundsException ex) {
            continue;
          }
        }
      }
    }
  }

  public static class VoteSplitterReducer extends Reducer<Text, Text, Text, IntWritable> {
    private Text voteKey = new Text();
    private static final IntWritable one = new IntWritable(1);

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      List<String> result = new ArrayList<>();
      for (Text str : values) {
        result.add(str.toString());
      }
      Map.Entry<String, Long> maxEntry =
          Collections.max(
              result.stream()
                  .collect(Collectors.groupingBy(str -> str, Collectors.counting()))
                  .entrySet(),
              Comparator.comparing(Map.Entry::getValue));
      voteKey.set(maxEntry.getKey());
      context.write(voteKey, one);
    }
  }

  public static class IntermediateResultMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] tokens = value.toString().split("\t");
      context.write(new Text(tokens[0]), new IntWritable(Integer.parseInt(tokens[1])));
    }
  }

  public static class IntermediateResultReducer
      extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        System.out.println(val);
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class ElectionResultFinalMapper extends Mapper<Object, Text, Text, Text> {
    private HashMap<String, Integer> hashMap = new HashMap<>();

    public void map(Object key, Text value, Context context) {
      String[] tokens = value.toString().split("\t");
      hashMap.put(tokens[0], Integer.parseInt(tokens[1]));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      Optional<Integer> f =
          hashMap.values().stream().collect(Collectors.maxBy(Comparator.comparing(item -> item)));
      Set<String> whateweNeed =
          hashMap.entrySet().stream()
              .filter(entry -> Objects.equals(entry.getValue(), f.get()))
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet());
      context.write(new Text("Winner(s) is\\are"), new Text(whateweNeed.toString()));
    }
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {

    FileUtils.deleteDirectory(new File("output"));
    FileUtils.deleteDirectory(new File("FinalOutput"));

    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "Get the Total Win count for Every Possible Pair ");
    job1.setJarByClass(CondorcetCylePredictor.class);
    job1.setMapperClass(VoteSplitterMapper.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setReducerClass(VoteSplitterReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    if (!job1.waitForCompletion(true)) {
      System.exit(1);
    }

    Job job2 =
        Job.getInstance(conf, "Collect by Distinct and Merge Pairwise keys and recount total Wins");
    job2.setJarByClass(CondorcetCylePredictor.class);
    job2.setMapperClass(IntermediateResultMapper.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setReducerClass(IntermediateResultReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[1], "Split2"));
    if (!job2.waitForCompletion(true)) {
      System.exit(1);
    }

    Job job3 = Job.getInstance(conf, "Predict the Winner(s)");
    job3.setJarByClass(CondorcetCylePredictor.class);
    job3.setMapperClass(ElectionResultFinalMapper.class);
    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(Text.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job3, new Path(args[1], "Split2"));
    FileOutputFormat.setOutputPath(job3, new Path("FinalOutput", "Result"));
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
