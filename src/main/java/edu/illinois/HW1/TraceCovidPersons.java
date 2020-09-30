package edu.illinois.HW1;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class TraceCovidPersons {

  public static class TracePersonInfoMapper extends Mapper<Object, Text, Text, MapWritable> {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] line = value.toString().split(",");
      MapWritable map = new MapWritable();
      map.put(new Text("location"), new Text(line[1]));
      String[] datetime = line[2].split("\\s");
      map.put(new Text("date"), new Text(datetime[0]));
      String[] datetime1 = datetime[1].split("-");
      map.put(new Text("startTime"), new Text(datetime1[0]));
      map.put(new Text("endTime"), new Text(datetime1[1]));
      context.write(new Text(line[0]), map);
    }
  }

  public static class TraceCovidInfoMapper extends Mapper<Object, Text, Text, MapWritable> {

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      MapWritable map = new MapWritable();
      map.put(new Text("isCovidAffected"), new BooleanWritable(true));
      context.write(value, map);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        Job job1 =
            new Job(
                conf,
                "Look up person Location Info and Infected Person info and get Infected Locations and corresponding times");
        FileUtils.deleteDirectory(new File("affectedGeographics"));
        job1.setJarByClass(TraceCovidPersons.class);
        MultipleInputs.addInputPath(
            job1, new Path(args[0]), TextInputFormat.class, TracePersonInfoMapper.class);
        MultipleInputs.addInputPath(
            job1, new Path(args[1]), TextInputFormat.class, TraceCovidInfoMapper.class);
        FileOutputFormat.setOutputPath(job1, new Path("affectedGeographics"));
        job1.setReducerClass(TraceInfoReducer.class);
        job1.setNumReduceTasks(1);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(MapWritable.class);
        if (!job1.waitForCompletion(true)) {
          System.exit(1);
        }

//        Job job2 = new Job(conf, "Trace Persons in contact with Covid infected persons");
//        job2.addCacheFile(new URI("affectedGeographics"));
//        FileUtils.deleteDirectory(new File("output_Part2"));
//        job2.setJarByClass(Trace2.class);
//        FileOutputFormat.setOutputPath(job2, new Path("output_Part2"));
//        FileInputFormat.addInputPath(job2, new Path(args[0]));
//        job2.setMapperClass(AffectedLocationTimeToPersonCompareMapper.class);
//        job2.setReducerClass(AffectedLocationTimeToPersonCompareReducer.class);
//        job2.setOutputKeyClass(Text.class);
//        job2.setOutputValueClass(Text.class);
//        if (!job2.waitForCompletion(true)) {
//          System.exit(1);
//        }

    Job job2 =
        Job.getInstance(conf, "Collect by Distinct and Merge Pairwise keys and recount total Wins");
    job2.setJarByClass(TraceCovidPersons.class);
    job2.addCacheFile(new URI("affectedGeographics"));
    FileUtils.deleteDirectory(new File("output_Part2"));
    job2.setMapperClass(AffectedLocationTimeToPersonCompareMapper.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(MapWritable.class);
    job2.setReducerClass(AffectedLocationTimeToPersonCompareReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path("output_Part2"));
    if (!job2.waitForCompletion(true)) {
      System.exit(1);
    }
  }

  public static class TraceInfoReducer
      extends Reducer<Text, MapWritable, Text, Collection<Writable>> {

    public void reduce(Text key, Iterable<MapWritable> values, Context context)
        throws IOException, InterruptedException {
      System.out.println("running for key" + key);
      MapWritable infectedInfo = new MapWritable();
      for (MapWritable val : values) {
        for (Writable s : val.keySet()) {
          infectedInfo.put(s, val.get(s));
        }
      }
      if (infectedInfo.containsKey(new Text("location"))
          && infectedInfo.containsKey(new Text("isCovidAffected"))) {
        infectedInfo.remove(new Text("isCovidAffected"));
        String location = String.valueOf(infectedInfo.get(new Text("location")));
        infectedInfo.remove(new Text("location"));
        context.write(new Text(location), infectedInfo.values());
      }
    }
  }

  public static class AffectedLocationTimeToPersonCompareMapper
      extends Mapper<Object, Text, Text, MapWritable> {

    private Set<String> affectedGeographics = new HashSet<>();
    private Set<String> possibleContactWithInfected = new HashSet<>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      String thisLine = null;
      URI[] cacheFiles = context.getCacheFiles();
      System.out.println("---------------------------------am in setup");
      if (cacheFiles != null && cacheFiles.length > 0) {
        try {
          FileSystem fs = FileSystem.get(context.getConfiguration());
          Path path = new Path(cacheFiles[0].toString() + "/part-r-00000");
          BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
          while ((thisLine = reader.readLine()) != null) {
            System.out.println(thisLine);
            affectedGeographics.add(thisLine);
          }
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    }

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      System.out.println("---------------------------------am in map");
      //            System.out.println(affectedGeographics);
      String[] line = value.toString().split(",");
      String location = line[1];
      String[] datetime = line[2].split("\\s");
//      String date = datetime[0];
      String[] datetime1 = datetime[1].split("-");
//      String startTime = datetime1[0];
//      String endTime = datetime1[1];
      MapWritable outlist = new MapWritable();
      affectedGeographics.stream()
          .forEach(
              item -> {
                String[] affected = item.split("\t");
                if (location.equalsIgnoreCase(affected[0])) {
                                      outlist.put(new Text("infected"), new Text(line[0]));
                }
              });
      context.write(new Text("infected"), outlist);
    }
  }

  public static class AffectedLocationTimeToPersonCompareReducer
      extends Reducer<Text, MapWritable, Text, Text> {

    public void reduce(Text key, Iterable<MapWritable> values, Context context)
        throws IOException, InterruptedException {
      System.out.println(
          "--------------------------AffectedLocationTimeToPersonCompareReducer running for key"
              + key);
      Set<String> finalAffectedList = new HashSet<>();
      for (MapWritable aff : values) {
        if (aff.keySet().size() > 0) {
            String tmp = aff.get(new Text("infected")).toString();
                    finalAffectedList.add(tmp);
        }
      }
      for (String s : finalAffectedList) {
        context.write(new Text(""), new Text(s));
      }
    }
  }
}
