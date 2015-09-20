import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Recommender {
	
	
	
	public static class FriendMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		  
		
		
		  IntWritable userID = new IntWritable();
		  Text friends = new Text();
		  HashMap<String,String> hash = new HashMap<String,String>(); 
		  @Override
		  public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException {

		    // Program now begins reading input dataset line by line
			  
			  String split_1[] = value.toString().split("\t");
			  String split_2[]=null;
			  
			  userID.set(Integer.parseInt(split_1[0]));
			  if(split_1.length==1){friends.set("null");context.write(userID,friends);}
			  else{
			  split_2 = split_1[1].split(",");
			  
			//Write user id as key and each friendID,-1 as value. This shows that user and those id's are friends already and shouldnt be recommended.
			  
			  }
			  if(split_2!=null)
			  {
			  for(int i=0;i<split_2.length;i++)
			  {
				  if(split_2[i] != null)
				  {
				  friends.set(split_2[i] + ",-1");
				  
				  context.write(userID,friends);
		//		  hash.put(split_2[i],split_2[i]);
				  }
				
			  }
			  
			//Now iterate over the hashmap to map each friend combo. This time use +1 since we do not know whether or not they are friends.
			  
		/*	  for(String hashkey : hash.keySet()){
				  
				  for(String hashValue : hash.values()){
					  
					  if((hashkey.equals(hashValue))==false)
					  {
						  userID.set(Integer.parseInt(hashkey));
						  friends.set(hashValue + ",1");
						  context.write(userID, friends);
					  }
				  }
			  }*/
			  
			  for(int i=0;i<split_2.length;i++){
				  for(int j=0;j<split_2.length;j++){
					  if(split_2[i] != split_2[j]){
						  userID.set(Integer.parseInt(split_2[i]));
						  friends.set(split_2[j]+",1");
						  context.write(userID, friends);
					  }
				  }
			  }
			  
			  }
			  
			  
			  
		  }
		  
		  
		}
	
	public static class FriendReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		  
		
		  HashMap<Integer,Integer> hash = new HashMap<Integer,Integer>();
		  StringBuilder recommendedList = new StringBuilder();
		  LinkedList<Integer> friendId = new LinkedList<Integer>();
		  LinkedList<Integer> comFriendCount = new LinkedList<Integer>();
		  Text result = new Text();
		  Text currentVal = new Text();
		  int count,flag;
	      int temp,temp1,temp2;

		  
		  public void reduce(IntWritable key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
		  
			  for(Text value: values){
				  
				  currentVal = value;
				  if(currentVal.toString().equals("null") == false){
				  
				  flag = Integer.parseInt(currentVal.toString().split(",")[1]);
				  if(hash.containsKey(Integer.parseInt(currentVal.toString().split(",")[0]))){
						  
					      if(flag==1){
					    	  if(hash.get(Integer.parseInt(currentVal.toString().split(",")[0])) != 0){
					    		  
					    	  
					    		  temp=hash.get(Integer.parseInt(currentVal.toString().split(",")[0])) + 1;
					    		  hash.put(Integer.parseInt(currentVal.toString().split(",")[0]),temp);
					    	  }
					      }
					      else{
							  hash.put((Integer.parseInt(currentVal.toString().split(",")[0])),0); 
					  }
					      
					    	  
						  
					  }
					  else{
						  if(flag==1){
						  hash.put(Integer.parseInt(currentVal.toString().split(",")[0]), 1);
						  }
						  else{
							  hash.put((Integer.parseInt(currentVal.toString().split(",")[0])),0); 
					  }
				  }
				  }
				  
				  else {
					  
					  result.set("\t");
					  context.write(key,result);
				  }
			  }
			 for(Map.Entry<Integer,Integer> entry : hash.entrySet() ){
				 if(entry.getValue()!=0){
				 friendId.add(entry.getKey());
				 comFriendCount.add(entry.getValue());
				 }
				 
			 }
			 
			 
			 for(int i=0;i<comFriendCount.size();i++){
				 for(int j=i+1;j<comFriendCount.size();j++){
					 
					 if(comFriendCount.get(i)<comFriendCount.get(j)){
						 temp1=friendId.get(j);
						 friendId.set(j, friendId.get(i));
						 friendId.set(i,temp1);
						 
						 temp2=comFriendCount.get(j);
						 comFriendCount.set(j, comFriendCount.get(i));
						 comFriendCount.set(i,temp2);
					 }
					 
					 else{
						 
						 if(comFriendCount.get(i)==comFriendCount.get(j) && friendId.get(i)>friendId.get(j)){
							 
							 temp1=friendId.get(j);
							 friendId.set(j, friendId.get(i));
							 friendId.set(i,temp1);
							 
						 }
					 }
				 }
			 }
			 if(friendId.size()>0)
			 {
			 recommendedList.append("\t").append(friendId.get(0).toString());
			 for(int k=1;k<Math.min(10,friendId.size());k++){
				 recommendedList = recommendedList.append(",").append(friendId.get(k).toString());
			 }
			 result.set(recommendedList.toString());
			 hash.clear();
			 friendId.clear();
			 comFriendCount.clear();
			 recommendedList.setLength(0);
			 
			 context.write(key, result);
			
			 }
			 
			 
		 
			  }
			
		  }
		  
		  
		
	
	
		public static void main(String[] args) throws Exception{
			
			/*int res  = ToolRunner .run( new Recommender(), args);
		     System .exit(res);*/
			Configuration myconf = new Configuration();
			Job job  = Job .getInstance(myconf, " recommender ");
			job.setJarByClass(Recommender.class);
			
			FileInputFormat.addInputPath(job,  new Path(args[ 0]));
		     FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
		     
		     job.setMapperClass( FriendMapper.class);
		     job.setReducerClass( FriendReducer.class);
		     job.setOutputKeyClass( IntWritable .class);
		     job.setOutputValueClass( Text .class);
		     System.exit(job.waitForCompletion(true)  ? 0 :1);
		
			
		}
		
		
			
			


}
