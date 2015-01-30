import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankReducer extends Reducer <Text, Text, Text, Text> {
	public void reduce (Text baseUrl, Iterable <Text> values, Context context) throws IOException, InterruptedException {
	
		Double pageRank = 0.0 ;
		Double aPageRank = 0.0;
		String linkedUrl=null;
		
		for(Text value:values)
		{
			
			String[] mysteryObj = value.toString().split("\t", 2);
			
			//GraphObject
			if(mysteryObj.length==2)
			{
				linkedUrl=mysteryObj[1];
				
			}
			
			else
			{
				aPageRank=Double.parseDouble(mysteryObj[0].toString());
				pageRank+=aPageRank;	
				
		
			}
		}
		
		pageRank=(1-0.85)+(0.85*pageRank);
		String graphObject;
		
		if (linkedUrl==null || linkedUrl.equals(""))
		{
			graphObject=Double.toString(pageRank)+"\t";
		}
		
		else
		{
			StringBuilder pageRankAndLinkedUrl = new StringBuilder();
			pageRankAndLinkedUrl.append(Double.toString(pageRank)+"\t"+linkedUrl);
			graphObject=pageRankAndLinkedUrl.toString();
			//System.out.println("New Graph Object :"+graphObject);
			
		}
		
		Text pageRankAndLinkedUrlText = new Text(graphObject);
		context.write(baseUrl,pageRankAndLinkedUrlText);
	}
	

}
