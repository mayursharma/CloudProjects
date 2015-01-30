import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

	Double pageRank = null;
	String[] linkedUrl= null;
	
	public void map(Text baseUrl, Text value, Context context) throws IOException, InterruptedException 
	{
		String[] pageRankAndLinkedUrl = value.toString().split("\t", 2);
		
		if (pageRankAndLinkedUrl.length==2) //PageRank&Links
		{
			pageRank=Double.parseDouble(pageRankAndLinkedUrl[0]);
			linkedUrl= pageRankAndLinkedUrl[1].split(",");
			
			//Calculating new PageRank
			pageRank = pageRank/linkedUrl.length;
			
			//Sending GraphObject to reducer
			context.write(baseUrl,value);
			
			//Sending various PageRanks to reducer
			for(int i=0;i<linkedUrl.length;i++)
			{
				if (!linkedUrl[i].equals("")&&(!linkedUrl[i].equals("\t"))&&(!linkedUrl[i].equals(" ")&&(!linkedUrl[i].equals(","))))
				{
					context.write(new Text(linkedUrl[i]),new Text(String.valueOf(pageRank)));	
				}
				
			}
		}
		
		
		

	
	
	}
	
}
