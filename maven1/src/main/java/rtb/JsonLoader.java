package rtb;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.Preconditions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.OriginalType.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
 
public class JsonLoader {
	
	  final static Logger logger = Logger.getLogger(JsonLoader.class);

	      public void json_load_parquet(
                                        String src_json_file_path,
                                        String loaded_prq_fpath,
                                        String regstr_prq_fpath
                                       ){
		    try{
				JsonFactory f = new JsonFactory();
		        JsonParser jp = f.createParser(new File(src_json_file_path));
		        ObjectMapper mapper = new ObjectMapper(); 
		        
		  	    MessageType APPLOADED_FILE_SCHEMA = Types.buildMessage()
		  		      .required(INT64).as(TIMESTAMP_MILLIS).named("time")
		  		      .required(BINARY).as(UTF8).named("email")
		  		      .required(BINARY).as(UTF8).named("device_type")
		  		      .named("AppLoaded");
		  	    
		  	    MessageType REGISTERED_FILE_SCHEMA = Types.buildMessage()
			  		      .required(INT64).as(TIMESTAMP_MILLIS).named("time")
			  		      .required(BINARY).as(UTF8).named("email")
			  		      .optional(BINARY).as(UTF8).named("channel")
			  		      .named("Registered");

		  	    SimpleGroupFactory GROUP_FACTORY_APP_LOADED = new SimpleGroupFactory(APPLOADED_FILE_SCHEMA);
		  	    SimpleGroupFactory GROUP_FACTORY_REGISTERED = new SimpleGroupFactory(REGISTERED_FILE_SCHEMA);
		  	    
		  	    File fl_apploaded = new File(loaded_prq_fpath);
		  	    logger.info("Recreate parquet file for app_loaded events: "+fl_apploaded.getAbsolutePath());
		  	    if (fl_apploaded.exists()==true) {
			      Preconditions.checkArgument(fl_apploaded.delete(), "Could not remove parquet file "+fl_apploaded.getAbsolutePath());
		  	    }
			    Path pth_file_apploaded = new Path(fl_apploaded.toString());

		  	    File fl_registered = new File(regstr_prq_fpath);
		  	    logger.info("Recreate parquet file for registered events: "+fl_registered.getAbsolutePath());
		  	    if (fl_registered.exists()==true) {
			      Preconditions.checkArgument(fl_registered.delete(), "Could not remove parquet file "+fl_registered.getAbsolutePath());
		  	    }
			    Path pth_file_registered = new Path(fl_registered.toString());
				

			    ParquetWriter<Group> writer1 = ExampleParquetWriter.builder(pth_file_apploaded)
			        .withType(APPLOADED_FILE_SCHEMA)
			        .build();

			    ParquetWriter<Group> writer2 = ExampleParquetWriter.builder(pth_file_registered)
				        .withType(REGISTERED_FILE_SCHEMA)
				        .build();

		        int RegisterCounter = 0;
		        int LoadCounter     = 0;
		        int AnyCounter      = 0;
		        int TotalEventsCnt  = 0;

		        while(jp.nextToken() == JsonToken.START_OBJECT) {
		        	  JsonNode node = mapper.readTree(jp);
		        	  TotalEventsCnt++; 
		        	    if (node.get("_n").toString().equals("\"app_loaded\"")) {	
		        	    	LoadCounter++;
		        	    	Group group1 = GROUP_FACTORY_APP_LOADED.newGroup(); 
		        		    group1.add("time",       node.get("_t").asLong()*1000);
		        		    group1.add("email",      node.get("_p").toString());
		        		    group1.add("device_type",node.get("device_type").toString());
		        		    writer1.write(group1);
		        	    } 
		        	    else if (node.get("_n").toString().equals("\"registered\"")) {  
		        	    	RegisterCounter++;
		        	    	 Group group2 = GROUP_FACTORY_REGISTERED.newGroup();///   maybe move up
		        		        group2.add("time",       node.get("_t").asLong()*1000);
		        		        group2.add("email",      node.get("_p").toString());
		        		        if (node.has("channel")){
			        		      group2.add("channel",  node.get("channel").toString());
			        		    }
		        		        writer2.write(group2);
		        	    } 
		        	    else {
		        	    	AnyCounter++;	
		        	    }
		        	}
		        
		        logger.info("RegisterCounter = "+String.valueOf(RegisterCounter));
		    	logger.info("LoadCounter     = "+String.valueOf(LoadCounter));
		    	logger.info("AnyCounter      = "+String.valueOf(AnyCounter));
		    	logger.info("TotalEvents     = "+String.valueOf(TotalEventsCnt));
		    	
		     writer1.close();
		     writer2.close();  
		   
			 } 
		     catch (IOException e) {
			  logger.error(e.fillInStackTrace());
			 }

		       SparkSession spark = SparkSession
		    	    	.builder()
		    		    .appName("Java Spark SQL basic example")
		    		    .config("spark.master", "local[*]")
		    		    .getOrCreate();

		    	Dataset<Row> df_appl = spark.read().load(loaded_prq_fpath);
		    	df_appl.show();
		    	df_appl.printSchema();
		    	
		    	Dataset<Row> df_reg = spark.read().load(regstr_prq_fpath);
		    	df_reg.show();
		    	df_reg.printSchema();	 	
	}

	public void final_calculate(
                                String apploaded_prq_fpath,
                                String regstr_prq_fpath
                               ){
       SparkSession spark = SparkSession
    	       	.builder()
       	        .appName("Java Spark SQL basic example") 
       	        .config("spark.master", "local[*]")
	            .config("spark.sql.crossJoin.enabled", "true")
	            .getOrCreate();

		Dataset<Row> sqlLoadedDF = spark.read().load(apploaded_prq_fpath);
		Dataset<Row> sqlRegistrDF = spark.read().load(regstr_prq_fpath);

		sqlLoadedDF.createOrReplaceTempView("v_aload");
		sqlRegistrDF.createOrReplaceTempView("v_reg");

		// OLD Vers. exclude delta time. 48.2 % Bad result.
		// and v_aload.time > v_reg.time and v_aload.time between v_reg.time and
		// date_add(v_reg.time,7)
		// 48.2 because looks like date_add truncate timestamp to date before
		// and then add 7 days, we loss time interval.
		//---------------------------------------------------------------------
		// NEW Vers.
		// and v_aload.time > v_reg.time and datediff(v_aload.time,v_reg.time)
		// <= 7 -- 50.6% Good result.
		Dataset<Row> usrs_CntApplod_WeekReg = spark
				.sql(" SELECT ds.CNT_REG_WEEK_ALOAD, "
						+ "        reg_tot.CNT_REG_TOTAL, "
						+ "        CASE reg_tot.CNT_REG_TOTAL "
						+ "          WHEN 0 "
						+ "          THEN 100 "
						+ "          ELSE round((ds.CNT_REG_WEEK_ALOAD * 100)/reg_tot.CNT_REG_TOTAL,1) END as PRCNT "
						+ "   FROM ( "
						+ " SELECT count(distinct email) as CNT_REG_WEEK_ALOAD"
						+ "   FROM v_reg  "
						+ "  WHERE EXISTS("
						+ "               SELECT 1"
						+ "                 FROM v_aload "
						+ "                WHERE v_aload.email = v_reg.email "
						+ "                  and v_aload.time > v_reg.time and datediff(v_aload.time,v_reg.time) <= 7"
						+ "               )) ds,"
						+ "  (SELECT count(distinct r.email) as CNT_REG_TOTAL from v_reg r) reg_tot "
						+ " ");
		usrs_CntApplod_WeekReg.show();

		String prcnt = usrs_CntApplod_WeekReg.select("PRCNT")
				.as(Encoders.STRING()).collectAsList().stream()
				.collect(Collectors.joining(","));
		
		logger.info("                                                  ");
		logger.info("--------------------------------------------------");
		logger.info("                                                  ");
		logger.info("     Result of calculation is : " + prcnt + " %");
		System.out.println("     Result of calculation is : " + prcnt + " %");
		logger.info("                                                  ");
		logger.info("--------------------------------------------------");
		logger.info("                                                  ");
}
	
	

public void debug_ds(
            String apploaded_prq_fpath
           ){
		SparkSession spark = SparkSession.builder()
				.appName("Java Spark SQL basic example")
				.config("spark.master", "local[*]")
				.config("spark.sql.crossJoin.enabled", "true").getOrCreate();

		Dataset<Row> sqlLoadedDF = spark.read().load(apploaded_prq_fpath);

		sqlLoadedDF.createOrReplaceTempView("v_aload");

		Dataset<Row> usrs_CntApplod_WeekReg = spark
				.sql("SELECT v_aload.time,  current_timestamp as cts, datediff(current_timestamp,v_aload.time) as dd, date_add(v_aload.time,7) as t7 FROM v_aload ");

		usrs_CntApplod_WeekReg.show();     
}
	
	
	
}


