package com.platform.util;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
					
/**
 * @author dgunify
 * java 操作 sqoop
 *
 */
public class SqoopUtil {
    static SqoopClient client;

    //创建db连接
    public static boolean createDbLink(Map<String,String> parMap)throws Exception {
        try {
        	client = new SqoopClient(parMap.get("sqoopHost"));
			MLink link = client.createLink("generic-jdbc-connector");
			// 随机生成名字,不能过长,否则会报错
			link.setName(parMap.get("name"));
			link.setCreationUser(parMap.get("creationUser"));
			MLinkConfig linkConfig = link.getConnectorLinkConfig();
			linkConfig.getStringInput("linkConfig.connectionString").setValue(parMap.get("dbconnectionurl"));
			linkConfig.getStringInput("linkConfig.jdbcDriver").setValue(parMap.get("dbjdbcdriver"));
			linkConfig.getStringInput("linkConfig.username").setValue(parMap.get("dbusername"));
			linkConfig.getStringInput("linkConfig.password").setValue(parMap.get("dbpassword"));
			// 这里必须指定 identifierEnclose, 他默认是双引号,也会报错
			linkConfig.getStringInput("dialect.identifierEnclose").setValue(" ");
			Status status = client.saveLink(link);
			
			if (status.canProceed()) {
			    System.out.println("Created Link with Link Name : " + link.getName());
			    return true;
			} else {
			    System.out.println("Something went wrong creating the link");
			    return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
    }
    
  //创建oracle连接
    public static boolean createOracleLink(Map<String,String> parMap) throws Exception{
        try {
        	client = new SqoopClient(parMap.get("sqoopHost"));//parMap.get("sqoopHost")
			MLink link = client.createLink("oracle-jdbc-connector");
			// 随机生成名字,不能过长,否则会报错
			link.setName(parMap.get("name"));
			link.setCreationUser(parMap.get("creationUser"));
			MLinkConfig connectionConfig = link.getConnectorLinkConfig();
			connectionConfig.getStringInput("connectionConfig.connectionString").setValue(parMap.get("dbconnectionurl"));
			connectionConfig.getStringInput("connectionConfig.username").setValue(parMap.get("dbusername"));
			connectionConfig.getStringInput("connectionConfig.password").setValue(parMap.get("dbpassword"));
			
			Map<String,String> jdbcPropertiesMap = new HashMap<String, String>();
			jdbcPropertiesMap.put("jdbc.driverClassName", parMap.get("dbjdbcdriver"));
			connectionConfig.getMapInput("connectionConfig.jdbcProperties").setValue(jdbcPropertiesMap);
			
			Status status = client.saveLink(link);
			if (status.canProceed()) {
				System.out.println("=================");
				System.out.println("=================");
				System.out.println("link proceed name =  " + status.name());
				System.out.println("=================");
				System.out.println("=================");
			    System.out.println("Created Link with Link Name : " + link.getName());
			    return true;
			} else {
			    System.out.println("Something went wrong creating the link");
			    return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
    }


    //创建HDFS连接
    public static boolean createHdfsLink(Map<String,String> parMap) throws Exception{
        try {
        	client = new SqoopClient(parMap.get("sqoopHost"));
			MLink link = client.createLink("hdfs-connector");
			link.setName(parMap.get("name"));
			link.setCreationUser(parMap.get("creationUser"));
			MLinkConfig linkConfig = link.getConnectorLinkConfig();
			linkConfig.getStringInput("linkConfig.uri").setValue(parMap.get("hdfsuri"));
			linkConfig.getStringInput("linkConfig.confDir").setValue(parMap.get("hdfsconfdir"));
			Status status = client.saveLink(link);
			if (status.canProceed()) {
			    System.out.println("Created Link with Link Name : " + link.getName());
			    return true;
			} else {
			    System.out.println("Something went wrong creating the link");
			    return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
    }
    
    public static boolean deleteLink(Map<String,String> parMap)throws Exception {
    	try {
    			client = new SqoopClient(parMap.get("sqoopHost"));
    			client.deleteLink(parMap.get("linkId"));
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return true;
    }
    
    public static boolean deleteJob(Map<String,String> parMap)throws Exception {
    	try {
    			client = new SqoopClient(parMap.get("sqoopHost"));
    			client.deleteJob(parMap.get("jobName"));
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return true;
    }

    //创建db导入hdfs任务
    public static boolean createDb2HdfsJob(Map<String,String> parMap) throws Exception{
    	try {
			client = new SqoopClient(parMap.get("sqoopHost"));
			MJob job = client.createJob(parMap.get("fromLink"),parMap.get("toLink"));
			job.setName(parMap.get("name"));//任务名字
			job.setCreationUser(parMap.get("creationUser"));//创建人
			MFromConfig fromJobConfig = job.getFromJobConfig();

			//数据库名
			//mysql 使用数据库名，oracle 使用用户名
			fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue(parMap.get("dbname"));
			//表名
			fromJobConfig.getStringInput("fromJobConfig.tableName").setValue(parMap.get("dbtablename"));
			//字段
			fromJobConfig.getListInput("fromJobConfig.columnList").setValue(Arrays.asList(parMap.get("dbcolumnlist").split(",")));
			
			//添加  sql
			if(parMap.get("dbsql") != null && !"".equals(parMap.get("dbsql"))) {
				fromJobConfig.getStringInput("fromJobConfig.sql").setValue(parMap.get("dbsql"));
			}
			//dbpartitioncolumnlist
			if(parMap.get("dbpartitioncolumnlist") != null && !"".equals(parMap.get("dbpartitioncolumnlist"))) {
				fromJobConfig.getStringInput("fromJobConfig.partitionColumn").setValue(parMap.get("dbpartitioncolumnlist"));
			}
			
			MToConfig toJobConfig = job.getToJobConfig();
			
			List<MConfig> ls =toJobConfig.getConfigs();
			for(MConfig mc : ls) {
				String lbkey = mc.getLabelKey();
				System.out.println("lbkey:"+lbkey);
				String helpkey = mc.getHelpKey();
				System.out.println("helpkey:"+helpkey);
			}
			
			//输出位置 lt_drug_report
			toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue(parMap.get("outputdirectory"));
			//输出格式
			toJobConfig.getEnumInput("toJobConfig.outputFormat").setValue(parMap.get("outputformat"));
			//压缩格式
			toJobConfig.getEnumInput("toJobConfig.compression").setValue(parMap.get("compression"));
			
			
			//
			toJobConfig.getBooleanInput("toJobConfig.overrideNullValue").setValue(true);
			MDriverConfig driverConfig = job.getDriverConfig();
			//提取数量
			Integer numExtractors = 1;
			if(parMap.get("extractors") != null && !"".equals(parMap.get("extractors"))) {
				numExtractors = Integer.parseInt(parMap.get("extractors"));
			}
			driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(numExtractors);
			//加载数量
			Integer numLoaders = 0;
			if(parMap.get("loaders") != null && !"".equals(parMap.get("loaders"))) {
				numLoaders = Integer.parseInt(parMap.get("loaders"));
			}
			driverConfig.getIntegerInput("throttlingConfig.numLoaders").setValue(numLoaders);
			
			
			//保存任务
			Status status = client.saveJob(job);
			if (status.canProceed()) {
			    System.out.println("Created Job with Job Name: " + job.getName());
			    return true;
			} else {
			    System.out.println("Something went wrong creating the job");
			    return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return false;
    }
    
    //创建oracle导入hdfs任务
    public static boolean createOracle2HdfsJob(Map<String,String> parMap) throws Exception{
    	try {
			client = new SqoopClient(parMap.get("sqoopHost"));
			MJob job = client.createJob(parMap.get("fromLink"),parMap.get("toLink"));
			job.setName(parMap.get("name"));//任务名字
			job.setCreationUser(parMap.get("creationUser"));//创建人
			//ID,ORG_NAME,DEPT_NAME,USER_NAME,PASSWORD,EMAIL,QQ,LINK_PHONE,PHONE_NO,CONTACT_ADDRESS
			MFromConfig fromJobConfig = job.getFromJobConfig();

			//表名
			fromJobConfig.getStringInput("fromJobConfig.tableName").setValue(parMap.get("dbtablename"));
			//字段
			if(parMap.get("dbcolumnlist") !=null && !"".equals(parMap.get("dbcolumnlist"))) {
				fromJobConfig.getListInput("fromJobConfig.columns").setValue(Arrays.asList(parMap.get("dbcolumnlist").split(",")));
			}
			
			fromJobConfig.getEnumInput("fromJobConfig.dataChunkMethod").setValue("ROWID");
			
			fromJobConfig.getEnumInput("fromJobConfig.dataChunkAllocationMethod").setValue("RANDOM");
			
			fromJobConfig.getEnumInput("fromJobConfig.whereClauseLocation").setValue("SPLIT");
			
			
			
			MToConfig toJobConfig = job.getToJobConfig();
			//输出位置
			toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue(parMap.get("outputdirectory"));
			//输出格式
			toJobConfig.getEnumInput("toJobConfig.outputFormat").setValue(parMap.get("outputformat"));
			//压缩格式
			toJobConfig.getEnumInput("toJobConfig.compression").setValue(parMap.get("compression"));
			//
			toJobConfig.getBooleanInput("toJobConfig.overrideNullValue").setValue(true);
			MDriverConfig driverConfig = job.getDriverConfig();
			driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(1);
			//保存任务
			Status status = client.saveJob(job);
			if (status.canProceed()) {
			    System.out.println("Created Job with Job Name: " + job.getName());
			    return true;
			} else {
			    System.out.println("Something went wrong creating the job");
			    return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return false;
    }

    //创建hdfs导入mysql任务
    public static boolean createHdfs2DbJob(Map<String,String> parMap) throws Exception {
    	try {
			client = new SqoopClient(parMap.get("sqoopHost"));
			MJob job = client.createJob(parMap.get("fromLink"),parMap.get("toLink"));
			job.setName(parMap.get("name"));
			job.setCreationUser(parMap.get("creationUser"));
			MFromConfig fromJobConfig = job.getFromJobConfig();

			fromJobConfig.getStringInput("fromJobConfig.inputDirectory").setValue(parMap.get("inputdirectory"));
			MToConfig toJobConfig = job.getToJobConfig();
			toJobConfig.getStringInput("toJobConfig.tableName").setValue(parMap.get("dbtablename"));
			toJobConfig.getListInput("toJobConfig.columnList").setValue(Arrays.asList(parMap.get("dbcolumnlist").split(",")));
			MDriverConfig driverConfig = job.getDriverConfig();
			driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(5);

			Status status = client.saveJob(job);
			if (status.canProceed()) {
			    System.out.println("Created Job with Job Name: " + job.getName());
			    return true;
			} else {
			    System.out.println("Something went wrong creating the job");
			    return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return false;
    }
    
    //停止任务
    public static boolean stopJob(Map<String,String> parMap) throws Exception{
    	try {
			client = new SqoopClient(parMap.get("sqoopHost"));
			MSubmission submission = client.getJobStatus(parMap.get("jobName"));
			if (submission.getStatus().isRunning()) {
				client.stopJob(parMap.get("jobName"));
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return false;
    }
    
    //获取进度
    public static String getJobProgress(Map<String,String> parMap) throws Exception{
    	try {
			client = new SqoopClient(parMap.get("sqoopHost"));
			 MSubmission submission = client.getJobStatus(parMap.get("jobName"));
			if (submission.getStatus().isRunning() && submission.getProgress() != -1) {
	            System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
	            return String.format("%.2f %%", submission.getProgress() * 100);
	        }
			if(submission.getStatus().isFailure()) {
				 return "任务失败。";
			}
			if(submission.getStatus().isRunning()) {
				 return "运行中。";
			}
			if(submission.getStatus().isRunning() == false) {
				 return submission.getStatus().toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return "";
    }
    
    //启动任务
    public static Map<String, String> startJob(Map<String,String> parMap) throws Exception{
    	Map<String, String> reMap = new HashMap<String,String>();
		try {
			client = new SqoopClient(parMap.get("sqoopHost"));
			//Job start
			MSubmission submission = client.startJob(parMap.get("jobName"));
			System.out.println("Job Submission Status : " + submission.getStatus());
			/*
			 * if (submission.getStatus().isRunning() && submission.getProgress() != -1) {
			 * System.out.println("Progress : " + String.format("%.2f %%",
			 * submission.getProgress() * 100)); return String.format("%.2f %%",
			 * submission.getProgress() * 100); }
			 */

			System.out.println("Hadoop job id :" + submission.getExternalJobId());
			System.out.println("Job link : " + submission.getExternalLink());
			
			reMap.put("hadoopJobId", submission.getExternalJobId());
			reMap.put("hadoopJobLink", submission.getExternalLink());
			
			Counters counters = submission.getCounters();
			if (counters != null) {
			    System.out.println("Counters:");
			    for (CounterGroup group : counters) {
			        System.out.print("\t");
			        System.out.println(group.getName());
			        for (Counter counter : group) {
			            System.out.print("\t\t");
			            System.out.print(counter.getName());
			            System.out.print(": ");
			            System.out.println(counter.getValue());
			        }
			    }
			}
			reMap.put("status","ok");
		} catch (Exception e) {
			e.printStackTrace();
			reMap.put("status","fail");
		}
		return reMap;
    }
    
    //主程序
    public static void main(String[] args) {
        // 注意sqoop 后面有个 /,如果没有会报下面的错,非常诡异
        // Exception: org.apache.sqoop.common.SqoopException Message: CLIENT_0004:Unable to find valid Kerberos ticket cache (kinit)
        //String url = "http://10.9.57.158:12000/sqoop/";
        
       // System.out.println(client);
    	System.out.println(String.format("%.2f %%", 0.3 * 100));
        //MLink mysqlLink = createMysqlLink();
        //MLink hdfsLink = createHdfsLink();
        // 先把数据导入 hdfs
        //startJob(createMysql2HdfsJob(mysqlLink, hdfsLink));
        // 然后再把数据导回 mysql
      //  startJob(createHdfs2MysqlJob(hdfsLink, mysqlLink));

    }
}
