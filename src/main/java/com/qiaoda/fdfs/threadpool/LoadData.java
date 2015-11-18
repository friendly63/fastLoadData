package com.qiaoda.fdfs.threadpool;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.csource.common.MyException;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.StorageClient1;
import org.csource.fastdfs.StorageServer;
import org.csource.fastdfs.TrackerClient;
import org.csource.fastdfs.TrackerGroup;
import org.csource.fastdfs.TrackerServer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qiaoda.fdfs.fastLoadData.RedisPoolBean;
import com.qiaoda.fdfs.fastLoadData.SpringContextUtils;
import com.qiaoda.mongo.MongoConnection;



public class LoadData implements Callable<String>{
	
	private static final Logger logger = Logger.getLogger(LoadData.class);
	StorageClient1 client = null;
	StorageServer storageServer = null;
    TrackerServer trackerServer = null;
    MongoConnection con = null;
    TrackerClient tracker = null;
	private final static HashMap<String,Object> fdfsMap = new HashMap<String,Object>(); 
	
	public String call() {
		initConf();
		JedisPool pool = (JedisPool)SpringContextUtils.getBean("jedisPool");
	    String jedis = getRedisString(pool);
	    logger.info("redis:"+jedis);
	    saveToFDFSandMongo(jedis);
	    return "";
	}
	/**
	 * init FastDFS 
	 */
	private void initConf(){
		try {
			if(fdfsMap.isEmpty()){
				logger.info("init fastdfs conf");
				InputStream in = this.getClass().getClassLoader().getResourceAsStream("fdfs-client.properties");
				Properties p = new Properties();
				p.load(in);
				String tgStr = p.getProperty("fdfs.tracker_server");
				String port = p.getProperty("fdfs.tracker_port");
				String con_timeout = p.getProperty("fdfs.connect_timeout");
				String network_timeout =p.getProperty("fdfs.network_timeout");
				String steal_token =p.getProperty("fdfs.http.anti_steal_token");
				String charset = p.getProperty("fdfs.charset");
				String wait_queue = p.getProperty("fdfs.wait_queue");
				String new_files = p.getProperty("fdfs.new_files");

				fdfsMap.put("ip", tgStr);
				fdfsMap.put("port", port);
				fdfsMap.put("connect_timeout", con_timeout);
				fdfsMap.put("network_timeout", network_timeout);
				fdfsMap.put("steal_token", steal_token);
				fdfsMap.put("charset", charset);
				fdfsMap.put("wait_queue", wait_queue);
				fdfsMap.put("new_files", new_files);

				InetSocketAddress[] trackerServers = new InetSocketAddress[1];
				trackerServers[0] = new InetSocketAddress(fdfsMap.get("ip").toString(), Integer.parseInt(fdfsMap.get("port").toString()));
				ClientGlobal.setG_tracker_group(new TrackerGroup(trackerServers));
				// 连接超时的时限，单位为毫秒
				ClientGlobal.setG_connect_timeout(Integer.parseInt(fdfsMap.get("connect_timeout").toString()));
				// 网络超时的时限，单位为毫秒
				ClientGlobal.setG_network_timeout(Integer.parseInt(fdfsMap.get("network_timeout").toString()));
				ClientGlobal.setG_anti_steal_token(Boolean.getBoolean(fdfsMap.get("steal_token").toString()));
				// 字符集
				ClientGlobal.setG_charset(fdfsMap.get("charset").toString());
				ClientGlobal.setG_secret_key(null);
				
				tracker = new TrackerClient();
				trackerServer = tracker.getConnection();
				storageServer = tracker.getStoreStorage(trackerServer);
				client = new StorageClient1(trackerServer, storageServer);
//				con = new MongoConnection();
				
				in.close();
				logger.info("ip:"+fdfsMap.get("ip").toString()+",port:"+fdfsMap.get("port").toString());
			}
			
		} catch (IOException e) {	
			e.printStackTrace();
			logger.error("init fail, com.qiaoda.fdfs.FDFSLoadData.initConf() "+e);
		}
	}
	
	/**
	 * single record fdfsFilePath   upload file to fdfs server,return filePath in storage server path
	 * @param fileName 
	 * @param filePath 
	 * @return return fdfsFilePath
	 */
	public String uploadFile1(String fileName, String filePath){
		String fileId = null ;
		try {
        NameValuePair[] metaList = new NameValuePair[2];
        metaList[0] = new NameValuePair("fileName", fileName);
        metaList[1] = new NameValuePair("filePath", filePath);
        logger.info("开始上传");
		fileId = client.upload_file1(filePath, null, metaList);
		logger.info("上传结束fileId "+fileId);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("upload fail,com.qiaoda.fdfs.FDFSLoadData.uploadFile1(): "+e);
		} catch (MyException e) {
			e.printStackTrace();
			logger.error("upload fail, com.qiaoda.fdfs.FDFSLoadData.uploadFile1(): "+e);
		}
		return fileId;
	}
	
	
	/**
	 * upload to fastdfs
	 * @param jsonString
	 * @return true success false fail
	 */
	public void saveToFDFSandMongo(String jsonString){
		if(jsonString==null)
			return ;
		JSONObject json = JSON.parseObject(jsonString);
		String filePath = json.getString("filename");
		filePath = "D:/FastDFS/aa.txt";
		String from_id = json.getString("from_id");
		String user_id = json.getString("user_id");
		File f = new File(filePath);
		if(f.isFile()){
			String filename =filePath.substring(filePath.lastIndexOf("/")+1);
			long uptime = System.currentTimeMillis();
			logger.info("filename:"+filename+",filePaht:"+filePath);
			String fileId=uploadFile1(filename,filePath);
			logger.info("================上传耗时："+(System.currentTimeMillis()-uptime));
			if(fileId==null){
				rpush(fdfsMap.get("wait_queue").toString(),jsonString);
				logger.info("upload fail,com.qiaoda.fdfs.FDFSLoadData.saveToFDFSandMongo(),right push waitqueue filePath:"+filePath);
			}else{
//				long deltime = System.currentTimeMillis();
//				boolean del = f.delete();
//				if(del){
//				
//					logger.info("upload fdfs success,del local file success");
//				}else{
//					f.delete();
//					logger.info("upload fdfs success,del local file fail ,filePath:" + filePath);
//				}
//				logger.info("==============删除耗时："+(System.currentTimeMillis()-deltime));
				long redistime = System.currentTimeMillis();
				RedisPoolBean redisBean = (RedisPoolBean)SpringContextUtils.getBean("redisPoolBean");
				redisBean.setFilename(fileId);
				redisBean.setFrom_id(from_id);
				redisBean.setFn2(filePath);
				redisBean.setUser_id(user_id);
				String redisJson = JSON.toJSONString(redisBean);
				pushNewQueue(fdfsMap.get("new_files").toString(),redisJson);
				logger.info("==============生成新队列耗时："+(System.currentTimeMillis()-redistime));
				logger.info("upload success ,push new queue new_files,new json: "+redisJson);
				
				long mogotime = System.currentTimeMillis();
				saveToMongo(fileId,filePath,from_id,user_id);
				logger.info("==============生成mogo耗时："+(System.currentTimeMillis()-mogotime));
			}
		}else{
			logger.info("upload fail com.qiaoda.fdfs.FDFSLoadData.saveToFDFSandMongo() ,file not exits, filePath:"+filePath);
			rpush(fdfsMap.get("wait_queue").toString(),jsonString);
			logger.info("rpush waitqueue "+filePath);
		}
		
	}
	/**
	 * 
	 * @param fdfsPath fastdfs filepath
	 * @param qfilePath queue filepath
	 * @param fmId from_id fen
	 * @param userId 
	 */
	public void saveToMongo(String fdfsPath,String qfilePath,String fmId,String userId){
		Document doc = new Document();
		
		
		doc.append("_id", fdfsPath).append("fn", qfilePath).append("fm_id", fmId).append("hash", "").append("tp", System.currentTimeMillis()).append("user_id", userId).append("stat", "");
		con.insertMg(doc);
		
		logger.info("save mongodb success filepath:"+qfilePath);
	}

	private String getRedisString(JedisPool pool) {
		Jedis jedis = null;
		String redisStr=null;
		
		try{
			jedis =  pool.getResource();
			redisStr = jedis.lpop(fdfsMap.get("wait_queue").toString());
		}catch(Exception e){
			logger.error("lpop redis error: "+e);
		}finally{
			jedis.close();
		}
		return redisStr;
	}

	private long getRedisLength(JedisPool pool) {
		long jedisLens=0;
		Jedis jedis = null;
		long gettime = System.currentTimeMillis();
		try{
			jedis =  pool.getResource();
			jedisLens = jedis.llen(fdfsMap.get("wait_queue").toString());
		}catch(Exception e){
			logger.error("get jedisLens error: "+e);
		}finally{
			jedis.close();
			logger.info("获取redisString 耗时："+(System.currentTimeMillis()-gettime));
		}
		
	
		return jedisLens;
	}
	/**
	 * fail upload ,right push queue
	 * @param keys
	 * @param jsonObject
	 */
	public void rpush(String keys ,String jsonObject){
		JedisPool pool = (JedisPool)SpringContextUtils.getBean("jedisPool");
        Jedis jedis =  pool.getResource();  
        jedis.rpush(keys, jsonObject);
        jedis.close();
	}
	/**
	 * upload success,create new jsonstring,left push new queue
	 * @param keys
	 * @param jsonObject
	 */
	public void pushNewQueue(String keys,String jsonObject){
		JedisPool pool = (JedisPool)SpringContextUtils.getBean("jedisPool");
        Jedis jedis =  pool.getResource();  
        jedis.lpush(keys, jsonObject);
        jedis.close();
	}
	
	/**
	 * close fdfs connection
	 */
	public void close(){
	    if(storageServer!=null){
	    	try {
				storageServer.close();
			} catch (IOException e) {
				logger.info("close storageServer fail "+e);
				e.printStackTrace();
			}
	    }
	    if(trackerServer!=null){
	    	try {
				trackerServer.close();
			} catch (IOException e) {
				logger.info("close trackerServer fail "+e);
				e.printStackTrace();
			}
	    }
	    if(con != null){
	    	con.close();
	    }
	}
	

	
	
}
