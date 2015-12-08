package com.zaq;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

import com.qiniu.common.QiniuException;
import com.qiniu.http.Response;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.UploadManager;
import com.qiniu.util.Auth;

public class MainRun {
	static String uploadToken = null;//牛哥的上传令牌
	static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static String bakZipFilePath=null;// 压缩的备份文件全路径
	static String fileName7qZip=null;//上传到牛哥空间的唯一文件名
	static String fileName7qPer=null;//上一个备份的名字
	static Auth auth=null;//牛哥授权
	static String bakRootPath="/usr/local/mysqlBak";//备份文件 的根目录
	static Properties props= new Properties();
	static final String perConf="perFile.properties";//记录上一个上传到牛哥的资源文件 
	static final String perFileNameKey="perFileName";//资源文件的Key
	//加载上次程序最后上传的文件名
	static{
		InputStream is=null;
		try {
			is = new BufferedInputStream(MainRun.class.getClassLoader().getResourceAsStream(perConf));
			props.load(is);
			fileName7qPer=props.getProperty(perFileNameKey);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				is.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 初始化
	 */
	private static void init() {
		try {
			auth = Auth.create("***-vcAkvS9pv", "****");
			uploadToken = auth.uploadToken("mysqlbak");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		System.out.println(MainRun.class.getClassLoader().getResource(perConf).getFile());
		
		while (true) {
			try {
				if (null == uploadToken) {
					init();
				}
				if (null != uploadToken) {
					
					if(null==bakZipFilePath){
						getMysqlBak();
					}
					
					UploadManager uploadManager = new UploadManager();
					uploadManager.put(new File(bakZipFilePath), fileName7qZip, uploadToken);
					bakZipFilePath=null;
					
					//删除7牛上一个备份
					if(null!=fileName7qPer){
						try{
							BucketManager bucketManager = new BucketManager(auth);
							bucketManager.delete("mysqlbak", fileName7qPer);
						}catch(Exception e){
							e.printStackTrace();
							System.out.println("7牛上一个备份文件"+fileName7qPer+"删除失败！");
						}
						//删除本地上一个备份
						try{
							File perBakFile=new File(bakRootPath+File.separator+fileName7qPer);
							if(perBakFile.exists()){
								perBakFile.delete();
							}
						}catch(Exception e){
							e.printStackTrace();
							System.out.println("本地上一个备份文件"+fileName7qPer+"删除失败！");
						}
					}
					
					
					fileName7qPer=fileName7qZip;
					
					props.setProperty(perFileNameKey, fileName7qPer);
					
					//保存上一个备份的文件名到时资源文件保存
					savePerName();
					
					System.out.println("文件"+fileName7qZip+"上傳成功！");
					// 一小时后再上传
					try {
						Thread.sleep(60 * 60 * 1000);
					} catch (InterruptedException e1) {}
				}else{
					// 100秒后重试
					sleep();
				}
				
			} catch (QiniuException e) {
				e.printStackTrace();

				Response r = e.response;
				// 请求失败时简单状态信息
				System.out.println(r.toString());
				try {
					// 响应的文本信息
					System.out.println(r.bodyString());
				} catch (QiniuException e1) {
				}

				// 100秒后重试
				sleep();
			} catch (Exception e) {
				e.printStackTrace();
				// 100秒后重试
				sleep();
			}

		}
	}

	/**
	 * 记录上一个上传的文件名
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void savePerName() throws FileNotFoundException, IOException {
		// 文件输出流 
		try {
			FileOutputStream fos = new FileOutputStream(MainRun.class.getClassLoader().getResource(perConf).getFile()); 
			// 将Properties集合保存到流中 
			props.store(fos, "Copyright (c) By zaq369cde "); 
			fos.close();// 关闭流 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 暫停100秒
	 */
	private static void sleep() {
		uploadToken = null;
		try {
			Thread.sleep(100 * 1000);
		} catch (InterruptedException e1) {}
	}

	/**
	 * 生成mysql備份文件
	 */
	private static void getMysqlBak(){
		InputStream in=null;
		 InputStreamReader xx=null;
		 BufferedReader br=null;
		 OutputStreamWriter writer=null;
		 FileOutputStream fout=null;
		 final String fileTmp="mysqlBak-"+sdf.format(new Date());
		 fileName7qZip=fileTmp+".zip";
		 bakZipFilePath=bakRootPath+File.separator+fileName7qZip;
		 final String fileSqlPathTmp=bakRootPath+File.separator+fileTmp+".sql";
		try {
		   Runtime rt = Runtime.getRuntime();
		   // 调用 调用mysql的安装目录的命令
		   Process child = rt
		     .exec("mysqldump -h localhost -uzaqzaq -pzaqzaq  b3log");
		   // 设置导出编码为utf-8。这里必须是utf-8
		   // 把进程执行中的控制台输出信息写入.sql文件，即生成了备份文件。注：如果不对控制台信息进行读出，则会导致进程堵塞无法运行
		   in = child.getInputStream();// 控制台的输出信息作为输入流
		   xx = new InputStreamReader(in, "utf-8");
		   // 设置输出流编码为utf-8。这里必须是utf-8，否则从流中读入的是乱码
		   String inStr;
		   StringBuffer sb = new StringBuffer("");
		   String outStr;
		   // 组合控制台输出信息字符串
		   br = new BufferedReader(xx);
		   while ((inStr = br.readLine()) != null) {
		    sb.append(inStr + "\r\n");
		   }
		   outStr = sb.toString();
		   // 要用来做导入用的sql目标文件：
		   File bakFile=new File(fileSqlPathTmp);
		   bakFile.createNewFile();
		   fout = new FileOutputStream(bakFile);
		   writer = new OutputStreamWriter(fout, "utf-8");
		   writer.write(outStr);
		   writer.flush();
		  
		   System.out.println("");
		  } catch (Exception e) {
			  e.printStackTrace();
			  bakZipFilePath=null;
		  }finally{
			  try {
				   in.close();
				   xx.close();
				   br.close();
				   writer.close();
				   fout.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		  }
		
		zipBakSql(fileSqlPathTmp);
		
	}
	/**
	 * 压缩备份的sql文件
	 * @param fileSqlTmp
	 */
	private static void zipBakSql(final String fileSqlPathTmp) {
		File fileSqlTmp=null;
		try {
			ZipFile zipFile = new ZipFile(bakZipFilePath);
			ArrayList filesToAdd = new ArrayList();
			fileSqlTmp=new File(fileSqlPathTmp);
			filesToAdd.add(fileSqlTmp);

			ZipParameters parameters = new ZipParameters();  
			parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);  
			parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);

			//设置密码
			parameters.setEncryptFiles(true);  
			parameters.setEncryptionMethod(Zip4jConstants.ENC_METHOD_STANDARD);   
			parameters.setPassword("mysqlbakpassword");  
			zipFile.addFiles(filesToAdd, parameters);
			
		} catch (ZipException e) {
			e.printStackTrace();
			 bakZipFilePath=null;
		}
		
		//删除临时导出的sql文件 ，只留备份
		try {
			fileSqlTmp.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
