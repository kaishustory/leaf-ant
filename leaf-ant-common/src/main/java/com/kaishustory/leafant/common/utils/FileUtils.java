/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.kaishustory.leafant.common.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


/**
 * <p>
 * Description: 文件操作工具类
 * </p>
 *
 * @author liguoyang
 * @version 1.0
 *
 *          <p>
 *          History:
 *
 *          Date Author Version Description
 *          --------------------------------------
 *          ------------------------------------------- 2015-3-30 下午5:53:37
 *          liguoyang 1.0 To create
 *          </p>
 * @since
 * @see
 */
public class FileUtils {

	/**
	 * 默认编码
	 */
	private final static String DEFAULT_ENCODE = "UTF-8";

	/**
	 * 保存文本文件
	 *
	 * @param filePath
	 *            文件地址(包含名词)
	 * @param text
	 *            文件内容
	 * @param append
	 *            是否继续写入
	 */
	public static void saveText(String filePath, List<String> text,
			boolean append) {
		saveText(filePath, text, DEFAULT_ENCODE, append);
	}

	/**
	 * 保存文本文件（自动补全路径）
	 *
	 * @param filePath
	 *            文件地址(包含名词)
	 * @param text
	 *            文件内容
	 * @param append
	 *            是否继续写入
	 */
	public static void saveTextAs(String filePath, List<String> text,
			boolean append) {
		saveText(setServerPath(filePath), text, DEFAULT_ENCODE, append);
	}

	/**
	 * 保存文本文件
	 *
	 * @param filePath
	 *            文件地址(包含名词)
	 * @param text
	 *            文件内容
	 */
	public static void saveText(String filePath, List<String> text) {
		saveText(filePath, text, DEFAULT_ENCODE, false);
	}

	/**
	 * 保存文本文件（自动补全路径）
	 *
	 * @param filePath
	 *            文件地址(包含名词)
	 * @param text
	 *            文件内容
	 */
	public static void saveTextAs(String filePath, List<String> text) {
		saveText(setServerPath(filePath), text, DEFAULT_ENCODE, false);
	}

	/**
	 * 保存文本文件
	 *
	 * @param filePath
	 *            文件地址(包含名词)
	 * @param text
	 *            文件内容
	 * @param encode
	 *            编码
	 * @param append
	 *            是否继续写入
	 */
	public static void saveText(String filePath, List<String> text,
			String encode, boolean append) {
		try {
			// 检查父级是否存在,不存在创建目录
			createParentDir(filePath);

			FileOutputStream fileOutputStream = new FileOutputStream(filePath,
					append);
			for (String line : text) {
				if (line == null) {
					continue;
				}
				fileOutputStream.write(line.getBytes(encode));
				fileOutputStream.write("\n".getBytes(encode));
			}
			fileOutputStream.close();
			Log.info("文件: " + filePath + ",保存成功!");
		} catch (FileNotFoundException e) {
			Log.error("文件: " + filePath + ",保存失败!", e);
		} catch (IOException e) {
			Log.error("文件: " + filePath + ",保存失败!", e);
		}
	}

	/**
	 * 保存文本文件
	 *
	 * @param filePath
	 *            文件地址(包含名词)
	 * @param doc
	 *            文件内容
	 */
	public static void saveText(String filePath, String doc) {
		try {
			// 检查父级是否存在,不存在创建目录
			createParentDir(filePath);

			FileOutputStream fileOutputStream = new FileOutputStream(filePath);
			fileOutputStream.write(doc.getBytes("UTF-8"));
			fileOutputStream.write("\n".getBytes("UTF-8"));
			fileOutputStream.close();
			Log.info("文件: " + filePath + ",保存成功!");
		} catch (FileNotFoundException e) {
			Log.error("文件: " + filePath + ",保存失败!", e);
		} catch (IOException e) {
			Log.error("文件: " + filePath + ",保存失败!", e);
		}
	}

	/**
	 * 读取文本文件（自动补全路径）
	 *
	 * @param path
	 *            文件地址
	 */
	public static List<String> readTextAs(String path) {
		return readText(setServerPath(path), DEFAULT_ENCODE);
	}

	/**
	 * 读取文本文件
	 *
	 * @param path
	 *            文件地址
	 */
	public static List<String> readText(String path) {
		return readText(path, DEFAULT_ENCODE);
	}

	/**
	 * 读取文本文件
	 *
	 * @param path
	 *            文件地址
	 * @param encode
	 *            编码
	 */
	public static List<String> readText(String path, String encode) {
		try {
			if (path == null) {
				return null;
			}
			// 读取路径
			File file = new File(path);
			// 路径为目录,读取目录下全部文件
			if (file.isDirectory()) {
				List<String> allft = new ArrayList<String>();
				for (File f : file.listFiles()) {
					List<String> ft = readText(f.getPath());
					allft.addAll(ft);
				}
				return allft;
			}
			// 路径为文件,读取文件
			// 只读取txt文件
			if (file.isFile() /*
							 * && (file.getName().endsWith(".txt") ||
							 * file.getName().endsWith(".dic"))
							 */) {
				// 文件列表
				List<String> docs = new ArrayList<String>();
				// 文件读取
				BufferedReader br = new BufferedReader(new InputStreamReader(
						new FileInputStream(file), encode));
				// 读取行内容
				String line = br.readLine();
				while (line != null) {
					// 注释过滤
					if (line != null && line.length() > 0
							&& line.startsWith("/*") == false) {
						docs.add(line);
					}
					line = br.readLine();
				}
				br.close();
				return docs;
			}
			return new ArrayList<String>(0);
		} catch (IOException e) {
			Log.error("文件: " + path + ",读取失败!", e);
			return null;
		}
	}

	/**
	 * 读取文本文件
	 *
	 * @param file
	 *            文件
	 */
	public static List<String> readText(File file) {
		return readText(file.getPath());
	}

	/**
	 * 读取文本文件
	 *
	 * @param file
	 *            文件
	 * @param encode
	 *            编码
	 */
	public static List<String> readText(File file, String encode) {
		return readText(file.getPath(), encode);
	}

	/**
	 * 读取SQL
	 * @param path 相对路径
	 * @return SQL
	 */
	public static String readSQLas(String path){
		List<String> file = readTextAs(path);
		StringBuffer sql = new StringBuffer();
		for (int i = 0; i < file.size(); i++) {
			if(i>0){
				sql.append(" ");
			}
			String line = file.get(i);
			//注释提取出
			if(line.indexOf("--")==0){
				line = "";
			}else if(line.indexOf("--")!=-1){
				line = line.substring(0, line.indexOf("--"));
			}
			sql.append(line);
		}
		return sql.toString();
	}

	/**
	 * 读取SQL
	 * @param path 绝对路径
	 * @return SQL
	 */
	public static String readSQL(String path){
		List<String> file = readText(path);
		StringBuffer sql = new StringBuffer();
		for (int i = 0; i < file.size(); i++) {
			if(i>0){
				sql.append(" ");
			}
			String line = file.get(i);
			//注释提取出
			if(line.indexOf("--")==0){
				line = "";
			}else if(line.indexOf("--")!=-1){
				line = line.substring(0, line.indexOf("--"));
			}
			sql.append(line);
		}
		return sql.toString();
	}

	/**
	 * 保存对象为文件
	 *
	 * @param path
	 *            文件位置
	 * @param obj
	 *            对象
	 */
	public static void saveObjectAs(String path, Object obj) {
		try {
			// 输出流保存的文件,ObjectOutputStream能把Object输出成Byte流
			ObjectOutputStream oos = new ObjectOutputStream(
					new FileOutputStream(setServerPath(path)));
			oos.writeObject(obj);
			oos.flush(); // 缓冲流
			oos.close(); // 关闭流
		} catch (IOException e) {
			Log.error("保存文件流发生异常！", e);
		}
	}

	/**
	 * 读取文件为对象
	 *
	 * @param path
	 *            文件位置
	 * @return 对象
	 */
	public static <T> T readObjectAs(String path) {
		try {
			@SuppressWarnings("resource")
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
					setServerPath(path)));
			@SuppressWarnings("unchecked")
			T t = (T) ois.readObject();
			return t;
		} catch (IOException e) {
			Log.error("读取文件流发生异常！", e);
		} catch (ClassNotFoundException e) {
			Log.error("没找到文件发生异常！", e);
		}
		return null;
	}

	/**
	 * 获取所有文件
	 *
	 * @param path
	 *            文件夹地址
	 */
	public static File[] getFiles(String path) {
		try {
			if (path == null) {
				return null;
			}
			// 读取路径
			File file = new File(path);
			// txt
			// 路径为目录,读取目录下全部文件
			if (file.isDirectory()) {
				// 只有后缀为.txt的文件才会被列举出来形成数组
				File[] files = file.listFiles(new FileFilter() {

					@Override
					public boolean accept(File pathname) {
						String filename = pathname.getName().toLowerCase();
						if (filename.endsWith(".txt")) {
							return true;
						} else {
							return false;
						}
					}
				});
				return files;
			}
			if (file.exists()) {
				File[] f = { file };
				return f;
			}

		} catch (Exception exception) {
			exception.printStackTrace();
		}
		return new File[0];
	}

	/**
	 * 检查文件是否存在
	 *
	 * @param path
	 *            路径
	 * @return 是否存在
	 */
	public static boolean exists(String path) {
		File file = new File(path);
		return file.exists();
	}

	/**
	 * 检查父级是否存在,不存在创建目录
	 *
	 * @param path
	 *            文件路径
	 */
	private static void createParentDir(String path) {
		// 父级目录是否存在,不存在创建目录
		if (new File(path).getParent() != null) {
			File parent = new File(new File(path).getParent());
			if (parent.exists() == false) {
				// 继续检查父级
				createParentDir(parent.getPath());
				// 创建目录
				parent.mkdir();
			}
		}
	}

	/**
	 * 删除文件（自动补全路径）
	 *
	 * @param path
	 *            文件地址
	 */
	public static void delFileAs(String path) {
		delFile(FileUtils.setServerPath(path));
	}

	/**
	 * 删除文件
	 *
	 * @param path
	 *            文件地址
	 */
	public static void delFile(String path) {
		new File(path).delete();
	}

	/**
	 * 递归删除文件夹
	 *
	 * @param dir
	 * @return
	 */
	public static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			// 递归删除目录中的子目录下
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		// 目录此时为空，可以删除
		return dir.delete();
	}

	/**
	 * 创建文件夹
	 *
	 * @param path
	 */
	public static void createDirs(String path) {
		File file = new File(path);
		// 如果文件夹不存在则创建
		if (!file.exists() && !file.isDirectory()) {
			file.mkdirs();
		}
	}

	/**
	 * 修改文件地址为服务器地址
	 *
	 * @param path
	 *            文件地址
	 * @return 服务器文件地址
	 */
	public static String setServerPath(String path) {
		String dir = FileUtils.class.getResource("").getPath();
		if(dir.indexOf("/WEB-INF")!=-1){
			dir = dir.substring(0, dir.indexOf("WEB-INF"));
		}
		// 判断系统
		String os = System.getProperty("os.name");
		if (os.toLowerCase().indexOf("windows") != -1) {
			// windows去除前面的斜杠
			if (dir.startsWith("/") && dir.indexOf(":") == 2) {
				dir = dir.substring(1);
			}
		}
		return dir + path;
	}

	/**
	 * 修改配置地址为服务器地址
	 *
	 * @param path
	 *            文件地址
	 * @return 服务器文件地址
	 */
	public static String setServerConfPath(String path) {
		String dir = FileUtils.class.getResource("").getPath();
		dir = dir.substring(0, dir.indexOf("classes") + "classes".length() + 1);
		// 判断系统
		String os = System.getProperty("os.name");
		if (os.toLowerCase().indexOf("windows") != -1) {
			// windows去除前面的斜杠
			if (dir.startsWith("/") && dir.indexOf(":") == 2) {
				dir = dir.substring(1);
			}
		}
		return dir + path;
	}


}
