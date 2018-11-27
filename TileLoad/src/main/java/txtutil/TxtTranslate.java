package txtutil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TxtTranslate {
	public static String[] readTxt(String path) throws IOException {
		StringBuilder result = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));// 构造一个BufferedReader类来读取文件
			result.append(br.readLine());
			String s = null;
			while ((s = br.readLine()) != null) {// 使用readLine方法，一次读一行
				result.append(" " + s);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		String[] parameters = result.toString().split(" ");
		return parameters;
	}

	public static void main(String[] args) throws IOException {
		readTxt("D:\\test.txt");
	}
}
