package iniFile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.dtools.ini.BasicIniFile;
import org.dtools.ini.IniFile;
import org.dtools.ini.IniFileReader;
import org.dtools.ini.IniFileWriter;
import org.dtools.ini.IniItem;
import org.dtools.ini.IniSection;

public class ReadUpdateIni {
	public static void main(String[] args) throws Exception {
	}

	public static void writeFile(String filePath, String sets) throws IOException {
		FileWriter fw = new FileWriter(filePath);
		PrintWriter out = new PrintWriter(fw);
		out.write(sets);
		out.println();
		fw.close();
		out.close();
	}

	public static IniSection readIniFile(String IniPath, String iniSection) throws Exception {
		IniFile iniFile = new BasicIniFile();
		IniFileReader reader = new IniFileReader(iniFile, new File(IniPath));
		reader.read();
		IniSection sec = iniFile.getSection(iniSection); // 得到指定部分
		return sec;
	}

	public void readUpdateIniFile() {
		IniFile iniFile = new BasicIniFile();
		File file = new File("D:\\test\\test.ini");
		IniFileReader rad = new IniFileReader(iniFile, file);
		IniFileWriter wir = new IniFileWriter(iniFile, file);
		try {
			// 读取item
			rad.read();
			// IniSection iniSection = iniFile.getSection(0);
			IniSection iniSection = iniFile.getSection("sect");
			IniItem iniItem = iniSection.getItem("name");
			String name = iniItem.getValue();
			iniItem.setValue("Konan");
			iniSection.addItem(iniItem);
			iniFile.addSection(iniSection);
			wir.write();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
