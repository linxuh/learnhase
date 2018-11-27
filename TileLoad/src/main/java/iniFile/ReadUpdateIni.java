package iniFile;

import java.io.File;
import java.io.IOException;

import org.dtools.ini.BasicIniFile;
import org.dtools.ini.IniFile;
import org.dtools.ini.IniFileReader;
import org.dtools.ini.IniFileWriter;
import org.dtools.ini.IniItem;
import org.dtools.ini.IniSection;

public class ReadUpdateIni {
	public static void main(String[] args) throws Exception {
		ReadUpdateIni readUpdateIni = new ReadUpdateIni();
		IniSection sec = readUpdateIni.readIniFile("C:\\Users\\Administrator\\Desktop\\example.ini", "LoadData");
		System.out.println(sec.getItem("Scope").getValue());

	}

	public static IniSection readIniFile(String IniPath, String iniSection) throws Exception {
		IniFile iniFile = new BasicIniFile();
		IniFileReader reader = new IniFileReader(iniFile, new File(IniPath));
		reader.read();
		IniSection sec = iniFile.getSection(iniSection); // 得到指定部分
		// System.out.println(sec.getNumberOfItems());
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
