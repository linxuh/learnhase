package txtutil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TxtTranslate {
	public static void writeTxt(String path, int level, double minlat, double maxlat, double minlon, double maxlon)
			throws IOException {
		String html = "<!DOCTYPE html>\r\n" + "<html lang=\"en\" >\r\n" + "<head>\r\n"
				+ "    <meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\">\r\n" + "\r\n"
				+ "    <link rel=\"stylesheet\" href=\"./css/ol.css\" type=\"text/css\">\r\n"
				+ "    <link rel=\"stylesheet\" href=\"./css/div.css\" type=\"text/css\">\r\n" + "    <style>\r\n"
				+ "        .map {\r\n" + "            height: 300px;\r\n" + "            width: 85%;\r\n"
				+ "        }\r\n" + "    </style>\r\n" + "    <script src=\"./js/ol.js\"></script>\r\n"
				+ "    <script src=\"./js/jquery-3.3.1.js\"></script>\r\n" + "    <title>OpenLayers example</title>\r\n"
				+ "\r\n" + "</head>\r\n" + "<body>\r\n" + "<div id=\"div1\" style=\"width: 15%;float:left\">\r\n"
				+ "    <form id=\"formTag\"  class=\"form-horizontal\" enctype=\"multipart/form-data\">\r\n"
				+ "        level:<input type=\"text\" id=\"zoom\"  style=\"width:120px\"	value=" + level
				+ "><br/>\r\n" + "        <ul></ul>\r\n"
				+ "        minLon: <input type=\"text\" id=\"minX\" style=\"width:120px\"value=" + minlon + "><br/>\r\n"
				+ "        <ul></ul>\r\n"
				+ "        minLat: <input type=\"text\" id=\"minY\" style=\"width:120px\"value=" + minlat + "><br/>\r\n"
				+ "        <ul></ul>\r\n"
				+ "        maxLon: <input type=\"text\" id=\"maxX\" style=\"width:120px\"value=" + maxlon + "><br/>\r\n"
				+ "        <ul></ul>\r\n"
				+ "        maxLat: <input type=\"text\" id=\"maxY\" style=\"width:120px\"value=" + maxlat + "><br/>\r\n"
				+ "        <ul>\r\n"
				+ "        <button id=\"btn\" class=\"btn btn-primary\" type=\"button\" onclick=\"choal01()\" >查询</button>\r\n"
				+ "        </ul>\r\n" + "    </form>\r\n"
				+ "    <button id=\"btn_min\" onmousedown=\"min_center()\">lower-left corner</button>\r\n"
				+ "        <ul></ul>\r\n"
				+ "    <button id=\"btn_max\" onmousedown=\"max_center()\">upper-right corner</button>\r\n"
				+ "</div>\r\n" + "<div class=\"ol-full-screen\">\r\n"
				+ "    <div id=\"map\" style=\"width:85%;float:right\" ></div>\r\n" + "</div>\r\n" + "\r\n"
				+ "<script type=\"text/javascript\">\r\n" + "    var minCenter,maxCenter;\r\n"
				+ "    var view;var map;\r\n"
				+ "    var center1=ol.proj.transform([0, 0], 'EPSG:4326', 'EPSG:3857');\r\n"
				+ "    view= new ol.View({\r\n" + "        center:center1,\r\n" + "        zoom:2\r\n" + "    });\r\n"
				+ "    map = new ol.Map({\r\n" + "        view:view,\r\n" + "        target: 'map'\r\n" + "    });\r\n"
				+ "    var index=0;\r\n" + "    function choal01() {\r\n" + "\r\n"
				+ "        var minX=parseFloat(document.getElementById(\"minX\").value);\r\n"
				+ "        var minY=parseFloat(document.getElementById(\"minY\").value);\r\n"
				+ "        var maxX=parseFloat(document.getElementById(\"maxX\").value);\r\n"
				+ "        var maxY=parseFloat(document.getElementById(\"maxY\").value);\r\n"
				+ "        var centerX=(minX+maxX)/2;\r\n" + "        var centerY=(minY+maxY)/2;\r\n" + "\r\n"
				+ "       var  center = ol.proj.transform([centerX, centerY], 'EPSG:4326', 'EPSG:3857');\r\n"
				+ "        minCenter= ol.proj.transform([minX, minY], 'EPSG:4326', 'EPSG:3857');\r\n"
				+ "        maxCenter=ol.proj.transform([maxX, maxY], 'EPSG:4326', 'EPSG:3857');\r\n"
				+ "        view.animate({\r\n" + "            center:center,\r\n"
				+ "            zoom:document.getElementById(\"zoom\").value\r\n" + "        });\r\n"
				+ "        index++;\r\n" + "        if (index>=1){\r\n" + "            map.addLayer(chaxunLayer);\r\n"
				+ "            map.addControl(mousePositionControl);\r\n" + "        } else {\r\n"
				+ "        map.addLayer(chaxunLayer);\r\n" + "        map.addControl(mousePositionControl);}\r\n"
				+ "    }\r\n" + "    function min_center(){\r\n" + "        view.animate({\r\n"
				+ "            center: minCenter,\r\n" + "            duration: 2000\r\n" + "        });\r\n"
				+ "    }\r\n" + "    function max_center(){\r\n" + "        view.animate({\r\n"
				+ "            center: maxCenter,\r\n" + "            duration: 2000\r\n" + "        });\r\n"
				+ "    }\r\n" + "    var mousePositionControl=new ol.control.MousePosition({\r\n"
				+ "        className:'mosuePosition',\r\n"
				+ "        coordinateFormat:ol.coordinate.createStringXY(2),\r\n"
				+ "        projection:'EPSG:4326',\r\n" + "        target:document.getElementById('myposition')\r\n"
				+ "    });\r\n" + "    var chaxunLayer = new ol.layer.Tile({\r\n"
				+ "        source: new ol.source.XYZ({\r\n"
				+ "            // 设置本地离线瓦片所在路径，由于例子里面只有一/WMTS/{z}/{x}/{y}.png'张瓦片，页面显示时就只看得到一张瓦片。\r\n"
				+ "            url: './result1/{z}_{y}_{x}[\\s\\S]*.png'\r\n" + "        })\r\n" + "    });\r\n"
				+ "\r\n" + "</script>\r\n" + "\r\n" + "</body>\r\n" + "</html>";
		try {
			// 创建文件对象
			File fileText = new File(path);
			// 向文件写入对象写入信息
			FileWriter fileWriter = new FileWriter(fileText);

			// 写文件
			fileWriter.write(html);
			// 关闭
			fileWriter.close();
		} catch (IOException e) {
			//
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		writeTxt("/var/www/wmts/tilequery.html", 6, 80, 89, 67, 800);
	}
}
