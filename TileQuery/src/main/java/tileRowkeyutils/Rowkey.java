package tileRowkeyutils;

import java.io.File;
import java.math.BigInteger;
import java.util.BitSet;

/**
 * Created by Linxu Han on 2018/11/05
 */
public class Rowkey {

	private static final String base32Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

	/**
	 * Get Z-order number
	 * 
	 * @param level:tile level
	 * @param number:number of row or column
	 * @param floor:minnumber of this level
	 * @param ceiling:maxnumber of this level
	 * @return BitSet
	 */
	public static BitSet getBits(int level, int number, double floor, double ceiling) {
		BitSet buffer = new BitSet(level);
		for (int i = 0; i < level; i++) {
			double mid = (floor + ceiling) / 2;
			if (number > mid) {
				buffer.set(i);
				floor = mid;
			} else {
				ceiling = mid;
			}
		}
		return buffer;
	}

	/**
	 * Get Z-order number
	 * 
	 * @param level:tile level
	 * @param number:number of lat or lon
	 * @param floor:minnumber of this level
	 * @param ceiling:maxnumber of this level
	 * @return BitSet
	 */
	public static BitSet getBits(int level, double number, double floor, double ceiling) {
		BitSet buffer = new BitSet(level);
		for (int i = 0; i < level; i++) {
			double mid = (floor + ceiling) / 2;
			if (number > mid) {
				buffer.set(i);
				floor = mid;
			} else {
				ceiling = mid;
			}
		}
		return buffer;
	}

	/**
	 * Get Row key of tile
	 * 
	 * @param level:tile level
	 * @param col: tile column
	 * @param row: tile row
	 * @return String
	 */
	public static String getRowkey(int level, int row, int col) {
		StringBuilder buffer = new StringBuilder();
		StringBuilder rowkey = new StringBuilder();
		String str = base32Encode(decimalToBinary(level));
		rowkey.append(str);
		// String str = String.format("%02d", level);
		if (row < (int) Math.pow(2, level) && col < (int) Math.pow(2, level)) {
			if (level == 0) {
				return rowkey.toString();
			} else {
				BitSet rowbits = getBits(level, row + 1, 0, (int) Math.pow(2, level));
				BitSet colbits = getBits(level, col + 1, 0, (int) Math.pow(2, level));
				for (int i = 0; i < level; i++) {
					buffer.append(colbits.get(i) ? '1' : '0');
					buffer.append(rowbits.get(i) ? '1' : '0');
				}
				rowkey.append(base32Encode(buffer.toString()));
				// System.out.print(buffer.toString() + ",");
			}
		} else {
			rowkey.append('!');
			System.out.println("wrong row number or wrong col number!");
		}
		return rowkey.toString();
	}

	/**
	 * Get Row key of tile
	 * 
	 * @param level:tile level
	 * @param col: tile column
	 * @param row: tile row
	 * @return String
	 */
	public static String getRowkey(String level, String row, String col) {
		return getRowkey(Integer.parseInt(level), Integer.parseInt(row), Integer.parseInt(col));
	}

	/**
	 * Get row number from a path name
	 * 
	 * @param rowFileName:filename of a tile
	 * @param level: tile level
	 * @return String
	 */
	public static String getRightRowNumber(File rowFileName, String level) {
		String row = rowFileName.getName().substring(0, rowFileName.getName().lastIndexOf("."));
		String realrow = String.valueOf(getRightRowNumber(Integer.parseInt(row), Integer.parseInt(level)));
		return realrow;
	}

	/**
	 * Transform the row number
	 * 
	 * @param row: tile row
	 * @param level: tile level
	 * @return Integer
	 */
	public static int getRightRowNumber(int row, int level) {
		int realrow = (int) Math.pow(2, level) - 1 - row;
		return realrow;
	}

	/**
	 * Decimal string conversion to binary string
	 * 
	 * @param str: Decimal string
	 * @return String
	 */
	public static String decimalToBinary(String str) {
		int DecimalNum = Integer.valueOf(str).intValue();
		String BinaryString = Integer.toBinaryString(DecimalNum);
		return BinaryString;
	}

	/**
	 * Decimal integer conversion to binary string
	 * 
	 * @param str: Decimal string
	 * @return String
	 */
	public static String decimalToBinary(int num) {
		String BinaryString = Integer.toBinaryString(num);
		return BinaryString;
	}

	public static String binaryToDecimal(String binarySource) {
		BigInteger bi = new BigInteger(binarySource, 2); // 转换为BigInteger类型
		return bi.toString(); // 转换成十进制
	}

	/**
	 * Encodes string num to Base32 String.
	 *
	 * @param int num to encode.
	 * @return Encoded num as a String.
	 *
	 */
	static public String base32Encode(String binary) {
		String modifiedBinary;
		if (binary.length() % 5 != 0) {
			int numOfAppend = 5 - binary.length() % 5;
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < numOfAppend; i++) {
				sb.append('0');
			}
			sb.append(binary);
			modifiedBinary = sb.toString();
		} else {
			modifiedBinary = binary;
		}

		StringBuilder buf = new StringBuilder();
		for (int i = 0; i < modifiedBinary.length(); i += 5) {
			String tmp = modifiedBinary.substring(i, i + 5);
			int base32Index = Integer.parseInt(new BigInteger(tmp, 2).toString());
			buf.append(base32Chars.charAt(base32Index));
		}
		// System.out.println("origin: " + binary + ", base32: " + buf.toString());
		return buf.toString();
	}

	public static int[] conLatLon2RowCol(int level, double lat, double lon) {
		StringBuilder latbuffer = new StringBuilder();
		StringBuilder lonbuffer = new StringBuilder();
		BitSet latbits = getBits(level, lat, -85.05112878, 85.05112878);
		BitSet lonbits = getBits(level, lon, -180, 180);
		for (int i = 0; i < level; i++) {
			latbuffer.append(latbits.get(i) ? '1' : '0');
			lonbuffer.append(lonbits.get(i) ? '1' : '0');
		}
		String row = binaryToDecimal(latbuffer.toString());
		String col = binaryToDecimal(lonbuffer.toString());
		return new int[] { Integer.parseInt(row), Integer.parseInt(col) };
	}

	public static int[] getTileNumber(int zoom, double lat, double lon) {
		int xtile = (int) Math.floor((lon + 180) / 360 * (1 << zoom));
		int ytile = (int) Math
				.floor((1 - Math.log(Math.tan(Math.toRadians(lat)) + 1 / Math.cos(Math.toRadians(lat))) / Math.PI) / 2
						* (1 << zoom));
		if (xtile < 0)
			xtile = 0;
		if (xtile >= (1 << zoom))
			xtile = ((1 << zoom) - 1);
		if (ytile < 0)
			ytile = 0;
		if (ytile >= (1 << zoom))
			ytile = ((1 << zoom) - 1);
		// System.out.println("" + zoom + "/" + xtile + "/" + ytile);
		return new int[] { ytile, xtile };
	}

	public static double[] tile2boundingBox(int x, int y, int zoom) {
		double north = tile2lat(y, zoom);
		double south = tile2lat(y + 1, zoom);
		double west = tile2lon(x, zoom);
		double east = tile2lon(x + 1, zoom);
		return new double[] { south, north, west, east };
	}

	public static void printBoundingBox(int x, int y, int zoom) {

	}

	public static double tile2lon(int x, int z) {
		return x / Math.pow(2.0, z) * 360.0 - 180;
	}

	public static double tile2lat(int y, int z) {
		double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
		return Math.toDegrees(Math.atan(Math.sinh(n)));
	}

	public static void main(String[] args) {
		/*
		 * File file = new File("D:\\b\\02_03"); ArrayList<String> strArray = new
		 * ArrayList<String>(); String name = file.getName(); String[] path =
		 * name.split("_");
		 */
		getTileNumber(9, 90, 100);
		/*
		 * int level = 6; String number; for (int x = 0; x < level; x++) { for (int i =
		 * 0; i < (int) Math.pow(2, x); i++) { for (int j = 0; j < (int) Math.pow(2, x);
		 * j++) { number = getRowkey(x, i, j); System.out.println(number); } } }
		 */

		// getGeohash(2, Integer.valueOf(path[0]), Integer.valueOf(path[1]));
		/*
		 * for (String str : path) try { strArray.add(DecimalToBinary(str)); //
		 * System.out.println(strArray.get(0)); } catch (NumberFormatException e) {
		 * e.printStackTrace(); } System.out.println(strArray.size());
		 */
	}

}
