package tileutils;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;

/**
 * Created by Liebing Yu (huleryo.ylb@cug.edu.cn) on 2018/9/27
 */
public class FileUtils {

    private FileUtils() {}

    /**
     * List Directories under the `path`.
     * @param path: `path` need to be listed
     * @return java.io.File[] if `path` exists
     *          null if `path` not exists
     */
    public static File[] listDirectories(String path) {
        File file = new File(path);
        // if path is not existed, return null.
        if (!file.exists()) return null;
        return file.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.isDirectory();
            }
        });
    }

    /**
     * List Files under the `path`.
     * @param path: `path` need to be listed
     * @return java.io.File[] if `path` exists
     *          null if `path` not exists
     */
    public static File[] listFiles(String path) {
        File file = new File(path);
        // if path is not existed, return null.
        if (!file.exists()) return null;
        return file.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return pathname.isFile();
            }
        });
    }

    /**
     * List PNG under the `path`.
     * @param path: `path` need to be listed
     * @return java.io.File[] if `path` exists
     *          null if `path` not exists
     */
    public static File[] listPng(String path) {
        File file = new File(path);
        // if path is not existed, return null.
        if (!file.exists()) return null;
        return file.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".png");
            }
        });
    }

    public static void main(String[] args) {
        File[] files = FileUtils.listPng("C:\\Users\\80784_000\\Desktop\\tmp\\0\\0");
        for (File f : files) {
            System.out.println(f);
        }
    }
}
