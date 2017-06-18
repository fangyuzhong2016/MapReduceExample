package com.fangyuzhong.utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Created by fangyuzhong on 17-6-17.
 */
public class JobUtilJar
{

    // To declare global field
    private static List<URL> classPath = new ArrayList<URL>();

    // To declare method
    public static File createTempJar(String root) throws IOException
    {
        if (!new File(root).exists())
        {
            return null;
        }
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        final File jarFile = File.createTempFile("JobUtilJar-", ".jar", new File(System.getProperty("java.io.tmpdir")));

        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            public void run()
            {
                jarFile.delete();
            }
        });

        JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile), manifest);
        createTempJarInner(out, new File(root), "");
        out.flush();
        out.close();
        return jarFile;
    }

    private static void createTempJarInner(JarOutputStream out, File f, String base) throws IOException
    {
        if (f.isDirectory())
        {
            File[] fl = f.listFiles();
            if (base.length() > 0)
            {
                base = base + "/";
            }
            for (int i = 0; i < fl.length; i++)
            {
                createTempJarInner(out, fl[i], base + fl[i].getName());
            }
        } else
        {
            out.putNextEntry(new JarEntry(base));
            FileInputStream in = new FileInputStream(f);
            byte[] buffer = new byte[1024];
            int n = in.read(buffer);
            while (n != -1)
            {
                out.write(buffer, 0, n);
                n = in.read(buffer);
            }
            in.close();
        }
    }

    public static ClassLoader getClassLoader()
    {
        ClassLoader parent = Thread.currentThread().getContextClassLoader();

        if (parent == null)
        {
            parent = JobUtilJar.class.getClassLoader();
        }
        if (parent == null)
        {
            parent = ClassLoader.getSystemClassLoader();
        }
        return new URLClassLoader(classPath.toArray(new URL[0]), parent);
    }

    public static void addClasspath(String component)
    {

        if ((component != null) && (component.length() > 0))
        {
            try
            {
                File f = new File(component);

                if (f.exists())
                {
                    URL key = f.getCanonicalFile().toURL();
                    if (!classPath.contains(key))
                    {
                        classPath.add(key);
                    }
                }
            } catch (IOException e)
            {
            }
        }
    }

}