package com.kigo.thirds.tika;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;

/**
 * Created by kigo on 18-4-10.
 */
public class TikaTest {

    @Before
    public void before() throws Exception {

    }

    @After
    public void after() throws Exception {

    }



    /**
     * Tika支持简单的解析方法，解析文件的内容为String
     * @throws Exception
     */
    @Test
    public void test() throws Exception {
        String content;
        Tika tika = new Tika();
        try (InputStream stream = TikaTest.class.getResourceAsStream("/doc/XX_中文_20170109_44631986.doc")) {
            content = tika.parseToString(stream);
        }
    }


    public void testParseToOutFile() throws IOException, TikaException, SAXException {
        String inFile = "/tmp/b.zip";
        String outFile = "/tmp/out.log";
        InputStream in = new BufferedInputStream(new FileInputStream(new File(inFile)));//待解析文档的输入流
        OutputStream out = new BufferedOutputStream(new FileOutputStream(
                new File(outFile)));                                       //解析出的文档内容输出流，可以指定文件
        Metadata meta = new Metadata();
        ContentHandler handler = new BodyContentHandler(out);               //处理器
        Parser parser = new AutoDetectParser();                              //解析器，如果知道该用哪个解析器，可以指定，否则就用自动匹配的解析器
        parser.parse(in, handler, meta, new ParseContext());
        for (String name : meta.names()) {                                   //查看解析出的文档的元信息
            System.out.println(name + ":" + meta.get(name));
        }
    }

    @Test
    public void testParseFile() throws Exception {
        // Create a Tika instance with the default configuration
        Tika tika = new Tika();
        // Parse all given files and print out the extracted text content
        String[] args = {"/tmp/tika-files/a.txt", "/tmp/tika-files/b.zip", "/tmp/tika-files/c.ppt", "/tmp/tika-files/d.doc"}; ///tmp/b.zip,"/tmp/b.doc"
        for (String file : args) {
            String text = tika.parseToString(new File(file));
            System.out.println(text.trim());
        }
    }


    /**
     * Prase解析
        也可以使用更灵活的Tika Prase方法进行解析
     * @throws Exception
     */
    @Test
    public void testAutoDetectParser() throws Exception {
        String content;
        AutoDetectParser parser = new AutoDetectParser();
        BodyContentHandler handler = new BodyContentHandler();
        Metadata metadata = new Metadata();
        try (InputStream stream = TikaTest.class.getResourceAsStream("/doc/XX_中文_20170109_44631986.doc")) {
            parser.parse(stream, handler, metadata);
            content = handler.toString();
        }
        System.out.println(content);
    }

    @Test
    public void testParseToXhtml()throws Exception {
        String content;
        AutoDetectParser parser = new AutoDetectParser();
        //解析为XHTML
        ContentHandler handler = new ToXMLContentHandler();
        Metadata metadata = new Metadata();
        try (InputStream stream = TikaTest.class.getResourceAsStream("/doc/XX_中文_20170109_44631986.doc")) {
            parser.parse(stream, handler, metadata);
            content = handler.toString();
        }
        System.out.println(content);
    }
}
