package test.xml;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.junit.Test;

/**
 * Created by zhuhq on 2015/12/14.
 */
public class TestDocumentHelper {
    @Test
    public void testParseText() throws DocumentException {
        System.setProperty("test.name","helloBaby");
        Document doc = DocumentHelper.parseText("<jobs><job><name>${test.name}</name></job></jobs>");
        System.out.println(doc.selectSingleNode("/jobs/job/name").getText());
    }
}
