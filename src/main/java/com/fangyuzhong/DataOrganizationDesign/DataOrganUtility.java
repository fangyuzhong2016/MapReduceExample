package com.fangyuzhong.DataOrganizationDesign;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

/**
 * Created by fangyuzhong on 17-7-10.
 */
public class DataOrganUtility
{
    public static String nestElements(DocumentBuilderFactory dbf, String post, List<String> comments)
    {
        try
        {
            // Create the new document to build the XML
            DocumentBuilder bldr = dbf.newDocumentBuilder();
            Document doc = bldr.newDocument();

            // Copy parent node to document
            Element postEl = getXmlElementFromString(dbf, post);
            Element toAddPostEl = doc.createElement("post");

            // Copy the attributes of the original post element to the new
            // one
            copyAttributesToElement(postEl.getAttributes(), toAddPostEl);

            // For each comment, copy it to the "post" node
            for (String commentXml : comments)
            {
                Element commentEl = getXmlElementFromString(dbf, commentXml);
                Element toAddCommentEl = doc.createElement("comments");

                // Copy the attributes of the original comment element to
                // the new one
                copyAttributesToElement(commentEl.getAttributes(),
                        toAddCommentEl);

                // Add the copied comment to the post element
                toAddPostEl.appendChild(toAddCommentEl);
            }

            // Add the post element to the document
            doc.appendChild(toAddPostEl);

            // Transform the document into a String of XML and return
            return transformDocumentToString(doc);

        } catch (Exception e)
        {
            return null;
        }
    }

    public static Element getXmlElementFromString(DocumentBuilderFactory dbf, String xml)
    {
        try
        {
            // Create a new document builder
            DocumentBuilder bldr = dbf.newDocumentBuilder();

            // Parse the XML string and return the first element
            return bldr.parse(new InputSource(new StringReader(xml)))
                    .getDocumentElement();
        } catch (Exception e)
        {
            return null;
        }
    }

    public static void copyAttributesToElement(NamedNodeMap attributes,
                                               Element element)
    {

        // For each attribute, copy it to the element
        for (int i = 0; i < attributes.getLength(); ++i)
        {
            Attr toCopy = (Attr) attributes.item(i);
            element.setAttribute(toCopy.getName(), toCopy.getValue());
        }
    }

    public static String transformDocumentToString(Document doc)
    {
        try
        {
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
                    "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(
                    writer));
            // Replace all new line characters with an empty string to have
            // one record per line.
            return writer.getBuffer().toString().replaceAll("\n|\r", "");
        } catch (Exception e)
        {
            return null;
        }
    }
}
