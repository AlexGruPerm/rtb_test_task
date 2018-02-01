package rtb;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;

public class AppSettingsXML {
	
	public String src_fpath;
	public String loaded_prq_fpath;
	public String regstr_prq_fpath;
	
	public void ReadAppSettings(String xml_setting_file){
		try {
			File fXmlFile = new File(xml_setting_file);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);

			doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("app_settings");
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					this.src_fpath = eElement.getElementsByTagName("source_json_file").item(0).getTextContent();
					this.loaded_prq_fpath = eElement.getElementsByTagName("app_loaded_parquet").item(0).getTextContent();
					this.regstr_prq_fpath = eElement.getElementsByTagName("registered_parquet").item(0).getTextContent();
				}
			}
		    } catch (Exception e) {
			e.printStackTrace();
		    }
	}

}
