package rtb;

import java.io.File;
import java.net.URISyntaxException;
import java.security.CodeSource;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class App { 

	final static Logger logger = Logger.getLogger(App.class);
	
	static String log4jConfPath;

	static String src_fpath;
	static String loaded_prq_fpath;
	static String regstr_prq_fpath;

	public static void main(String[] args) {
		
		CodeSource codeSource = App.class.getProtectionDomain().getCodeSource();
		File jarFile;
		String jarDir="";
		try {
			jarFile = new File(codeSource.getLocation().toURI().getPath());
			System.out.println("JAR FILE FULL PATH = "+jarFile.getAbsolutePath());
			jarDir = jarFile.getParentFile().getPath();
			System.out.println("JAR DIR PATH = "+jarDir);
			log4jConfPath = jarDir+File.separator+"log4j.properties";     
			PropertyConfigurator.configure(log4jConfPath);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
		AppSettingsXML app_set = new AppSettingsXML();
		app_set.ReadAppSettings(jarDir+File.separator+"settings.xml");    

		src_fpath        = app_set.src_fpath;
		loaded_prq_fpath = app_set.loaded_prq_fpath;
		regstr_prq_fpath = app_set.regstr_prq_fpath;

		JsonLoader jsonl_inst = new JsonLoader(); 

		jsonl_inst.json_load_parquet(src_fpath, loaded_prq_fpath, regstr_prq_fpath);

		jsonl_inst.final_calculate(loaded_prq_fpath, regstr_prq_fpath);

		//jsonl_inst.debug_ds(loaded_prq_fpath);
		}
	
}
