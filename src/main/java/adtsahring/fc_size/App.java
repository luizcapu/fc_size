package adtsahring.fc_size;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


public class App 
{
	
	private static final Logger logger = Logger.getLogger("fc_size");
	
	private static Connection adtsConn;	
	private static final String DB_ADTS_JDBC_URL = "DB_ADTS_JDBC_URL";
	private static final String DB_ADTS_USER = "DB_ADTS_USER";
	private static final String DB_ADTS_PASS = "DB_ADTS_PASS";

	private static final String FILE_CACHE_TABLE = "oc_filecache";
	private static final String SEL_STORAGE_FROM_FC = "SELECT distinct (storage) FROM "+FILE_CACHE_TABLE+" where (size =0 or size is null) and path like 'files/%' order by storage;";

	private static final String SEL_FOLDERS_TO_FIX = "SELECT fileid, parent FROM "+FILE_CACHE_TABLE+" where storage=? and (path like 'files/%' or path='files') and mimetype=2 and (size =0 or size is null) order by fileid desc";
	private static final String SEL_FOLDER_INFO = "SELECT parent, size FROM "+FILE_CACHE_TABLE+" where fileid=?";
	private static final String SEL_CHILDREN_SIZE = "SELECT coalesce(sum(size), 0) as children_size FROM "+FILE_CACHE_TABLE+" where parent=?";
	private static final String UPD_FOLDER_SIZE = "update "+FILE_CACHE_TABLE+" set size=? where fileid=?";
	
	private static PreparedStatement selStoragesFromFC;
	private static PreparedStatement selFoldersToFix;
	private static PreparedStatement selFolderInfo;
	private static PreparedStatement selChildrenSize;
	private static PreparedStatement updFolderSize;
	
	private static Connection controlConn;	
	private static final String DB_JDBC_URL = "DB_JDBC_URL";
	private static final String DB_USER = "DB_USER";
	private static final String DB_PASS = "DB_PASS";
	
	private static final String STORAGE_CONTROL_TABLE = "st_control";
	private static final String INS_STORAGE_IN_CONTROL = "insert into "+STORAGE_CONTROL_TABLE+" (st_id) values (?)";
	private static final String SEL_STORAGE_TO_PROCESS = "select st_id from "+STORAGE_CONTROL_TABLE+" where pending is true order by st_id";
	private static final String SET_STORAGE_PROCESSED = "update "+STORAGE_CONTROL_TABLE+" set pending = false where st_id = ?";
	
	private static PreparedStatement insStorageInControl;
	private static PreparedStatement selStoragesToProcess;
	private static PreparedStatement setStorageProcessed;
	
	private static Map<Integer, Folder> foldersMap;
	private static Map<Integer, Folder> foldersToFixMap;
	
    public static void main( String[] args ) throws Exception
    {
        App app = new App();
        app.initialize();
        app.run();
        app.tearDown();
    }
    
    public App() throws SQLException {
    	
    	try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
    	
    	//-- BEGIN ADTS CONN
        adtsConn = DriverManager.getConnection(
        		System.getenv(DB_ADTS_JDBC_URL), 
        		System.getenv(DB_ADTS_USER), 
        		System.getenv(DB_ADTS_PASS));
        
        selStoragesFromFC = adtsConn.prepareStatement(SEL_STORAGE_FROM_FC);
        selFoldersToFix = adtsConn.prepareStatement(SEL_FOLDERS_TO_FIX);
        selFolderInfo = adtsConn.prepareStatement(SEL_FOLDER_INFO);
        selChildrenSize = adtsConn.prepareStatement(SEL_CHILDREN_SIZE);
        updFolderSize = adtsConn.prepareStatement(UPD_FOLDER_SIZE);
    	//-- END ADTS CONN
        
    	//-- BEGIN CONTROL CONN
        controlConn = DriverManager.getConnection(
        		System.getenv(DB_JDBC_URL), 
        		System.getenv(DB_USER), 
        		System.getenv(DB_PASS));
        
        insStorageInControl = controlConn.prepareStatement(INS_STORAGE_IN_CONTROL);
        selStoragesToProcess = controlConn.prepareStatement(SEL_STORAGE_TO_PROCESS);
        setStorageProcessed = controlConn.prepareStatement(SET_STORAGE_PROCESSED);
    	//-- END ADTS CONN
    }
    
    private void initialize() throws Exception {
    	if (!storagesLoaded()) {
    		populateStoragesToProcess();
    	}
    }
    
    private void tearDown() throws SQLException {
    	adtsConn.close();
    	controlConn.close();
    }
    
    private void populateStoragesToProcess() throws Exception {
    	ResultSet storages = selStoragesFromFC.executeQuery();
    	
		controlConn.setAutoCommit(false);
    	try {
    		try {
    	    	while (storages.next()) {
    	    		insStorageInControl.setInt(1, storages.getInt(1));
    	    		insStorageInControl.execute();
    	    	}
    	    	controlConn.commit();
    		} catch (Exception e) {
    			controlConn.rollback();
    			throw e;
    		}
    	} finally {
    		controlConn.setAutoCommit(true);
    		storages.close();
    	}
    }
    
    private Boolean storagesLoaded() throws SQLException {    	
    	Statement selCount = controlConn.createStatement();
    	ResultSet rs = selCount.executeQuery("select count(1) as count from " + STORAGE_CONTROL_TABLE);
    	try {
        	while (rs.next()) {
        		return rs.getInt("count") > 0;
        	}    	
        	return false;    		
    	} finally {
    		rs.close();
    	}
    }
    
    private void run() throws Exception {
        ResultSet storages = selStoragesToProcess.executeQuery();
        try{
            while (storages.next()) {
            	adtsConn.setAutoCommit(false);
            	try {
            		try {
            			Integer storageId = storages.getInt(1); 
            			processStorage(storageId);
            			adtsConn.commit();
            			
            			setStorageProcessed.setInt(1,  storageId);
            			setStorageProcessed.execute();
            		} catch (Exception e) {
            			adtsConn.rollback();
            			throw e;
            		}
            	} finally {
            		adtsConn.setAutoCommit(true);
            	}
            }        	
        } finally {
        	storages.close();
        }
    }
    
    private void processStorage(Integer storageId) throws SQLException {
    	logger.info("Processing storage: " + storageId);
    	
    	List<Folder> folders = new ArrayList<Folder>();
    	
    	selFoldersToFix.setInt(1,  storageId);
    	ResultSet rs = selFoldersToFix.executeQuery();
    	try {
        	foldersMap = new HashMap<Integer, Folder>();
        	foldersToFixMap = new HashMap<Integer, Folder>();
        	while (rs.next()) {
        		Folder folder = new Folder(rs.getInt(1), rs.getInt(2), 0l);
        		// hold all folders that will be fixed (used later to decide update parent)
        		foldersToFixMap.put(folder.getFolderId(), folder);    		
        		// store in a list to keep order of processing
        		folders.add(folder);
        	}    		
    	} finally {
    		rs.close();
    	}
    	
    	for (Folder f : folders) {
    		fixFolder(f);
    	}
    }
    
    private void fixFolder(Folder folder) throws SQLException {
    	logger.info("Processing folder: " + folder.getFolderId());
    	foldersMap.put(folder.getFolderId(), folder);
    	
    	Long childrenSize = 0l;
    	selChildrenSize.setInt(1, folder.getFolderId());    	
    	ResultSet rs = selChildrenSize.executeQuery();
    	try {
        	while (rs.next()) {
        		childrenSize = rs.getLong(1);
        		break;
        	}    		
    	} finally {
    		rs.close();    		
    	}

    	if (childrenSize > 0) {
    		// update current folder and parents recursively
    		incFolderSize(folder.getFolderId(), childrenSize);
    	}    	
    	
    }
    
    private void incFolderSize(Integer folderId, final Long additionalSize) throws SQLException {
    	
    	Folder folder = getFolderInfo(folderId);    	
    	folder.setCurrentSize(folder.getCurrentSize() + additionalSize);
    	
    	updFolderSize.setLong(1, folder.getCurrentSize());
    	updFolderSize.setInt(2, folder.getFolderId());
    	updFolderSize.execute();
    	
    	if (folder.getParentId() > 0 && foldersToFixMap.get(folder.getParentId()) == null) {
    		// update parent incrementally only if parent will not be fully processed (not in foldersToFixMap) 
    		incFolderSize(folder.getParentId(), additionalSize);
    	}
    }
    
    private Folder getFolderInfo(Integer folderId) throws SQLException {
    	if (foldersMap.get(folderId) == null) {

    		Folder folder = null;    		
    		
    		selFolderInfo.setInt(1, folderId);    		
    		ResultSet rs = selFolderInfo.executeQuery();
    		try {
        		while (rs.next()) {
            		folder = new Folder(folderId, rs.getInt(1), rs.getLong(2));
            		break;
        		}    			
    		} finally {
    			rs.close();
    		}
    		
    		if (folder == null) {
    			throw new SQLException("Folder "+folderId+" not found!");
    		}
    		
    		foldersMap.put(folderId, folder);
    	}
    	return foldersMap.get(folderId);
    }
}
