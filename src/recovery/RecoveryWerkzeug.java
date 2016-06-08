package recovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class RecoveryWerkzeug {
    List<String> NutzDatenRead = new ArrayList<String>();
    List<String> LogDatenRead = new ArrayList<String>();
	PersistenzManager pm = PersistenzManager.getInstance();
	private static File _TMP_DATEN = null;

    public RecoveryWerkzeug() throws Exception{
               
        BufferedReader brOfNutz = new BufferedReader(new InputStreamReader(new FileInputStream("data.txt")));
        
        for (String line = brOfNutz.readLine(); line != null; line = brOfNutz.readLine()) {  
            NutzDatenRead.add(line);
        }  
        brOfNutz.close();  
            	
        BufferedReader brOfLog = new BufferedReader(new InputStreamReader(new FileInputStream("log.txt")));
        
        for (String line = brOfLog.readLine(); line != null; line = brOfLog.readLine()) {  
            LogDatenRead.add(line);
        }  
        brOfLog.close();  
        
    }

	
	public void crashRecovery(){
		List<Integer> lsnOfNutzCol = new ArrayList<Integer>();
		List<Integer> pageIdOfNutzCol = new ArrayList<Integer>();
		HashMap<Integer, Integer> lsnPageIDofNutz = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> logPageIDUndAnzahl = new HashMap<Integer, Integer>();
		HashMap<Integer, List<Integer>> logPageIDUndLSN = new HashMap<Integer, List<Integer>>();


		int LSN = 0;
		int taid = 0;
		int logPageid = 0;
		String data;
		List<Integer> fertigTaid = new ArrayList<Integer>();
		List<Integer> commitlLogSN = new ArrayList<Integer>();
		List<Integer> LSNinSamePageID;

		for (int i = 0; i < this.NutzDatenRead.size(); i++) {
			String tmpNutz = this.NutzDatenRead.get(i);
			int erstNutzKommaPosition = tmpNutz.indexOf(",");
			int zweiteNutzCommaPosition =tmpNutz.indexOf(",", erstNutzKommaPosition+1);
			
//			String tests = tmpNutz.substring(0, erstNutzKommaPosition).trim();
//			for (int j = 0; j < tests.length(); j++) {
//				System.out.print(tests.charAt(j)+" ");
//			}
System.out.println(tmpNutz.substring(0, erstNutzKommaPosition).trim().length());
			int pageIdOfNutz =Integer.valueOf(tmpNutz.
					substring(0, erstNutzKommaPosition).trim());
			int lsnOfNutz = Integer.valueOf(tmpNutz.
					substring(erstNutzKommaPosition+1, zweiteNutzCommaPosition).trim());
			lsnPageIDofNutz.put(pageIdOfNutz,lsnOfNutz);
			pageIdOfNutzCol.add(pageIdOfNutz);
			lsnOfNutzCol.add(lsnOfNutz);
		}
        // Bestimmung von Gewinner Transaktionen
		for (int i = 0; i < this.LogDatenRead.size(); i++) {
			String tmpLog = LogDatenRead.get(i);
			int erstLogCommaPosition = tmpLog.indexOf(",");
			int lastLogCommaPosition = tmpLog.lastIndexOf(',');
			String ifCommited = tmpLog.substring(lastLogCommaPosition+1, tmpLog.length()-1);
			LSN = Integer.valueOf(tmpLog.substring
					(0,erstLogCommaPosition).trim());
            if (ifCommited.equalsIgnoreCase("committeds")) {
				int tmpTaid = Integer.valueOf(tmpLog.substring
						(erstLogCommaPosition+1, lastLogCommaPosition));
				fertigTaid.add(tmpTaid);
				commitlLogSN.add(LSN);
			}else {
				int anzahl=0;
				int zweiteLogCommaPosition = tmpLog.indexOf(",", erstLogCommaPosition+1);
				int dritteLogCommaPosition = tmpLog.indexOf(",", zweiteLogCommaPosition+1);
				logPageid = Integer.valueOf(tmpLog.substring
						(zweiteLogCommaPosition+1, dritteLogCommaPosition));
				
				if (logPageIDUndAnzahl.containsKey(logPageid)) {
					anzahl = logPageIDUndAnzahl.remove(logPageid);
					LSNinSamePageID = logPageIDUndLSN.remove(logPageid);
					anzahl++;
					LSNinSamePageID.add(LSN);
					logPageIDUndAnzahl.put(logPageid, anzahl);
					logPageIDUndLSN.put(logPageid, LSNinSamePageID);
				}else{
					anzahl = 1;
					LSNinSamePageID = new ArrayList<Integer>();;
					LSNinSamePageID.add(LSN);
					logPageIDUndLSN.put(logPageid, LSNinSamePageID);
					logPageIDUndAnzahl.put(logPageid, anzahl);
				}
			}
		}
		
		
		// selektive Redo da noforce, nosteal, non-atomic 
                
		for (int i = 0; i < this.LogDatenRead.size(); i++) {
			String tmpLog = LogDatenRead.get(i);
			int erstLogCommaPosition = tmpLog.indexOf(",");
			LSN = Integer.valueOf(tmpLog.substring
					(0,erstLogCommaPosition).trim());
			
			if (commitlLogSN.contains(LSN)) {
				
			}else{
				int zweiteLogCommaPosition = tmpLog.indexOf(",", erstLogCommaPosition+1);
				int dritteLogCommaPosition = tmpLog.indexOf(",", zweiteLogCommaPosition+1);
				taid =Integer.valueOf(tmpLog.substring
						(erstLogCommaPosition+1, zweiteLogCommaPosition));
				logPageid = Integer.valueOf(tmpLog.substring
						(zweiteLogCommaPosition+1, dritteLogCommaPosition));
				data = tmpLog.substring(dritteLogCommaPosition+1,tmpLog.length()-1);
				if (fertigTaid.contains(taid) && !lsnOfNutzCol.contains(LSN)) {
					if (pageIdOfNutzCol.contains(logPageid)) {
						int nutzLSN = lsnPageIDofNutz.get(logPageid);
						if (LSN > nutzLSN) {
							try {
								String strLogSN = null;
								if (LSN<10) {
									strLogSN = "0"+LSN;
								}else strLogSN = LSN+"";
								String nutzDatenToBeRecov = logPageid+","+strLogSN+","+ data +";";// + "\n";
								File nutzDaten = PersistenzManager.get_NUTZ_DATEN();				 
//								a.write(taid, logPageid, data,true,LSN);	
								rcvInsert(nutzDaten, logPageid, nutzDatenToBeRecov);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
					
					else if(logPageIDUndAnzahl.get(logPageid) > 1){
						LSNinSamePageID = logPageIDUndLSN.get(logPageid);
						int bigLSN = 0;
						for (int j = 0; j < LSNinSamePageID.size(); j++) {
							if (LSNinSamePageID.get(j) > bigLSN) {
								bigLSN = LSNinSamePageID.get(j);
							}
						}
						if (LSN == bigLSN) {
							try {
//								a.write(taid, logPageid, data,true,bigLSN);	
								String strLogSN = null;
								if (LSN<10) {
									strLogSN = "0"+LSN;
								}else strLogSN = LSN+"";
								String nutzDatenToBeRecov = logPageid+","+strLogSN+","+ data +";";// + "\n";
								File nutzDaten = PersistenzManager.get_NUTZ_DATEN();				 
//								a.write(taid, logPageid, data,true,LSN);	
								rcvInsert(nutzDaten, logPageid, nutzDatenToBeRecov);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
					
					else {
						try {
//							a.write(taid, logPageid, data,true,LSN);
							String strLogSN = null;
							if (LSN<10) {
								strLogSN = "0"+LSN;
							}else strLogSN = LSN+"";
							String nutzDatenToBeRecov = logPageid+","+strLogSN+","+ data +";";// + "\n";
							File nutzDaten = PersistenzManager.get_NUTZ_DATEN();				 
//							a.write(taid, logPageid, data,true,LSN);	
							rcvInsert(nutzDaten, logPageid, nutzDatenToBeRecov);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
		    }
		}
	}
	
	public void rcvInsert(File inFileNutz, int pageID, String nutzDatenToBeRecov)throws Exception {
		if (this._TMP_DATEN == null) {
			this._TMP_DATEN = new File(
	        "tmpdaten.txt");
		}
		boolean flagInsert = false;
		
		File outFile = File.createTempFile("tmpFile", ".tmp");
		
		FileInputStream fis = new FileInputStream(inFileNutz);
		BufferedReader in = new BufferedReader(new InputStreamReader(fis));
		
		FileOutputStream fos = new FileOutputStream(outFile);
		PrintWriter out = new PrintWriter(fos);
		
		String thisLine;
//		int i = 10;
//System.out.println("pageID: "+pageID);
//System.out.println("nutzDatenToBeRecov: "+nutzDatenToBeRecov); 

		while ((thisLine = in.readLine()) != null) {
	        int erstComma = thisLine.indexOf(",");
	        int pageIdOfScs = Integer.valueOf(thisLine.substring(0, erstComma).trim());
            if (pageIdOfScs < pageID) {
       
			}
			if (pageIdOfScs > pageID && !flagInsert) {
				out.println(nutzDatenToBeRecov);
				System.out.println("nutzDatenToBeRecovINSIDE: "+nutzDatenToBeRecov);
				flagInsert = true;
			}
			  // �����ȡ��������
System.out.println("thisLine: "+thisLine);
			out.println(thisLine);
			  // �к�����
//			i++;
		}if ((thisLine = in.readLine()) == null && !flagInsert) {
			out.println(nutzDatenToBeRecov);
			System.out.println("nutzDatenToBeRecovOUTSIDE: "+nutzDatenToBeRecov);
			flagInsert = true;

//			out.flush();
//			out.close();
//			in.close();
//			
//			//inFileNutz.delete();
//			
////			this._TMP_DATEN = outFile;
//		    outFile.renameTo(this.pm.get_NUTZ_DATEN());
//		    return;
		}
		out.flush();
		out.close();
		in.close();
		
		//inFileNutz.delete();
		
//		this._TMP_DATEN = outFile;
	    outFile.renameTo(this.pm.get_NUTZ_DATEN());

//	    outFile.renameTo(inFileNutz);
	}
}
